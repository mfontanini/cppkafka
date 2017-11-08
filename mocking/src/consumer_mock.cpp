#include <atomic>
#include <stdexcept>
#include <cassert>
#include <cppkafka/mocking/consumer_mock.h>
#include <cppkafka/mocking/kafka_cluster.h>
#include <cppkafka/mocking/kafka_topic_mock.h>
#include <cppkafka/mocking/kafka_message_mock.h>

using std::atomic;
using std::vector;
using std::string;
using std::unordered_map;
using std::to_string;
using std::move;
using std::bind;
using std::runtime_error;
using std::make_tuple;
using std::unique_ptr;
using std::tie;
using std::get;
using std::lock_guard;
using std::unique_lock;
using std::mutex;

using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cppkafka {
namespace mocking {

static const string CONFIG_GROUP_ID = "group.id";

uint64_t ConsumerMock::make_consumer_id() {
    static atomic<uint64_t> current_id{0};
    return current_id++;
}

ConsumerMock::ConsumerMock(ConfigurationMock config, EventProcessorPtr processor,
                           ClusterPtr cluster)
: HandleMock(move(processor), move(cluster)), config_(move(config)),
  offset_reset_policy_(get_offset_policy()), emit_eofs_(get_partition_eof_enabled()),
  auto_commit_(get_auto_commit()), consumer_id_(make_consumer_id()) {
    if (!config_.has_key(CONFIG_GROUP_ID)) {
        throw runtime_error("Failed to find " + CONFIG_GROUP_ID + " in config");
    }
    group_id_ = config_.get(CONFIG_GROUP_ID);
}

ConsumerMock::~ConsumerMock() {
    get_cluster().unsubscribe(group_id_, consumer_id_);
}

void ConsumerMock::close() {
    unsubscribe();
}

void ConsumerMock::commit(const rd_kafka_message_t& message) {
    get_cluster().commit(
        group_id_,
        consumer_id_,
        { { rd_kafka_topic_name(message.rkt), message.partition, message.offset + 1 } }
    );
}

void ConsumerMock::commit(const vector<TopicPartitionMock>& topic_partitions) {
    get_cluster().commit(group_id_,consumer_id_, topic_partitions);
}

void ConsumerMock::subscribe(const vector<string>& topics) {
    using namespace std::placeholders;
    get_cluster().subscribe(
        group_id_,
        consumer_id_,
        topics,
        bind(&ConsumerMock::on_assignment, this, _1),
        bind(&ConsumerMock::on_revocation, this)
    );
}

void ConsumerMock::unsubscribe() {
    get_cluster().unsubscribe(group_id_, consumer_id_);
}

void ConsumerMock::assign(const vector<TopicPartitionMock>& topic_partitions) {
    {
        lock_guard<mutex> _(mutex_);
        // Create entries for all topic partitions in our assigned partitions map
        for (const TopicPartitionMock& topic_partition : topic_partitions) {
            const auto id = make_id(topic_partition);
            uint64_t next_offset;
            if (topic_partition.get_offset() == RD_KAFKA_OFFSET_INVALID) {
                next_offset = 0;
            }
            else {
                next_offset = topic_partition.get_offset();
            }
            
            auto iter = assigned_partitions_.find(id);
            if (iter == assigned_partitions_.end()) {
                iter = assigned_partitions_.emplace(id, TopicPartitionInfo{}).first;
            }
            else {
                // The offset changed, clean up any messages with a lower offset than the 
                // next one
                auto& queue = iter->second.messages;
                while (!queue.empty() && queue.front().offset < next_offset) {
                    queue.pop();
                }
            }
            iter->second.next_offset = next_offset;
        }
    }
    using namespace std::placeholders;
    // Now assign these partitions. This will atomically fetch all message we should fetch and
    // then subscribe us to the topic/partitions
    get_cluster().assign(consumer_id_, topic_partitions, offset_reset_policy_,
                         bind(&ConsumerMock::on_message, this, _1, _2, _3, _4));
}

void ConsumerMock::unassign() {
    lock_guard<mutex> _(mutex_);
    assigned_partitions_.clear();
    consumable_topic_partitions_.clear();
    get_cluster().unassign(consumer_id_);
}

void ConsumerMock::pause_partitions(const vector<TopicPartitionMock>& topic_partitions) {
    lock_guard<mutex> _(mutex_);
    for (const TopicPartitionMock& topic_partition : topic_partitions) {
        auto id = make_id(topic_partition);
        consumable_topic_partitions_.erase(id);
        paused_topic_partitions_.emplace(move(id));
    }
}

void ConsumerMock::resume_partitions(const vector<TopicPartitionMock>& topic_partitions) {
    lock_guard<mutex> _(mutex_);
    for (const TopicPartitionMock& topic_partition : topic_partitions) {
        auto id = make_id(topic_partition);
        paused_topic_partitions_.erase(id);
        auto iter = assigned_partitions_.find(id);
        if (iter != assigned_partitions_.end() && !iter->second.messages.empty()) {
            consumable_topic_partitions_.emplace(move(id));
        }
    }
}

unique_ptr<MessageHandle> ConsumerMock::poll(milliseconds timeout) {
    unique_lock<mutex> lock(mutex_);
    if (consumable_topic_partitions_.empty()) {
        messages_condition_.wait_for(lock, timeout);
    }
    if (consumable_topic_partitions_.empty()) {
        return nullptr;
    }
    const auto id = *consumable_topic_partitions_.begin();
    auto iter = assigned_partitions_.find(id);
    assert(iter != assigned_partitions_.end());

    auto& queue = iter->second.messages;
    if (emit_eofs_ && queue.empty()) {
        // We emit the EOF so it's no longer consumable
        consumable_topic_partitions_.erase(id);
        return unique_ptr<MessageHandle>(new MessageHandle(
            unique_ptr<TopicHandle>(new TopicHandle(get<0>(id), nullptr)),
            get<1>(id),
            iter->second.next_offset,
            nullptr, 0, // key
            nullptr, 0, // payload
            RD_KAFKA_RESP_ERR_NO_ERROR,
            MessageHandlePrivateData{},
            MessageHandle::PointerOwnership::Unowned
        ));
    }
    else {
        assert(!queue.empty());
    }
    MessageAggregate aggregate = move(queue.front());
    queue.pop();

    // If we have no more mesages we can't consume from it anymore
    if (queue.empty()) {
        consumable_topic_partitions_.erase(id);
    }

    const auto& message = *aggregate.message;
    unique_ptr<MessageHandle> output(new MessageHandle(
        unique_ptr<TopicHandle>(new TopicHandle(get<0>(id), nullptr)),
        get<1>(id),
        aggregate.offset,
        (void*)message.get_key().data(), message.get_key().size(),
        (void*)message.get_payload().data(), message.get_payload().size(),
        RD_KAFKA_RESP_ERR__PARTITION_EOF,
        MessageHandlePrivateData{message.get_timestamp_type(), message.get_timestamp()},
        MessageHandle::PointerOwnership::Unowned
    ));
    if (auto_commit_) {
        commit(output->get_message());
    }
    return output;
}

vector<TopicPartitionMock> ConsumerMock::get_assignment() const {
    vector<TopicPartitionMock> output;
    lock_guard<mutex> _(mutex_);
    for (const auto& partition_pair : assigned_partitions_) {
        output.emplace_back(get<0>(partition_pair.first), get<1>(partition_pair.first));
    }
    return output;
}

ConsumerMock::TopicPartitionId ConsumerMock::make_id(const TopicPartitionMock& topic_partition) {
    return make_tuple(topic_partition.get_topic(), topic_partition.get_partition());
}

KafkaCluster::ResetOffsetPolicy ConsumerMock::get_offset_policy() const {
    static const string KEY_NAME = "auto.offset.reset";
    static unordered_map<string, KafkaCluster::ResetOffsetPolicy> MAPPINGS = {
        { "smallest", KafkaCluster::ResetOffsetPolicy::Earliest },
        { "earliest", KafkaCluster::ResetOffsetPolicy::Earliest },
        { "beginning", KafkaCluster::ResetOffsetPolicy::Earliest },
        { "latest", KafkaCluster::ResetOffsetPolicy::Latest },
        { "largest", KafkaCluster::ResetOffsetPolicy::Latest },
        { "end", KafkaCluster::ResetOffsetPolicy::Latest },
    };

    const ConfigurationMock* topic_config = config_.get_default_topic_configuration();
    if (!topic_config || !topic_config->has_key(KEY_NAME)) {
        return KafkaCluster::ResetOffsetPolicy::Earliest;
    }
    else {
        auto iter = MAPPINGS.find(topic_config->get(KEY_NAME));
        if (iter == MAPPINGS.end()) {
            throw runtime_error("invalid auto.offset.reset value");
        }
        return iter->second;
    }
}

bool ConsumerMock::get_partition_eof_enabled() const {
    static const string KEY_NAME = "enable.partition.eof";
    return !config_.has_key(KEY_NAME) || config_.get(KEY_NAME) == "true";
}

bool ConsumerMock::get_auto_commit() const {
    static const vector<string> KEY_NAMES = {
        "enable.auto.commit",
        "auto.commit.enable"
    };
    for (const string& key : KEY_NAMES) {
        if (config_.has_key(key)) {
            return config_.get(key) == "true";
        }
    }
    // By default, auto commit
    return true;
}

void ConsumerMock::on_assignment(const vector<TopicPartitionMock>& topic_partitions) {
    handle_rebalance(RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS, topic_partitions);
}

void ConsumerMock::on_revocation() {
    // Fetch and all assigned topic partitions
    vector<TopicPartitionMock> topic_partitions = [&]() {
        lock_guard<mutex> _(mutex_);
        vector<TopicPartitionMock> output;
        for (const auto& topic_partition_pair : assigned_partitions_) {
            const TopicPartitionId& id = topic_partition_pair.first;
            output.emplace_back(get<0>(id), get<1>(id));
        }
        return output;
    }();
    handle_rebalance(RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS, topic_partitions);
}

void ConsumerMock::on_message(const string& topic_name, unsigned partition, uint64_t offset,
                              const KafkaMessageMock& message) {
    auto id = make_tuple(topic_name, partition);
    MessageAggregate aggregate = { topic_name, partition, offset, &message };

    // We should only process this if we don't have this topic/partition assigned (assignment
    // pending?) or the message offset comes after the next offset we have stored
    lock_guard<mutex> _(mutex_);
    auto iter = assigned_partitions_.find(id);
    if (iter == assigned_partitions_.end()) {
        throw runtime_error("got message for unexpected partition " + to_string(partition));
    }
    if (offset > iter->second.next_offset) {
        throw runtime_error("got message with unexpected offset " + to_string(offset));
    }
    else if (offset < iter->second.next_offset) {
        return;
    }
    // This is the message we were waiting for
    iter->second.next_offset++;
    iter->second.messages.push(move(aggregate));
    if (!paused_topic_partitions_.count(id)) {
        consumable_topic_partitions_.emplace(move(id));
        messages_condition_.notify_one();
    }
}

void ConsumerMock::handle_rebalance(rd_kafka_resp_err_t type,
                                    const vector<TopicPartitionMock>& topic_partitions) {
    auto rebalance_callback = config_.get_rebalance_callback();
    if (rebalance_callback) {
        auto handle = to_rdkafka_handle(topic_partitions);
        rebalance_callback(nullptr, type, handle.get(), config_.get_opaque());
    }
}

} // mocking
} // cppkafka
