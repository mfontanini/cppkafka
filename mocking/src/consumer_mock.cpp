#include <atomic>
#include <stdexcept>
#include <cppkafka/mocking/consumer_mock.h>
#include <cppkafka/mocking/kafka_cluster.h>
#include <cppkafka/mocking/kafka_topic_mock.h>
#include <cppkafka/mocking/kafka_message_mock.h>

using std::atomic;
using std::vector;
using std::string;
using std::move;
using std::bind;
using std::runtime_error;
using std::make_tuple;
using std::tie;
using std::get;
using std::lock_guard;
using std::unique_lock;
using std::mutex;

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
  consumer_id_(make_consumer_id()) {
    if (!config_.has_key(CONFIG_GROUP_ID)) {
        throw runtime_error("Failed to find " + CONFIG_GROUP_ID + " in config");
    }
}

ConsumerMock::~ConsumerMock() {
    get_cluster().unsubscribe(group_id_, consumer_id_);
}

void ConsumerMock::subscribe(const vector<string>& topics) {
    using namespace std::placeholders;
    get_cluster().subscribe(
        group_id_,
        consumer_id_,
        topics,
        bind(&ConsumerMock::on_assignment, this, _1),
        bind(&ConsumerMock::on_revocation, this),
        bind(&ConsumerMock::on_message, this, _1, _2, _3, _4)
    );
}

void ConsumerMock::assign(const vector<TopicPartitionMock>& topic_partitions) {
    for (const TopicPartitionMock& topic_partition : topic_partitions) {
        handle_assign(topic_partition);
    }
}

void ConsumerMock::unassign() {
    lock_guard<mutex> _(assigned_partitions_mutex_);
    assigned_partitions_.clear();
}

void ConsumerMock::set_opaque(void* opaque) {
    opaque_ = opaque;
}

ConsumerMock::TopicPartitionId ConsumerMock::make_id(const TopicPartitionMock& topic_partition) {
    return make_tuple(topic_partition.get_topic(), topic_partition.get_partition());
}

void ConsumerMock::on_assignment(const vector<TopicPartitionMock>& topic_partitions) {
    handle_rebalance(RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS, topic_partitions);
}

void ConsumerMock::on_revocation() {
    // Fetch and reset all assigned topic partitions
    vector<TopicPartitionMock> topic_partitions = [&]() {
        lock_guard<mutex> _(assigned_partitions_mutex_);
        vector<TopicPartitionMock> output;
        for (const auto& topic_partition_pair : assigned_partitions_) {
            const TopicPartitionId& id = topic_partition_pair.first;
            output.emplace_back(get<0>(id), get<1>(id));
        }
        assigned_partitions_.clear();
        return output;
    }();
    handle_rebalance(RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS, topic_partitions);
}

void ConsumerMock::on_message(const string& topic_name, unsigned partition, uint64_t offset,
                              const KafkaMessageMock* message) {
    // We should only process this if we don't have this topic/partition assigned (assignment
    // pending?) or the message offset comes after the next offset we have stored
    const bool valid_offset = [&]() {
        lock_guard<mutex> _(assigned_partitions_mutex_);
        auto iter = assigned_partitions_.find(make_tuple(topic_name, partition));
        return iter == assigned_partitions_.end() || iter->second.next_offset >= offset;
    }();
    if (!valid_offset) {
        return;
    }
    unique_lock<mutex> lock(message_queue_mutex_);
    new_message_queue_.push({ topic_name, partition, offset, message });
    message_queue_condition_.notify_one();
}

void ConsumerMock::handle_rebalance(rd_kafka_resp_err_t type,
                                    const vector<TopicPartitionMock>& topic_partitions) {
    auto rebalance_callback = config_.get_rebalance_callback();
    if (rebalance_callback) {
        auto handle = to_rdkafka_handle(topic_partitions);
        rebalance_callback(nullptr, type, handle.get(), opaque_);
    }
}

void ConsumerMock::handle_assign(const TopicPartitionMock& topic_partition) {
    const auto id = make_id(topic_partition);
    // We'll store the next offset from the one we've seen so far
    const uint64_t next_offset = topic_partition.get_offset() + 1;
    
    {
        lock_guard<mutex> _(assigned_partitions_mutex_);
        if (assigned_partitions_.count(id)) {
            return;
        }
        assigned_partitions_.emplace(id, next_offset);
    }

    // Fetch any existing messages and push them to the available message queue
    auto& cluster = get_cluster();
    KafkaTopicMock& topic = cluster.get_topic(topic_partition.get_topic());
    fetch_existing_messages(topic_partition.get_partition(), next_offset, topic);
}

void ConsumerMock::fetch_existing_messages(unsigned partition_index, uint64_t next_offset,
                                           KafkaTopicMock& topic) {
    KafkaPartitionMock& partition = topic.get_partition(partition_index);
    uint64_t start_offset;
    uint64_t end_offset;
    tie(start_offset, end_offset) = partition.get_offset_bounds();
    if (start_offset < next_offset) {
        throw runtime_error("Stored offset is too high");
    }
    // Nothing to fetch
    if (next_offset == end_offset) {
        return;
    }
    unique_lock<mutex> lock(message_queue_mutex_);
    for (uint64_t i = next_offset; i != end_offset; ++i) {
        const KafkaMessageMock& message = partition.get_message(i);
        available_message_queue_.push({ topic.get_name(), partition_index, i, &message });
    }
    message_queue_condition_.notify_all();
}

// TopicPartitionInfo

ConsumerMock::TopicPartitionInfo::TopicPartitionInfo(uint64_t next_offset)
: next_offset(next_offset) {

}

} // mocking
} // cppkafka
