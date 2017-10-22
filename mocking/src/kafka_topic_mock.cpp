#include <stdexcept>
#include <algorithm>
#include <cppkafka/mocking/kafka_topic_mock.h>
#include <cppkafka/mocking/kafka_message_mock.h>

using std::string;
using std::move;
using std::lock_guard;
using std::mutex;
using std::vector;
using std::iota;
using std::bind;
using std::out_of_range;
using std::runtime_error;

namespace cppkafka {
namespace mocking {

KafkaTopicMock::KafkaTopicMock(string name, unsigned partition_count,
                               OffsetManagerPtr offset_manager)
: name_(move(name)), partitions_(partition_count), offset_manager_(move(offset_manager)) {

}

const string& KafkaTopicMock::get_name() const {
    return name_;
}

void KafkaTopicMock::add_message(unsigned partition, KafkaMessageMock message) {
    if (partitions_.size() >= partition) {
        throw out_of_range("invalid partition index");
    }
    partitions_[partition].add_message(move(message));
}

void KafkaTopicMock::subscribe(const string& group_id, uint64_t consumer_id,
                               AssignmentCallback assignment_callback,
                               RevocationCallback revocation_callback,
                               MessageCallback message_callback) {
    lock_guard<mutex> _(subscribers_mutex_);
    auto& members_data = subscribers_[group_id];
    // If we're already subscribed, there's nothing to do
    if (members_data.count(consumer_id)) {
        return;
    }
    members_data.emplace(consumer_id,
                         MemberMetadata{move(assignment_callback),
                                        move(revocation_callback),
                                        move(message_callback)});
    generate_assignments(group_id, members_data);
}

void KafkaTopicMock::unsubscribe(const std::string& group_id, uint64_t consumer_id) {
    lock_guard<mutex> _(subscribers_mutex_);
    auto iter = subscribers_.find(group_id);
    if (iter != subscribers_.end()) {
        iter->second.erase(consumer_id);
        generate_assignments(group_id, iter->second);
    }
}

KafkaPartitionMock& KafkaTopicMock::get_partition(unsigned partition) {
    if (partition >= partitions_.size()) {
        throw runtime_error("Partition doesn't exist");
    }
    return partitions_[partition];
}

const KafkaPartitionMock& KafkaTopicMock::get_partition(unsigned partition) const {
    return const_cast<KafkaTopicMock&>(*this).get_partition(partition);
}

void KafkaTopicMock::generate_assignments(const string& group_id,
                                          MembersMetadataMap& members_metadata) {
    vector<int> all_partitions(partitions_.size());
    iota(all_partitions.begin(), all_partitions.end(), 0);

    size_t chunk_size = all_partitions.size() / members_metadata.size();
    size_t member_index = 0;
    for (auto& member_data_pair  : members_metadata) {
        MemberMetadata& member = member_data_pair.second;
        // Execute revocation callback and unsubscribe from the partition object
        if (!member.partitions_assigned.empty()) {
            member.revocation_callback(member.partitions_assigned);
            for (const auto& partition_subscription : member.partition_subscriptions) {
                auto& partition_mock = partitions_[partition_subscription.first];
                partition_mock.unsubscribe(partition_subscription.second);
            }
        }
        member.partition_subscriptions.clear();

        // Generate partition assignment
        const size_t chunk_start = chunk_size * member_index;
        // For the last one, add any remainders
        if (member_index == members_metadata.size() - 1) {
            chunk_size += all_partitions.size() % members_metadata.size();
        }
        vector<TopicPartitionMock> topic_partitions;
        for (size_t i = 0; i < chunk_size; ++i) {
            topic_partitions.emplace_back(name_, chunk_start + i);
        }
        // Try to load the offsets and store the topic partitions
        topic_partitions = offset_manager_->get_offsets(group_id, move(topic_partitions));
        member.partitions_assigned = move(topic_partitions);

        // Subscribe to every assigned partition
        using namespace std::placeholders;
        for (const TopicPartitionMock& topic_partition : member.partitions_assigned) {
            auto& partition_mock = partitions_[topic_partition.get_partition()];
            auto callback = bind(member.message_callback, topic_partition.get_topic(),
                                 topic_partition.get_partition(), _1, _2);
            const auto subscriber_id = partition_mock.subscribe(move(callback));
            member.partition_subscriptions.emplace(topic_partition.get_partition(),
                                                   subscriber_id);
        }
        // Now trigger the assignment callback
        member.assignment_callback(member.partitions_assigned);
        member_index++;
    }
}

} // mocking
} // cppkafka
