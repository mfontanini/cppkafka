#include <stdexcept>
#include <algorithm>
#include <cppkafka/mocking/kafka_topic_mock.h>
#include <cppkafka/mocking/kafka_message_mock.h>

using std::string;
using std::out_of_range;
using std::move;
using std::lock_guard;
using std::mutex;
using std::vector;
using std::iota;

namespace cppkafka {
namespace mocking {

KafkaTopicMock::KafkaTopicMock(string name, unsigned partition_count,
                               OffsetManagerPtr offset_manager)
: name_(move(name)), partitions_(partition_count), offset_manager_(move(offset_manager)) {

}

void KafkaTopicMock::add_message(unsigned partition, KafkaMessageMock message) {
    if (partitions_.size() >= partition) {
        throw out_of_range("invalid partition index");
    }
    partitions_[partition].add_message(move(message));
}

void KafkaTopicMock::subscribe(const string& group_id, uint64_t consumer_id,
                               AssignmentCallback assignment_callback,
                               RevocationCallback revocation_callback) {
    lock_guard<mutex> _(subscribers_mutex_);
    auto& members_data = subscribers_[group_id];
    // If we're already subscribed, there's nothing to do
    if (members_data.count(consumer_id)) {
        return;
    }
    members_data.emplace(consumer_id,
                         MemberMetadata{move(assignment_callback), move(revocation_callback)});
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

void KafkaTopicMock::generate_assignments(const string& group_id,
                                          MembersMetadataMap& members_metadata) {
    vector<int> all_partitions(partitions_.size());
    iota(all_partitions.begin(), all_partitions.end(), 0);

    size_t chunk_size = all_partitions.size() / members_metadata.size();
    size_t member_index = 0;
    for (auto& member_data_pair  : members_metadata) {
        MemberMetadata& member = member_data_pair.second;
        if (!member.partitions_assigned.empty()) {
            member.revocation_callback(member.partitions_assigned);
        }
        const size_t chunk_start = chunk_size * member_index;
        // For the last one, add any remainders
        if (member_index == members_metadata.size() - 1) {
            chunk_size += all_partitions.size() % members_metadata.size();
        }
        vector<TopicPartitionMock> topic_partitions;
        for (size_t i = 0; i < chunk_size; ++i) {
            topic_partitions.emplace_back(name_, chunk_start + i);
        }
        topic_partitions = offset_manager_->get_offsets(group_id, move(topic_partitions));
        member.assignment_callback(topic_partitions);
        member.partitions_assigned = move(topic_partitions);
        member_index++;
    }
}

} // mocking
} // cppkafka
