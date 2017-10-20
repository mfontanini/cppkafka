#ifndef CPPKAFKA_MOCKING_KAFKA_TOPIC_MOCK_H
#define CPPKAFKA_MOCKING_KAFKA_TOPIC_MOCK_H

#include <unordered_map>
#include <unordered_set>
#include <string>
#include <cstdint>
#include <vector>
#include <mutex>
#include <functional>
#include <memory>
#include <cppkafka/mocking/kafka_partition_mock.h>
#include <cppkafka/mocking/topic_partition_mock.h>
#include <cppkafka/mocking/offset_manager.h>

namespace cppkafka {
namespace mocking {

class KafkaMessageMock;

class KafkaTopicMock {
public:
    using AssignmentCallback = std::function<void(std::vector<TopicPartitionMock>&)>;
    using RevocationCallback = std::function<void(const std::vector<TopicPartitionMock>&)>;
    using OffsetManagerPtr = std::shared_ptr<OffsetManager>;

    KafkaTopicMock(std::string name, unsigned partition_count, OffsetManagerPtr offset_manager);
    void add_message(unsigned partition, KafkaMessageMock message);
    void subscribe(const std::string& group_id, uint64_t consumer_id,
                   AssignmentCallback assignment_callback,
                   RevocationCallback revocation_callback);
    void unsubscribe(const std::string& group_id, uint64_t consumer_id);
private:
    struct MemberMetadata {
        const AssignmentCallback assignment_callback;
        const RevocationCallback revocation_callback;
        std::vector<TopicPartitionMock> partitions_assigned;
    };
    using MembersMetadataMap = std::unordered_map<uint64_t, MemberMetadata>;

    void generate_assignments(const std::string& group_id, MembersMetadataMap& members_metadata);

    const std::string name_;
    std::vector<KafkaPartitionMock> partitions_;
    OffsetManagerPtr offset_manager_;
    std::unordered_map<std::string, MembersMetadataMap> subscribers_;
    mutable std::mutex subscribers_mutex_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_TOPIC_MOCK_H
