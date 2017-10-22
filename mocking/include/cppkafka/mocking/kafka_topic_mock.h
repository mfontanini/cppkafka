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

namespace cppkafka {
namespace mocking {

class KafkaMessageMock;

class KafkaTopicMock {
public:
    KafkaTopicMock(std::string name, unsigned partition_count);
    const std::string& get_name() const;

    void add_message(unsigned partition, KafkaMessageMock message);
    KafkaPartitionMock& get_partition(unsigned partition);
    const KafkaPartitionMock& get_partition(unsigned partition) const;
    size_t get_partition_count() const;
private:

    const std::string name_;
    std::vector<KafkaPartitionMock> partitions_;
    mutable std::mutex subscribers_mutex_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_TOPIC_MOCK_H
