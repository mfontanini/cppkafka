#ifndef CPPKAFKA_MOCKING_KAFKA_TOPIC_MOCK_H
#define CPPKAFKA_MOCKING_KAFKA_TOPIC_MOCK_H

#include <string>
#include <vector>
#include <cppkafka/mocking/kafka_partition_mock.h>

namespace cppkafka {
namespace mocking {

class KafkaMessageMock;

class KafkaTopicMock {
public:
    KafkaTopicMock(std::string name, unsigned partition_count);
    void add_message(unsigned partition, KafkaMessageMock message);
private:
    const std::string name_;
    std::vector<KafkaPartitionMock> partitions_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_TOPIC_MOCK_H
