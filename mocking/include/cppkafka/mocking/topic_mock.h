#ifndef CPPKAFKA_MOCKING_TOPIC_MOCK_H
#define CPPKAFKA_MOCKING_TOPIC_MOCK_H

#include <string>
#include <vector>
#include <cppkafka/mocking/partition_mock.h>

namespace cppkafka {
namespace mocking {

class MessageMock;

class TopicMock {
public:
    TopicMock(std::string name, unsigned partition_count);
    void add_message(unsigned partition, MessageMock message);
private:
    const std::string name_;
    std::vector<PartitionMock> partitions_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_TOPIC_MOCK_H
