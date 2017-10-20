#ifndef CPPKAFKA_MOCKING_TOPIC_PARTITION_MOCK_H
#define CPPKAFKA_MOCKING_TOPIC_PARTITION_MOCK_H

#include <string>
#include <cstdint>
#include <librdkafka/rdkafka.h>

namespace cppkafka {
namespace mocking {

class TopicPartitionMock {
public:
    TopicPartitionMock(std::string topic, int partition, int64_t offset = RD_KAFKA_OFFSET_INVALID);

    const std::string& get_topic() const;
    int get_partition() const;
    int64_t get_offset() const;

    void set_offset(int64_t offset);
private:
    std::string topic_;
    int partition_;
    int64_t offset_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_TOPIC_PARTITION_MOCK_H
