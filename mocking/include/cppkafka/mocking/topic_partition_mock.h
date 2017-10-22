#ifndef CPPKAFKA_MOCKING_TOPIC_PARTITION_MOCK_H
#define CPPKAFKA_MOCKING_TOPIC_PARTITION_MOCK_H

#include <string>
#include <cstdint>
#include <memory>
#include <vector>
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
    const std::string topic_;
    const int partition_;
    int64_t offset_;
};

using TopicPartitionMockListPtr = std::unique_ptr<rd_kafka_topic_partition_list_t,
                                                 decltype(&rd_kafka_topic_partition_list_destroy)>;
TopicPartitionMockListPtr
to_rdkafka_handle(const std::vector<TopicPartitionMock>& topic_partitions);
std::vector<TopicPartitionMock>
from_rdkafka_handle(const rd_kafka_topic_partition_list_t& topic_partitions);

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_TOPIC_PARTITION_MOCK_H
