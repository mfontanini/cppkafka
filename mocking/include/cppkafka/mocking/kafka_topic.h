#ifndef CPPKAFKA_MOCKING_KAFKA_TOPIC_H
#define CPPKAFKA_MOCKING_KAFKA_TOPIC_H

#include <string>
#include <vector>
#include <cppkafka/mocking/kafka_partition.h>
#include <cppkafka/mocking/kafka_message.h>

namespace cppkafka {
namespace mocking {

class KafkaTopic {
public:
    KafkaTopic(std::string name, unsigned partition_count);
    void add_message(unsigned partition, KafkaMessage message);
private:
    const std::string name_;
    std::vector<KafkaPartition> partitions_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_TOPIC_H
