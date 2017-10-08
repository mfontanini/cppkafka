#ifndef CPPKAFKA_MOCKING_KAFKA_CLUSTER_H
#define CPPKAFKA_MOCKING_KAFKA_CLUSTER_H

#include <unordered_map>
#include <cppkafka/mocking/kafka_topic.h>
#include <cppkafka/mocking/kafka_message.h>

namespace cppkafka {
namespace mocking {

class KafkaCluster {
public:
    KafkaCluster(std::string url);
    KafkaCluster(const KafkaCluster&) = delete;
    KafkaCluster& operator=(const KafkaCluster&) = delete;
    ~KafkaCluster();

    const std::string& get_url() const;

    void add_topic(const std::string& name, unsigned partitions);
    void produce(const std::string& topic, unsigned partition, KafkaMessage message);
private:
    const std::string url_;
    std::unordered_map<std::string, KafkaTopic> topics_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_CLUSTER_H
