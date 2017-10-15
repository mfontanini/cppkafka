#ifndef CPPKAFKA_MOCKING_KAFKA_CLUSTER_H
#define CPPKAFKA_MOCKING_KAFKA_CLUSTER_H

#include <unordered_map>
#include <memory>
#include <cppkafka/mocking/kafka_topic_mock.h>
#include <cppkafka/mocking/kafka_message_mock.h>

namespace cppkafka {
namespace mocking {

class KafkaCluster {
public:
    static std::shared_ptr<KafkaCluster> make_cluster(std::string url);

    KafkaCluster(const KafkaCluster&) = delete;
    KafkaCluster& operator=(const KafkaCluster&) = delete;
    ~KafkaCluster();

    const std::string& get_url() const;

    void add_topic(const std::string& name, unsigned partitions);
    void produce(const std::string& topic, unsigned partition, KafkaMessageMock message);
private:
    KafkaCluster(std::string url);

    const std::string url_;
    std::unordered_map<std::string, KafkaTopicMock> topics_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_CLUSTER_H
