#ifndef CPPKAFKA_MOCKING_KAFKA_CLUSTER_H
#define CPPKAFKA_MOCKING_KAFKA_CLUSTER_H

#include <unordered_map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <cppkafka/mocking/kafka_topic_mock.h>
#include <cppkafka/mocking/kafka_message_mock.h>
#include <cppkafka/mocking/offset_manager.h>

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
    bool topic_exists(const std::string& name) const;
    void produce(const std::string& topic, unsigned partition, KafkaMessageMock message);
    template <typename Functor>
    void acquire_topic(const std::string& topic, const Functor& functor);
private:
    KafkaCluster(std::string url);

    const std::string url_;
    std::shared_ptr<OffsetManager> offset_manager_;
    std::unordered_map<std::string, KafkaTopicMock> topics_;
    mutable std::mutex topics_mutex_;
};

template <typename Functor>
void KafkaCluster::acquire_topic(const std::string& topic, const Functor& functor) {
    std::unique_lock<std::mutex> lock(topics_mutex_);
    auto iter = topics_.find(topic);
    if (iter == topics_.end()) {
        throw std::runtime_error("Topic " + topic + " doesn't exist");
    }
    // Unlock and execute callback. We won't remove topics so this is thread safe on a
    // cluster level
    lock.unlock();
    functor(iter->second);
}

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_CLUSTER_H
