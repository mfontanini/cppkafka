#ifndef CPPKAFKA_MOCKING_KAFKA_CLUSTER_REGISTRY_H
#define CPPKAFKA_MOCKING_KAFKA_CLUSTER_REGISTRY_H

#include <mutex>
#include <memory>
#include <cppkafka/mocking/kafka_cluster.h>

namespace cppkafka {
namespace mocking {
namespace detail {

class KafkaClusterRegistry {
public:
    static KafkaClusterRegistry& instance();

    void add_cluster(std::shared_ptr<KafkaCluster> cluster);
    void remove_cluster(const std::string& url);
    std::shared_ptr<KafkaCluster> get_cluster(const std::string& name) const;
private:
    std::unordered_map<std::string, std::weak_ptr<KafkaCluster>> clusters_;
    mutable std::mutex clusters_mutex_;
};

} // detail
} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_CLUSTER_REGISTRY_H
