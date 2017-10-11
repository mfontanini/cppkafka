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
    using ClusterPtr = std::shared_ptr<KafkaCluster>;

    static KafkaClusterRegistry& instance();

    void add_cluster(ClusterPtr cluster);
    void remove_cluster(const KafkaCluster& cluster);
private:
    std::unordered_map<std::string, ClusterPtr> clusters_;
    mutable std::mutex clusters_mutex_;
};

} // detail
} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_CLUSTER_REGISTRY_H
