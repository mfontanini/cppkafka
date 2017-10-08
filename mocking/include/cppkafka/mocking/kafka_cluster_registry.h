#ifndef CPPKAFKA_MOCKING_KAFKA_CLUSTER_REGISTRY_H
#define CPPKAFKA_MOCKING_KAFKA_CLUSTER_REGISTRY_H

#include <mutex>
#include <cppkafka/mocking/kafka_cluster.h>

namespace cppkafka {
namespace mocking {
namespace detail {

class KafkaClusterRegistry {
public:
    static KafkaClusterRegistry& instance();

    void add_cluster(KafkaCluster* cluster);
    void remove_cluster(KafkaCluster* cluster);
private:
    std::unordered_map<std::string, KafkaCluster*> clusters_;
    mutable std::mutex clusters_mutex_;
};

} // detail
} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_CLUSTER_REGISTRY_H
