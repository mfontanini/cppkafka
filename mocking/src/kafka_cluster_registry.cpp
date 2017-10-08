#include <stdexcept>
#include <cppkafka/mocking/kafka_cluster_registry.h>

using std::lock_guard;
using std::mutex;
using std::runtime_error;

namespace cppkafka {
namespace mocking {
namespace detail {

KafkaClusterRegistry& KafkaClusterRegistry::instance() {
    static KafkaClusterRegistry registry;
    return registry;
}

void KafkaClusterRegistry::add_cluster(KafkaCluster* cluster) {
    lock_guard<mutex> _(clusters_mutex_);
    auto iter = clusters_.find(cluster->get_url());
    if (iter != clusters_.end()) {
        throw runtime_error("cluster already registered");
    }
    clusters_.emplace(cluster->get_url(), cluster);
}

void KafkaClusterRegistry::remove_cluster(KafkaCluster* cluster) {
    lock_guard<mutex> _(clusters_mutex_);
    clusters_.erase(cluster->get_url());
}

} // detail
} // mocking
} // cppkafka
