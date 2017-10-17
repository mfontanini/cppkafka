#include <stdexcept>
#include <cppkafka/mocking/kafka_cluster_registry.h>

using std::string;
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

void KafkaClusterRegistry::add_cluster(ClusterPtr cluster) {
    const string& url = cluster->get_url();
    lock_guard<mutex> _(clusters_mutex_);
    auto iter = clusters_.find(url);
    if (iter != clusters_.end()) {
        throw runtime_error("cluster already registered");
    }
    clusters_.emplace(url, move(cluster));
}

void KafkaClusterRegistry::remove_cluster(const KafkaCluster& cluster) {
    lock_guard<mutex> _(clusters_mutex_);
    clusters_.erase(cluster.get_url());
}

KafkaClusterRegistry::ClusterPtr KafkaClusterRegistry::get_cluster(const string& name) const {
    lock_guard<mutex> _(clusters_mutex_);
    auto iter = clusters_.find(name);
    return iter != clusters_.end() ? iter->second : ClusterPtr{};
}

} // detail
} // mocking
} // cppkafka
