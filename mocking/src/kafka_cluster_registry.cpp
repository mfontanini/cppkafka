#include <stdexcept>
#include <cppkafka/mocking/kafka_cluster_registry.h>

using std::string;
using std::lock_guard;
using std::mutex;
using std::runtime_error;
using std::shared_ptr;
using std::weak_ptr;

namespace cppkafka {
namespace mocking {
namespace detail {

KafkaClusterRegistry& KafkaClusterRegistry::instance() {
    static KafkaClusterRegistry registry;
    return registry;
}

void KafkaClusterRegistry::add_cluster(shared_ptr<KafkaCluster> cluster) {
    const string& url = cluster->get_url();
    lock_guard<mutex> _(clusters_mutex_);
    auto iter = clusters_.find(url);
    if (iter != clusters_.end()) {
        throw runtime_error("cluster already registered");
    }
    clusters_.emplace(url, weak_ptr<KafkaCluster>(cluster));
}

void KafkaClusterRegistry::remove_cluster(const string& url) {
    lock_guard<mutex> _(clusters_mutex_);
    clusters_.erase(url);
}

shared_ptr<KafkaCluster> KafkaClusterRegistry::get_cluster(const string& name) const {
    lock_guard<mutex> _(clusters_mutex_);
    auto iter = clusters_.find(name);
    return iter != clusters_.end() ? iter->second.lock() : nullptr;
}

} // detail
} // mocking
} // cppkafka
