#include <stdexcept>
#include <cppkafka/mocking/kafka_cluster.h>
#include <cppkafka/mocking/kafka_cluster_registry.h>

using std::string;
using std::invalid_argument;
using std::piecewise_construct;
using std::forward_as_tuple;
using std::move;

namespace cppkafka {
namespace mocking {

KafkaCluster::KafkaCluster(string url)
: url_(move(url)) {
    detail::KafkaClusterRegistry::instance().add_cluster(this);
}

KafkaCluster::~KafkaCluster() {
    detail::KafkaClusterRegistry::instance().remove_cluster(this);    
}

const string& KafkaCluster::get_url() const {
    return url_;
}

void KafkaCluster::add_topic(const string& name, unsigned partitions) {
    topics_.emplace(piecewise_construct, forward_as_tuple(name),
                    forward_as_tuple(name, partitions));
}

void KafkaCluster::produce(const string& topic, unsigned partition, KafkaMessage message) {
    auto iter = topics_.find(topic);
    if (iter == topics_.end()) {
        throw invalid_argument("topic does not exist");
    }
    iter->second.add_message(partition, move(message));
}

} // mocking
} // cppkafka
