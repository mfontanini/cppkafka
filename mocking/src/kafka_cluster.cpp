#include <stdexcept>
#include <cppkafka/mocking/kafka_cluster.h>
#include <cppkafka/mocking/kafka_cluster_registry.h>

using std::shared_ptr;
using std::make_shared;
using std::string;
using std::invalid_argument;
using std::piecewise_construct;
using std::forward_as_tuple;
using std::move;
using std::lock_guard;
using std::mutex;

namespace cppkafka {
namespace mocking {

shared_ptr<KafkaCluster> KafkaCluster::make_cluster(string url) {
    shared_ptr<KafkaCluster> output{ new KafkaCluster(move(url)) };
    detail::KafkaClusterRegistry::instance().add_cluster(output);
    return output;
}

KafkaCluster::KafkaCluster(string url)
: url_(move(url)), offset_manager_(make_shared<OffsetManager>()) {

}

KafkaCluster::~KafkaCluster() {
    detail::KafkaClusterRegistry::instance().remove_cluster(*this);    
}

const string& KafkaCluster::get_url() const {
    return url_;
}

void KafkaCluster::add_topic(const string& name, unsigned partitions) {
    lock_guard<mutex> _(topics_mutex_);
    topics_.emplace(piecewise_construct, forward_as_tuple(name),
                    forward_as_tuple(name, partitions, offset_manager_));
}

bool KafkaCluster::topic_exists(const string& name) const {
    lock_guard<mutex> _(topics_mutex_);
    return topics_.count(name) > 0;
}

void KafkaCluster::produce(const string& topic, unsigned partition, KafkaMessageMock message) {
    auto iter = topics_.find(topic);
    if (iter == topics_.end()) {
        throw invalid_argument("topic does not exist");
    }
    iter->second.add_message(partition, move(message));
}

} // mocking
} // cppkafka
