#include <stdexcept>
#include <cppkafka/mocking/handle_mock.h>

using std::runtime_error;
using std::move;

namespace cppkafka {
namespace mocking {

HandleMock::HandleMock(EventProcessorPtr processor)
: processor_(move(processor)) {

}

HandleMock::HandleMock(EventProcessorPtr processor, ClusterPtr cluster)
: processor_(move(processor)), cluster_(move(cluster)) {

}

void HandleMock::set_cluster(ClusterPtr cluster) {
    // Don't allow changing the cluster
    if (cluster_) {
        throw runtime_error("can't change the cluster");
    }
    cluster_ = move(cluster);
}

KafkaCluster& HandleMock::get_cluster() {
    if (!cluster_) {
        throw runtime_error("cluster not set");
    }
    return *cluster_;
}

const KafkaCluster& HandleMock::get_cluster() const {
    if (!cluster_) {
        throw runtime_error("cluster not set");
    }
    return *cluster_;
}

void HandleMock::generate_event(EventPtr event) {
    processor_->add_event(move(event));
}

} // mocking
} // cppkafka
