#include <cppkafka/mocking/events/event_base.h>
#include <cppkafka/mocking/kafka_cluster.h>

namespace cppkafka {
namespace mocking {

EventBase::EventBase(ClusterPtr cluster)
: cluster_(move(cluster)) {

}

void EventBase::execute() {
    execute_event(*cluster_);
}

} // mocking
} // cppkafka
