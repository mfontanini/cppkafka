#include <cppkafka/mocking/events/produce_message_event.h>
#include <cppkafka/mocking/kafka_cluster.h>

using std::string;
using std::move;

namespace cppkafka {
namespace mocking {

ProduceMessageEvent::ProduceMessageEvent(ClusterPtr cluster, MessageHandle message_handle)
: EventBase(move(cluster)), message_handle_(move(message_handle)) {

}

string ProduceMessageEvent::get_type() const {
    return "produce message";
}

void ProduceMessageEvent::execute_event(KafkaCluster& cluster) {
    const string& topic =  message_handle_.get_topic().get_topic();
    const int partition = message_handle_.get_message().partition;
    cluster.produce(topic, partition, message_handle_.make_message_mock());
}

} // mocking
} // cppkafka
