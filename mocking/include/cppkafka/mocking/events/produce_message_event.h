#ifndef CPPKAFKA_MOCKING_PRODUCE_MESSAGE_EVENT_H
#define CPPKAFKA_MOCKING_PRODUCE_MESSAGE_EVENT_H

#include <memory>
#include <librdkafka/rdkafka.h>
#include <cppkafka/mocking/events/event_base.h>
#include <cppkafka/mocking/message_handle.h>

namespace cppkafka {
namespace mocking {

class ProduceMessageEvent : public EventBase {
public:
    ProduceMessageEvent(ClusterPtr cluster, MessageHandle message_handle);

    std::string get_type() const;
private:
    void execute_event(KafkaCluster& cluster);

    MessageHandle message_handle_;
    std::string topic_;
    unsigned partition_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_PRODUCE_MESSAGE_EVENT_H
