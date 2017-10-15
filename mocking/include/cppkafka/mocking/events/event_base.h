#ifndef CPPKAFKA_MOCKING_EVENT_BASE_H
#define CPPKAFKA_MOCKING_EVENT_BASE_H

#include <memory>
#include <string>

namespace cppkafka {
namespace mocking {

class KafkaCluster;

class EventBase {
public:
    using ClusterPtr = std::shared_ptr<KafkaCluster>;

    EventBase(ClusterPtr cluster);
    virtual ~EventBase() = default;

    void execute();
    virtual std::string get_type() const = 0;
private:
    virtual void execute_event(KafkaCluster& cluster) = 0;

    ClusterPtr cluster_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_EVENT_BASE_H
