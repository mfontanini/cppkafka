#ifndef CPPKAFKA_MOCKING_HANDLE_MOCK_H
#define CPPKAFKA_MOCKING_HANDLE_MOCK_H

#include <memory>
#include <utility>
#include <cppkafka/mocking/event_processor.h>

namespace cppkafka {
namespace mocking {

class KafkaCluster;

class HandleMock {
public:
    using ClusterPtr = std::shared_ptr<KafkaCluster>;
    using EventProcessorPtr = std::shared_ptr<EventProcessor>;

    HandleMock(EventProcessorPtr processor);
    HandleMock(EventProcessorPtr processor, ClusterPtr cluster);
    virtual ~HandleMock() = default;

    void set_cluster(ClusterPtr cluster);
protected:
    using EventPtr = EventProcessor::EventPtr;

    KafkaCluster& get_cluster();
    const KafkaCluster& get_cluster() const;
    void generate_event(EventPtr event);
    template <typename T, typename... Args>
    void generate_event(Args&&... args) {
        generate_event(EventPtr(new T(cluster_, std::forward<Args>(args)...)));
    }
private:
    EventProcessorPtr processor_;
    ClusterPtr cluster_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_HANDLE_MOCK_H
