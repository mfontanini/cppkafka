#ifndef CPPKAFKA_MOCKING_PRODUCER_MOCK_H
#define CPPKAFKA_MOCKING_PRODUCER_MOCK_H

#include <cppkafka/mocking/handle_mock.h>
#include <cppkafka/mocking/configuration_mock.h>
#include <cppkafka/mocking/message_handle.h>

namespace cppkafka {
namespace mocking {

class ProducerMock : public HandleMock {
public:
    using ClusterPtr = std::shared_ptr<KafkaCluster>;

    ProducerMock(ConfigurationMock config, EventProcessorPtr processor, ClusterPtr cluster);

    void produce_message(MessageHandle message_handle);
private:
    ConfigurationMock config_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_PRODUCER_MOCK_H
