#ifndef CPPKAFKA_MOCKING_PRODUCER_MOCK_H
#define CPPKAFKA_MOCKING_PRODUCER_MOCK_H

#include <cppkafka/mocking/handle_mock.h>
#include <cppkafka/mocking/message_handle.h>

namespace cppkafka {
namespace mocking {

class ProducerMock : public HandleMock {
public:
    using ClusterPtr = std::shared_ptr<KafkaCluster>;

    using HandleMock::HandleMock;

    void produce_message(MessageHandle message_handle);
private:

};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_PRODUCER_MOCK_H
