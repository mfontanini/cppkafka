#include <cppkafka/mocking/producer_mock.h>
#include <cppkafka/mocking/events/produce_message_event.h>

using std::move;

using std::chrono::milliseconds;

namespace cppkafka {
namespace mocking {

ProducerMock::ProducerMock(ConfigurationMock config, EventProcessorPtr processor,
                           ClusterPtr cluster)
: HandleMock(move(processor), move(cluster)), config_(move(config)) {

}

void ProducerMock::produce(MessageHandle message_handle) {
    generate_event<ProduceMessageEvent>(move(message_handle));
}

bool ProducerMock::flush(milliseconds timeout) {
    // TODO: produce buffered events
    return get_event_processor().wait_until_empty(timeout);
}

size_t ProducerMock::poll(milliseconds timeout) {
    // TODO: do something
    return 0;
}

} // mocking
} // cppkafka
