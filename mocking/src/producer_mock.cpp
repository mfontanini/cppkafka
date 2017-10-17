#include <cppkafka/mocking/producer_mock.h>
#include <cppkafka/mocking/events/produce_message_event.h>

using std::move;

namespace cppkafka {
namespace mocking {

ProducerMock::ProducerMock(ConfigurationMock config, EventProcessorPtr processor,
                           ClusterPtr cluster)
: HandleMock(move(processor), move(cluster)), config_(move(config)) {

}

void ProducerMock::produce_message(MessageHandle message_handle) {
    generate_event<ProduceMessageEvent>(move(message_handle));
}

} // mocking
} // cppkafka
