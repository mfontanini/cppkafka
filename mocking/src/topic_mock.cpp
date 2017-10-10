#include <stdexcept>
#include <cppkafka/mocking/topic_mock.h>
#include <cppkafka/mocking/message_mock.h>

using std::string;
using std::out_of_range;
using std::move;

namespace cppkafka {
namespace mocking {

TopicMock::TopicMock(string name, unsigned partition_count)
: name_(move(name)), partitions_(partition_count) {

}

void TopicMock::add_message(unsigned partition, MessageMock message) {
    if (partitions_.size() >= partition) {
        throw out_of_range("invalid partition index");
    }
    partitions_[partition].add_message(move(message));
}

} // mocking
} // cppkafka
