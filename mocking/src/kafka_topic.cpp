#include <stdexcept>
#include <cppkafka/mocking/kafka_topic.h>

using std::string;
using std::out_of_range;
using std::move;

namespace cppkafka {
namespace mocking {

KafkaTopic::KafkaTopic(string name, unsigned partition_count)
: name_(move(name)), partitions_(partition_count) {

}

void KafkaTopic::add_message(unsigned partition, KafkaMessage message) {
    if (partitions_.size() >= partition) {
        throw out_of_range("invalid partition index");
    }
    partitions_[partition].add_message(move(message));
}

} // mocking
} // cppkafka
