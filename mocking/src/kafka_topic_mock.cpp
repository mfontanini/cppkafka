#include <stdexcept>
#include <cppkafka/mocking/kafka_topic_mock.h>
#include <cppkafka/mocking/kafka_message_mock.h>

using std::string;
using std::move;
using std::lock_guard;
using std::mutex;
using std::vector;
using std::out_of_range;
using std::runtime_error;

namespace cppkafka {
namespace mocking {

KafkaTopicMock::KafkaTopicMock(string name, unsigned partition_count)
: name_(move(name)), partitions_(partition_count) {

}

const string& KafkaTopicMock::get_name() const {
    return name_;
}

void KafkaTopicMock::add_message(unsigned partition, KafkaMessageMock message) {
    if (partition >= partitions_.size()) {
        throw out_of_range("invalid partition index");
    }
    partitions_[partition].add_message(move(message));
}

KafkaPartitionMock& KafkaTopicMock::get_partition(unsigned partition) {
    if (partition >= partitions_.size()) {
        throw runtime_error("partition doesn't exist");
    }
    return partitions_[partition];
}

const KafkaPartitionMock& KafkaTopicMock::get_partition(unsigned partition) const {
    return const_cast<KafkaTopicMock&>(*this).get_partition(partition);
}

size_t KafkaTopicMock::get_partition_count() const {
    return partitions_.size();
}

} // mocking
} // cppkafka
