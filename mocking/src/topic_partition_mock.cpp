#include <cppkafka/mocking/topic_partition_mock.h>

using std::string;

namespace cppkafka {
namespace mocking {

TopicPartitionMock::TopicPartitionMock(string topic, int partition, int64_t offset)
: topic_(move(topic)), partition_(partition), offset_(offset) {

}

const string& TopicPartitionMock::get_topic() const {
    return topic_;
}

int TopicPartitionMock::get_partition() const {
    return partition_;
}

int64_t TopicPartitionMock::get_offset() const {
    return offset_;
}

void TopicPartitionMock::set_offset(int64_t offset) {
    offset_ = offset;
}

} // mocking
} // cppkafka
