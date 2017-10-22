#include <tuple>
#include <cppkafka/mocking/topic_partition_mock.h>

using std::string;
using std::vector;
using std::tie;
using std::unique_ptr;

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

TopicPartitionMockListPtr to_rdkafka_handle(const vector<TopicPartitionMock>& topic_partitions){
    const size_t count = topic_partitions.size();
    TopicPartitionMockListPtr output{rd_kafka_topic_partition_list_new(count),
                                     &rd_kafka_topic_partition_list_destroy};
    for (const TopicPartitionMock& topic_partition : topic_partitions) {
        auto* ptr = rd_kafka_topic_partition_list_add(output.get(),
                                                      topic_partition.get_topic().data(),
                                                      topic_partition.get_partition());
        ptr->offset = topic_partition.get_offset();
    }
    return output;
}

vector<TopicPartitionMock>
from_rdkafka_handle(const rd_kafka_topic_partition_list_t& topic_partitions){
    vector<TopicPartitionMock> output;
    for (int i = 0; i < topic_partitions.cnt; ++i) {
        const auto& topic_partition = topic_partitions.elems[i];
        output.emplace_back(topic_partition.topic, topic_partition.partition,
                            topic_partition.offset);
    }
    return output;
}

} // mocking
} // cppkafka
