#include <librdkafka/rdkafka.h>
#include "topic_partition.h"

using std::string;

namespace cppkafka {

TopicPartition::TopicPartition(const string& topic) 
: TopicPartition(topic, RD_KAFKA_PARTITION_UA) {

}

TopicPartition::TopicPartition(const string& topic, int partition) 
: TopicPartition(topic, partition, RD_KAFKA_OFFSET_INVALID) {

}

TopicPartition::TopicPartition(const string& topic, int partition, int64_t offset) 
: topic_(topic), partition_(partition), offset_(offset) {

}

const string& TopicPartition::get_topic() const {
    return topic_;
}

int TopicPartition::get_partition() const {
    return partition_;
}

int64_t TopicPartition::get_offset() const {
    return offset_;
}

} // cppkafka
