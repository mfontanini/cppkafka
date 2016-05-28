#include <librdkafka/rdkafka.h>
#include "topic_partition.h"

using std::string;

namespace cppkafka {

TopicPartition::TopicPartition() 
: TopicPartition("") {

}

TopicPartition::TopicPartition(const char* topic) 
: TopicPartition(string(topic)) {

}

TopicPartition::TopicPartition(string topic) 
: TopicPartition(move(topic), RD_KAFKA_PARTITION_UA) {

}

TopicPartition::TopicPartition(string topic, int partition) 
: TopicPartition(move(topic), partition, RD_KAFKA_OFFSET_INVALID) {

}

TopicPartition::TopicPartition(string topic, int partition, int64_t offset) 
: topic_(move(topic)), partition_(partition), offset_(offset) {

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
