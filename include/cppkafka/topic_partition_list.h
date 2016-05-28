#ifndef CPPKAFKA_TOPIC_PARTITION_LIST_H
#define CPPKAFKA_TOPIC_PARTITION_LIST_H

#include <memory>
#include <algorithm>
#include <librdkafka/rdkafka.h>

namespace cppkafka {

class TopicPartition;

using TopicPartitionsListPtr = std::unique_ptr<rd_kafka_topic_partition_list_t, 
                                               decltype(&rd_kafka_topic_partition_list_destroy)>;
using TopicPartitionList = std::vector<TopicPartition>;

TopicPartitionsListPtr convert(const std::vector<TopicPartition>& topic_partitions);
std::vector<TopicPartition> convert(const TopicPartitionsListPtr& topic_partitions);
TopicPartitionsListPtr make_handle(rd_kafka_topic_partition_list_t* handle);

} // cppkafka

#endif // CPPKAFKA_TOPIC_PARTITION_LIST_H
