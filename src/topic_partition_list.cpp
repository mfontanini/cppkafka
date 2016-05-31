#include "topic_partition_list.h"
#include "topic_partition.h"
#include "exceptions.h"

using std::vector;

namespace cppkafka {

TopicPartitionsListPtr convert(const vector<TopicPartition>& topic_partitions) {
    TopicPartitionsListPtr handle(rd_kafka_topic_partition_list_new(topic_partitions.size()),
                                  &rd_kafka_topic_partition_list_destroy);
    for (const auto& item : topic_partitions) {
        rd_kafka_topic_partition_t* new_item = nullptr;
        new_item = rd_kafka_topic_partition_list_add(handle.get(),
                                                     item.get_topic().data(),
                                                     item.get_partition());
        new_item->offset = item.get_offset();
    }
    return handle;
}

vector<TopicPartition> convert(const TopicPartitionsListPtr& topic_partitions) {
    return convert(topic_partitions.get());
}

vector<TopicPartition> convert(rd_kafka_topic_partition_list_t* topic_partitions) {
    vector<TopicPartition> output;
    for (int i = 0; i < topic_partitions->cnt; ++i) {
        const auto& elem = topic_partitions->elems[i];
        output.emplace_back(elem.topic, elem.partition, elem.offset);
    }
    return output;
}

TopicPartitionsListPtr make_handle(rd_kafka_topic_partition_list_t* handle) {
    return TopicPartitionsListPtr(handle, &rd_kafka_topic_partition_list_destroy);
}

} // cppkafka
