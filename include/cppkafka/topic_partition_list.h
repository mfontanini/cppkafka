#ifndef CPPKAFKA_TOPIC_PARTITION_LIST_H
#define CPPKAFKA_TOPIC_PARTITION_LIST_H

#include <memory>
#include <algorithm>
#include <librdkafka/rdkafka.h>
#include "topic_partition.h"

namespace cppkafka {

class TopicPartition;

class TopicPartitionList {
public:
    TopicPartitionList();
    TopicPartitionList(size_t size);
    template <typename ForwardIterator>
    TopicPartitionList(ForwardIterator start, const ForwardIterator& end) 
    : TopicPartitionList(std::distance(start, end)) {
        while (start != end) {
            add(*start);
            ++start;
        }
    }
    TopicPartitionList(const TopicPartitionList& rhs);
    TopicPartitionList(TopicPartitionList&&) = default;
    TopicPartitionList& operator=(const TopicPartitionList& rhs);
    TopicPartitionList& operator=(TopicPartitionList&&) = default;

    void add(const TopicPartition& topic_partition);
    size_t size() const;
    bool empty() const;

    rd_kafka_topic_partition_list_t* get_handle() const;
private:
    static const size_t DEFAULT_CONTAINER_SIZE;

    using HandlePtr = std::unique_ptr<rd_kafka_topic_partition_list_t, 
                                      decltype(&rd_kafka_topic_partition_list_destroy)>;

    static HandlePtr make_handle(rd_kafka_topic_partition_list_t* ptr);

    HandlePtr handle_;
};

} // cppkafka

#endif // CPPKAFKA_TOPIC_PARTITION_LIST_H
