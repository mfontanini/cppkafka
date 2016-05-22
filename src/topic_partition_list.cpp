#include "topic_partition_list.h"
#include "topic_partition.h"

namespace cppkafka {

const size_t TopicPartitionList::DEFAULT_CONTAINER_SIZE = 5;

TopicPartitionList::TopicPartitionList() 
: TopicPartitionList(DEFAULT_CONTAINER_SIZE) {

}

TopicPartitionList::TopicPartitionList(size_t size) 
: handle_(make_handle(rd_kafka_topic_partition_list_new(size))) {

}

TopicPartitionList::TopicPartitionList(const TopicPartitionList& rhs) 
: handle_(make_handle(rd_kafka_topic_partition_list_copy(rhs.get_handle()))) {

}

TopicPartitionList& TopicPartitionList::operator=(const TopicPartitionList& rhs) {
    handle_.reset(rd_kafka_topic_partition_list_copy(rhs.get_handle()));
    return *this;
}

void TopicPartitionList::add(const TopicPartition& topic_partition) {
    rd_kafka_topic_partition_t* element = nullptr;
    element = rd_kafka_topic_partition_list_add(handle_.get(),
                                                topic_partition.get_topic().data(),
                                                topic_partition.get_partition());
    element->offset = topic_partition.get_offset();
}

size_t TopicPartitionList::size() const {
    return handle_->cnt;
}

bool TopicPartitionList::empty() const {
    return size() == 0;
}

rd_kafka_topic_partition_list_t* TopicPartitionList::get_handle() const {
    return handle_.get();
}

TopicPartitionList::HandlePtr
TopicPartitionList::make_handle(rd_kafka_topic_partition_list_t* ptr) {
    return HandlePtr(ptr, &rd_kafka_topic_partition_list_destroy);
}

} // cppkafka
