#include <algorithm>
#include <cppkafka/mocking/message_handle.h>
#include <cppkafka/mocking/kafka_message_mock.h>

using std::unique_ptr;
using std::move;
using std::swap;

namespace cppkafka {
namespace mocking {

MessageHandlePrivateData::MessageHandlePrivateData(rd_kafka_timestamp_type_t timestamp_type,
                                                   int64_t timestamp)
: timestamp_type_(timestamp_type), timestamp_(timestamp) {

}

rd_kafka_timestamp_type_t MessageHandlePrivateData::get_timestamp_type() const {
    return timestamp_type_;
}

int64_t MessageHandlePrivateData::get_timestamp() const {
    return timestamp_;
}

MessageHandle::MessageHandle(unique_ptr<TopicHandle> topic, int partition, int64_t offset,
                             void* key, size_t key_size, void* payload, size_t payload_size,
                             int error_code, MessageHandlePrivateData private_data,
                             PointerOwnership ownership)
: topic_(move(topic)), private_data_(private_data), ownership_(ownership) {
    message_.rkt = reinterpret_cast<rd_kafka_topic_t*>(topic_.get());
    message_.partition = partition;
    message_.payload = payload;
    message_.len = payload_size;
    message_.key = key;
    message_.key_len = key_size;
    message_.offset = offset;
    set_private_data_pointer();
}

MessageHandle::MessageHandle(MessageHandle&& other)
: ownership_(PointerOwnership::Unowned) {
    *this = move(other);
}

MessageHandle& MessageHandle::operator=(MessageHandle&& other) {
    swap(topic_, other.topic_);
    swap(message_, other.message_);
    swap(ownership_, other.ownership_);
    set_private_data_pointer();
    other.set_private_data_pointer();
    return *this;
}

MessageHandle::~MessageHandle() {
    if (ownership_ == PointerOwnership::Owned) {
        free(message_.payload);
        free(message_.key);
    }
}

const TopicHandle& MessageHandle::get_topic() const {
    return *topic_;
}

const rd_kafka_message_t& MessageHandle::get_message() const {
    return message_;
}

KafkaMessageMock MessageHandle::make_message_mock() const {
    auto key_ptr = reinterpret_cast<const char*>(message_.key);
    auto payload_ptr = reinterpret_cast<const char*>(message_.payload);
    return {
        KafkaMessageMock::Buffer(key_ptr, key_ptr + message_.key_len),
        KafkaMessageMock::Buffer(payload_ptr, payload_ptr + message_.len),
        private_data_.get_timestamp_type(),
        private_data_.get_timestamp()
    };
}

void MessageHandle::set_private_data_pointer() {
    message_._private = reinterpret_cast<void*>(&private_data_);
}

} // mocking
} // cppkafka
