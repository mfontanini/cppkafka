#ifndef CPPKAFKA_MOCKING_MESSAGE_HANDLE_H
#define CPPKAFKA_MOCKING_MESSAGE_HANDLE_H

#include <string>
#include <memory>
#include <librdkafka/rdkafka.h>
#include <cppkafka/mocking/topic_handle.h>

namespace cppkafka {
namespace mocking {

class KafkaMessageMock;

class MessageHandlePrivateData {
public:
    MessageHandlePrivateData() = default;
    MessageHandlePrivateData(rd_kafka_timestamp_type_t timestamp_type, int64_t timestamp);

    rd_kafka_timestamp_type_t get_timestamp_type() const;
    int64_t get_timestamp() const;
private:
    rd_kafka_timestamp_type_t timestamp_type_;
    int64_t timestamp_;
};

class MessageHandle {
public:
    enum class PointerOwnership {
        Owned,
        Unowned
    };

    MessageHandle(std::unique_ptr<TopicHandle> topic, int partition, int64_t offset, void* key,
                  size_t key_size, void* payload, size_t payload_size, int error_code,
                  MessageHandlePrivateData private_data, PointerOwnership ownership);
    MessageHandle(MessageHandle&& other);
    MessageHandle& operator=(MessageHandle&& other);
    ~MessageHandle();

    const TopicHandle& get_topic() const;
    const rd_kafka_message_t& get_message() const;
    KafkaMessageMock make_message_mock() const;
private:
    void set_private_data_pointer();

    std::unique_ptr<TopicHandle> topic_;
    rd_kafka_message_t message_{};
    MessageHandlePrivateData private_data_;
    PointerOwnership ownership_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_MESSAGE_HANDLE_H
