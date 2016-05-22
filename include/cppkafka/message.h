#ifndef CPPKAFKA_MESSAGE_H
#define CPPKAFKA_MESSAGE_H

#include <memory>
#include <cstdint>
#include <librdkafka/rdkafka.h>
#include "buffer.h"
#include "topic.h"

namespace cppkafka {

class Message {
public:
    Message(rd_kafka_message_t* handle);
    Message(const Message&) = delete;
    Message(Message&& rhs) = default;
    Message& operator=(const Message&) = delete;
    Message& operator=(Message&& rhs) = default;

    bool has_error() const;
    rd_kafka_resp_err_t get_error() const;
    std::string get_topic() const;
    int get_partition() const;
    const Buffer& get_payload() const;
    const Buffer& get_key() const;
    int64_t get_offset() const;
    void* private_data();
    explicit operator bool() const;

    rd_kafka_message_t* get_handle() const;
private:
    using HandlePtr = std::unique_ptr<rd_kafka_message_t, decltype(&rd_kafka_message_destroy)>;

    HandlePtr handle_;
    Buffer payload_;
    Buffer key_;
};

} // cppkafka

#endif // CPPKAFKA_MESSAGE_H
