#include <cppkafka/mocking/message_mock.h>

namespace cppkafka {
namespace mocking {

MessageMock::MessageMock(Buffer key, Buffer payload, rd_kafka_timestamp_type_t timestamp_type,
                         int64_t timestamp)
: key_(move(key)), payload_(move(payload)), timestamp_type_(timestamp_type),
  timestamp_(timestamp) {

}

const MessageMock::Buffer& MessageMock::get_key() const {
    return key_;
}

const MessageMock::Buffer& MessageMock::get_payload() const {
    return payload_;
}

rd_kafka_timestamp_type_t MessageMock::get_timestamp_type() const {
    return timestamp_type_;
}

int64_t MessageMock::get_timestamp() const {
    return timestamp_;
}

} // mocking
} // cppkafka
