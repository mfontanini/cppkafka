#include <cppkafka/mocking/kafka_message_mock.h>

namespace cppkafka {
namespace mocking {

KafkaMessageMock::KafkaMessageMock(Buffer key, Buffer payload,
                                   rd_kafka_timestamp_type_t timestamp_type, int64_t timestamp)
: key_(move(key)), payload_(move(payload)), timestamp_type_(timestamp_type),
  timestamp_(timestamp) {

}

const KafkaMessageMock::Buffer& KafkaMessageMock::get_key() const {
    return key_;
}

const KafkaMessageMock::Buffer& KafkaMessageMock::get_payload() const {
    return payload_;
}

rd_kafka_timestamp_type_t KafkaMessageMock::get_timestamp_type() const {
    return timestamp_type_;
}

int64_t KafkaMessageMock::get_timestamp() const {
    return timestamp_;
}

} // mocking
} // cppkafka
