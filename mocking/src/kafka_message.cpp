#include <cppkafka/mocking/kafka_message.h>

namespace cppkafka {
namespace mocking {

KafkaMessage::KafkaMessage(Buffer key, Buffer payload, rd_kafka_timestamp_type_t timestamp_type,
                           int64_t timestamp)
: key_(move(key)), payload_(move(payload)), timestamp_type_(timestamp_type),
  timestamp_(timestamp) {

}

const KafkaMessage::Buffer& KafkaMessage::get_key() const {
    return key_;
}

const KafkaMessage::Buffer& KafkaMessage::get_payload() const {
    return payload_;
}

rd_kafka_timestamp_type_t KafkaMessage::get_timestamp_type() const {
    return timestamp_type_;
}

int64_t KafkaMessage::get_timestamp() const {
    return timestamp_;
}

} // mocking
} // cppkafka
