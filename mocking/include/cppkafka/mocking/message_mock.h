#ifndef CPPKAFKA_MOCKING_MESSAGE_MOCK_H
#define CPPKAFKA_MOCKING_MESSAGE_MOCK_H

#include <vector>
#include <cstdint>
#include <librdkafka/rdkafka.h>

namespace cppkafka {
namespace mocking {

class MessageMock {
public:
    using Buffer = std::vector<uint8_t>;

    MessageMock(Buffer key, Buffer payload, rd_kafka_timestamp_type_t timestamp_type,
                int64_t timestamp);

    const Buffer& get_key() const;
    const Buffer& get_payload() const;
    rd_kafka_timestamp_type_t get_timestamp_type() const;
    int64_t get_timestamp() const;
private:
    const Buffer key_;
    const Buffer payload_;
    rd_kafka_timestamp_type_t timestamp_type_;
    int64_t timestamp_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_MESSAGE_MOCK_H
