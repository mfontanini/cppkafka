#ifndef CPPKAFKA_MOCKING_KAFKA_PARTITION_H
#define CPPKAFKA_MOCKING_KAFKA_PARTITION_H

#include <deque>
#include <mutex>
#include <cppkafka/mocking/kafka_message.h>

namespace cppkafka {
namespace mocking {

class KafkaPartition {
public:
    void add_message(KafkaMessage message);
    const KafkaMessage& get_message(uint64_t offset) const;
    size_t get_message_count() const;
private:
    uint64_t base_offset_{0};
    std::deque<KafkaMessage> messages_;
    mutable std::mutex messages_mutex_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_PARTITION_H
