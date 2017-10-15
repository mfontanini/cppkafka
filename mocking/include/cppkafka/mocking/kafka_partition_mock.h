#ifndef CPPKAFKA_MOCKING_KAFKA_PARTITION_MOCK_H
#define CPPKAFKA_MOCKING_KAFKA_PARTITION_MOCK_H

#include <deque>
#include <mutex>
#include <cppkafka/mocking/kafka_message_mock.h>

namespace cppkafka {
namespace mocking {

class KafkaPartitionMock {
public:
    void add_message(KafkaMessageMock message);
    const KafkaMessageMock& get_message(uint64_t offset) const;
    size_t get_message_count() const;
private:
    uint64_t base_offset_{0};
    std::deque<KafkaMessageMock> messages_;
    mutable std::mutex messages_mutex_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_PARTITION_MOCK_H
