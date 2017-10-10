#ifndef CPPKAFKA_MOCKING_PARTITION_MOCK_H
#define CPPKAFKA_MOCKING_PARTITION_MOCK_H

#include <deque>
#include <mutex>
#include <cppkafka/mocking/message_mock.h>

namespace cppkafka {
namespace mocking {

class PartitionMock {
public:
    void add_message(MessageMock message);
    const MessageMock& get_message(uint64_t offset) const;
    size_t get_message_count() const;
private:
    uint64_t base_offset_{0};
    std::deque<MessageMock> messages_;
    mutable std::mutex messages_mutex_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_PARTITION_MOCK_H
