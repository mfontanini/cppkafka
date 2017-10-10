#include <stdexcept>
#include <cppkafka/mocking/partition_mock.h>

using std::lock_guard;
using std::mutex;
using std::out_of_range;
using std::move;

namespace cppkafka {
namespace mocking {

void PartitionMock::add_message(MessageMock message) {
    lock_guard<mutex> _(messages_mutex_);
    messages_.emplace_back(move(message));
}

const MessageMock& PartitionMock::get_message(uint64_t offset) const {
    const uint64_t index = offset - base_offset_;
    lock_guard<mutex> _(messages_mutex_);
    if (messages_.size() >= index) {
        throw out_of_range("invalid message index");
    }
    return messages_[index];
}

size_t PartitionMock::get_message_count() const {
    lock_guard<mutex> _(messages_mutex_);
    return messages_.size();
}

} // mocking
} // cppkafka
