#include <stdexcept>
#include <tuple>
#include <cppkafka/mocking/kafka_partition_mock.h>

using std::vector;
using std::lock_guard;
using std::mutex;
using std::out_of_range;
using std::move;
using std::tuple;
using std::tie;
using std::make_tuple;

namespace cppkafka {
namespace mocking {

void KafkaPartitionMock::add_message(KafkaMessageMock message) {
    KafkaMessageMock* message_ptr;
    uint64_t offset;
    tie(message_ptr, offset) = [&] {
        lock_guard<mutex> _(messages_mutex_);
        messages_.emplace_back(move(message));
        return make_tuple(&messages_.back(), messages_.size() - 1);
    }();

    const vector<MessageCallback> callbacks = get_subscriber_callbacks();
    for (const MessageCallback& callback : callbacks) {
        callback(offset, message_ptr);
    }
}

const KafkaMessageMock& KafkaPartitionMock::get_message(uint64_t offset) const {
    const uint64_t index = offset - base_offset_;
    lock_guard<mutex> _(messages_mutex_);
    if (messages_.size() >= index) {
        throw out_of_range("invalid message index");
    }
    return messages_[index];
}

size_t KafkaPartitionMock::get_message_count() const {
    lock_guard<mutex> _(messages_mutex_);
    return messages_.size();
}

KafkaPartitionMock::SubscriberId KafkaPartitionMock::subscribe(MessageCallback callback) {
    lock_guard<mutex> _(subscribers_mutex_);
    auto id = current_subscriber_id_++;
    subscribers_.emplace(id, move(callback));
    return id;
}

void KafkaPartitionMock::unsubscribe(SubscriberId id) {
    lock_guard<mutex> _(subscribers_mutex_);
    subscribers_.erase(id);
}

tuple<uint64_t, uint64_t> KafkaPartitionMock::get_offset_bounds() const {
    lock_guard<mutex> _(messages_mutex_);
    return make_tuple(base_offset_, base_offset_ + messages_.size());
}

vector<KafkaPartitionMock::MessageCallback> KafkaPartitionMock::get_subscriber_callbacks() const {
    lock_guard<mutex> _(subscribers_mutex_);
    vector<MessageCallback> output;
    for (const auto& subcriber_pair : subscribers_) {
        output.emplace_back(subcriber_pair.second);
    }
    return output;
}

} // mocking
} // cppkafka
