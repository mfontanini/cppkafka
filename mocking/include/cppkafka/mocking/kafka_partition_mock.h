#ifndef CPPKAFKA_MOCKING_KAFKA_PARTITION_MOCK_H
#define CPPKAFKA_MOCKING_KAFKA_PARTITION_MOCK_H

#include <deque>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <functional>
#include <tuple>
#include <cppkafka/mocking/kafka_message_mock.h>

namespace cppkafka {
namespace mocking {

class KafkaPartitionMock {
public:
    using MessageCallback = std::function<void(uint64_t offset, const KafkaMessageMock&)>;
    using SubscriberId = uint64_t;

    void add_message(KafkaMessageMock message);
    const KafkaMessageMock& get_message(uint64_t offset) const;
    size_t get_message_count() const;
    SubscriberId subscribe(MessageCallback callback);
    void unsubscribe(SubscriberId id);
    // Returns interval [lowest offset, largest offset)
    std::tuple<int64_t, int64_t> get_offset_bounds() const;

    // Acquire this partition so that no messages can be produced while the callback is executed.
    template <typename Functor>
    void acquire(const Functor& functor);
private:
    std::vector<MessageCallback> get_subscriber_callbacks() const;

    uint64_t base_offset_{0};
    SubscriberId current_subscriber_id_{0};
    std::deque<KafkaMessageMock> messages_;
    std::unordered_map<SubscriberId, MessageCallback> subscribers_;
    mutable std::recursive_mutex mutex_;
};

template <typename Functor>
void KafkaPartitionMock::acquire(const Functor& functor) {
    std::lock_guard<std::recursive_mutex> _(mutex_);
    functor();
}

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_PARTITION_MOCK_H
