#ifndef CPPKAFKA_MOCKING_KAFKA_PARTITION_MOCK_H
#define CPPKAFKA_MOCKING_KAFKA_PARTITION_MOCK_H

#include <deque>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <functional>
#include <cppkafka/mocking/kafka_message_mock.h>

namespace cppkafka {
namespace mocking {

class KafkaPartitionMock {
public:
    using MessageCallback = std::function<void(uint64_t offset)>;
    using SubscriberId = uint64_t;

    void add_message(KafkaMessageMock message);
    const KafkaMessageMock& get_message(uint64_t offset) const;
    size_t get_message_count() const;
    SubscriberId subscribe(MessageCallback callback);
    void unsubscribe(SubscriberId id);
private:
    std::vector<MessageCallback> get_subscriber_callbacks() const;

    uint64_t base_offset_{0};
    SubscriberId current_subscriber_id_{0};
    std::deque<KafkaMessageMock> messages_;
    std::unordered_map<SubscriberId, MessageCallback> subscribers_;
    mutable std::mutex messages_mutex_;
    mutable std::mutex subscribers_mutex_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_KAFKA_PARTITION_MOCK_H
