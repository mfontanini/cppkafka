#ifndef CPPKAFKA_MOCKING_CONSUMER_MOCK_H
#define CPPKAFKA_MOCKING_CONSUMER_MOCK_H

#include <vector>
#include <string>
#include <cstdint>
#include <map>
#include <tuple>
#include <unordered_set>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <cppkafka/mocking/handle_mock.h>
#include <cppkafka/mocking/configuration_mock.h>
#include <cppkafka/mocking/topic_partition_mock.h>

namespace cppkafka {
namespace mocking {

class KafkaMessageMock;
class KafkaTopicMock;

class ConsumerMock : public HandleMock {
public:
    ConsumerMock(ConfigurationMock config, EventProcessorPtr processor, ClusterPtr cluster);
    ConsumerMock(const ConsumerMock&) = delete;
    ConsumerMock& operator=(const ConsumerMock&) = delete;
    ~ConsumerMock();

    void subscribe(const std::vector<std::string>& topics);
    void assign(const std::vector<TopicPartitionMock>& topic_partitions);
    void unassign();
    void set_opaque(void* opaque);
private:
    static uint64_t make_consumer_id();

    struct TopicPartitionInfo {
        TopicPartitionInfo(uint64_t next_offset);

        uint64_t next_offset;
        bool paused{false};
    };

    struct MessageAggregate {
        std::string topic;
        unsigned partition;
        uint64_t offset;
        const KafkaMessageMock* message;
    };

    using TopicPartitionId = std::tuple<std::string, int>;

    static TopicPartitionId make_id(const TopicPartitionMock& topic_partition);
    void on_assignment(const std::vector<TopicPartitionMock>& topic_partitions);
    void on_revocation();
    void on_message(const std::string& topic_name, unsigned partition, uint64_t offset,
                    const KafkaMessageMock* message);
    void handle_rebalance(rd_kafka_resp_err_t type,
                          const std::vector<TopicPartitionMock>& topic_partitions);
    void handle_assign(const TopicPartitionMock& topic_partition);
    void handle_unassign();
    void fetch_existing_messages(unsigned partition, uint64_t next_offset,
                                 KafkaTopicMock& topic);

    ConfigurationMock config_;
    const std::string group_id_;
    std::map<TopicPartitionId, TopicPartitionInfo> assigned_partitions_;
    mutable std::mutex assigned_partitions_mutex_;
    std::queue<MessageAggregate> new_message_queue_;
    std::queue<MessageAggregate> available_message_queue_;
    mutable std::mutex message_queue_mutex_;
    std::condition_variable message_queue_condition_;
    void* opaque_;
    uint64_t consumer_id_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_CONSUMER_MOCK_H
