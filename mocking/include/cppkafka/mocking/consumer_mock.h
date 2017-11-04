#ifndef CPPKAFKA_MOCKING_CONSUMER_MOCK_H
#define CPPKAFKA_MOCKING_CONSUMER_MOCK_H

#include <vector>
#include <string>
#include <cstdint>
#include <map>
#include <set>
#include <tuple>
#include <unordered_set>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <chrono>
#include <cppkafka/mocking/handle_mock.h>
#include <cppkafka/mocking/configuration_mock.h>
#include <cppkafka/mocking/topic_partition_mock.h>
#include <cppkafka/mocking/message_handle.h>
#include <cppkafka/mocking/kafka_cluster.h>

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

    void close();
    void subscribe(const std::vector<std::string>& topics);
    void unsubscribe();
    void assign(const std::vector<TopicPartitionMock>& topic_partitions);
    void unassign();
    void pause_partitions(const std::vector<TopicPartitionMock>& topic_partitions);
    void resume_partitions(const std::vector<TopicPartitionMock>& topic_partitions);
    std::unique_ptr<MessageHandle> poll(std::chrono::milliseconds timeout);
    std::vector<TopicPartitionMock> get_assignment() const;
private:
    static uint64_t make_consumer_id();

    struct MessageAggregate {
        std::string topic;
        unsigned partition;
        uint64_t offset;
        const KafkaMessageMock* message;
    };

    struct TopicPartitionInfo {
        uint64_t next_offset;
        std::queue<MessageAggregate> messages;
    };

    using TopicPartitionId = std::tuple<std::string, int>;

    static TopicPartitionId make_id(const TopicPartitionMock& topic_partition);
    KafkaCluster::ResetOffsetPolicy get_offset_policy() const;
    bool get_partition_eof_enabled() const;
    void on_assignment(const std::vector<TopicPartitionMock>& topic_partitions);
    void on_revocation();
    void on_message(const std::string& topic_name, unsigned partition, uint64_t offset,
                    const KafkaMessageMock& message);
    void handle_rebalance(rd_kafka_resp_err_t type,
                          const std::vector<TopicPartitionMock>& topic_partitions);

    ConfigurationMock config_;
    std::string group_id_;
    const KafkaCluster::ResetOffsetPolicy offset_reset_policy_;
    bool emit_eofs_;
    std::map<TopicPartitionId, TopicPartitionInfo> assigned_partitions_;
    std::set<TopicPartitionId> consumable_topic_partitions_;
    std::set<TopicPartitionId> paused_topic_partitions_;
    mutable std::mutex mutex_;
    std::condition_variable messages_condition_;
    uint64_t consumer_id_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_CONSUMER_MOCK_H
