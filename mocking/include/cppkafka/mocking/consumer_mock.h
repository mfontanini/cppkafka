#ifndef CPPKAFKA_MOCKING_CONSUMER_MOCK_H
#define CPPKAFKA_MOCKING_CONSUMER_MOCK_H

#include <vector>
#include <string>
#include <cstdint>
#include <unordered_set>
#include <cppkafka/mocking/handle_mock.h>
#include <cppkafka/mocking/configuration_mock.h>
#include <cppkafka/mocking/topic_partition_mock.h>

namespace cppkafka {
namespace mocking {

class ConsumerMock : public HandleMock {
public:
    ConsumerMock(ConfigurationMock config, EventProcessorPtr processor, ClusterPtr cluster);
    ConsumerMock(const ConsumerMock&) = delete;
    ConsumerMock& operator=(const ConsumerMock&) = delete;
    ~ConsumerMock();

    void subscribe(const std::vector<std::string>& topics);
private:
    static uint64_t make_consumer_id();

    void on_assignment(std::vector<TopicPartitionMock>& topic_partitions);
    void on_revocation(const std::vector<TopicPartitionMock>& topic_partitions);
    void on_message(uint64_t offset);

    ConfigurationMock config_;
    const std::string group_id_;
    std::unordered_set<std::string> subscribed_topics_;
    uint64_t consumer_id_;
};

} // mocking
} // cppkafka

#endif // CPPKAFKA_MOCKING_CONSUMER_MOCK_H
