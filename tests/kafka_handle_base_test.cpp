#include <set>
#include <unordered_set>
#include <gtest/gtest.h>
#include "cppkafka/producer.h"
#include "cppkafka/metadata.h"

using std::vector;
using std::set;
using std::unordered_set;
using std::string;

using namespace cppkafka;

class KafkaHandleBaseTest : public testing::Test {
public:
    static const string KAFKA_TOPIC;

    Configuration make_config() {
        Configuration config;
        config.set("metadata.broker.list", KAFKA_TEST_INSTANCE);
        return config;
    }

    string get_kafka_host() {
        string uri = KAFKA_TEST_INSTANCE;
        size_t index = uri.find(':');
        if (index == string::npos) {
            return uri;
        }
        else {
            return uri.substr(0, index);
        }
    }

    uint16_t get_kafka_port() {
        string uri = KAFKA_TEST_INSTANCE;
        size_t index = uri.find(':');
        if (index == string::npos) {
            return 9092;
        }
        else {
            return stoul(uri.substr(index + 1));
        }
    }
};

const string KafkaHandleBaseTest::KAFKA_TOPIC = "cppkafka_test1";

TEST_F(KafkaHandleBaseTest, BrokersMetadata) {
    Producer producer({});
    producer.add_brokers(KAFKA_TEST_INSTANCE);
    Metadata metadata = producer.get_metadata();

    vector<BrokerMetadata> brokers = metadata.get_brokers();
    ASSERT_EQ(1, brokers.size());
    const auto& broker = brokers[0];
    // TODO: resolve this
    //EXPECT_EQ(get_kafka_host(), broker.get_host());
    EXPECT_EQ(get_kafka_port(), broker.get_port());
}

TEST_F(KafkaHandleBaseTest, TopicsMetadata) {
    unordered_set<string> topic_names = { "cppkafka_test1", "cppkafka_test2" };
    size_t found_topics = 0;

    Producer producer(make_config());
    Metadata metadata = producer.get_metadata();

    const vector<TopicMetadata>& topics = metadata.get_topics();
    ASSERT_GE(topics.size(), 2);

    for (const auto& topic : topics) {
        if (topic_names.count(topic.get_topic()) == 1) {
            const vector<PartitionMetadata>& partitions = topic.get_partitions();
            EXPECT_EQ(3, partitions.size());
            set<int32_t> expected_ids = { 0, 1, 2 };
            for (const PartitionMetadata& partition : partitions) {
                EXPECT_EQ(1, expected_ids.erase(partition.get_id()));
                for (int32_t replica : partition.get_replicas()) {
                    EXPECT_EQ(0, replica);
                }
                for (int32_t isr : partition.get_in_sync_replica_brokers()) {
                    EXPECT_EQ(0, isr);
                }
            }
            found_topics++;
        }
    }
    EXPECT_EQ(topic_names.size(), found_topics);

    // Find by names
    EXPECT_EQ(topic_names.size(), metadata.get_topics(topic_names).size());
    // Find by prefix
    EXPECT_EQ(topic_names.size(), metadata.get_topics("cppkafka_").size());

    // Now get the whole metadata only for this topic
    Topic topic = producer.get_topic(KAFKA_TOPIC);
    EXPECT_EQ(KAFKA_TOPIC, producer.get_metadata(topic).get_topic());
}
