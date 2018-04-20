#include <set>
#include <unordered_set>
#include <gtest/gtest.h>
#include "cppkafka/consumer.h"
#include "cppkafka/producer.h"
#include "cppkafka/metadata.h"
#include "cppkafka/group_information.h"
#include "test_utils.h"

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
        if (topic_names.count(topic.get_name()) == 1) {
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
    EXPECT_EQ(topic_names.size(), metadata.get_topics_prefixed("cppkafka_").size());

    // Now get the whole metadata only for this topic
    Topic topic = producer.get_topic(KAFKA_TOPIC);
    EXPECT_EQ(KAFKA_TOPIC, producer.get_metadata(topic).get_name());
}

TEST_F(KafkaHandleBaseTest, ConsumerGroups) {
    string consumer_group = "kafka_handle_test";
    string client_id = "my_client_id";

    Configuration config = make_config();
    config.set("group.id", consumer_group);
    config.set("client.id", client_id);
    config.set("enable.auto.commit", false);

    // Build consumer
    Consumer consumer(config);
    consumer.subscribe({ KAFKA_TOPIC });
    ConsumerRunner runner(consumer, 0, 3);
    runner.try_join();

    GroupInformation information = consumer.get_consumer_group(consumer_group);
    EXPECT_EQ(consumer_group, information.get_name());
    EXPECT_EQ("consumer", information.get_protocol_type());
    ASSERT_EQ(1, information.get_members().size());

    auto member = information.get_members()[0];
    EXPECT_EQ(client_id, member.get_client_id());

    MemberAssignmentInformation assignment = member.get_member_assignment();
    EXPECT_EQ(0, assignment.get_version());
    TopicPartitionList expected_topic_partitions = {
        { KAFKA_TOPIC, 0 },
        { KAFKA_TOPIC, 1 },
        { KAFKA_TOPIC, 2 }
    };
    TopicPartitionList topic_partitions = assignment.get_topic_partitions();
    sort(topic_partitions.begin(), topic_partitions.end());
    EXPECT_EQ(expected_topic_partitions, topic_partitions);
    /*for (const auto c : ) {
        printf("%0d,", (int)c & 0xff);
    }
    std::cout << std::endl;*/
}
