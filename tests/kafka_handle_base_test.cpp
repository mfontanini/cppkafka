#include <set>
#include <unordered_set>
#include <catch.hpp>
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

TEST_CASE("metadata", "[handle_base]") {
    if (KAFKA_TOPICS.size() < 2) {
        return; //skip test
    }
    Producer producer({});
    producer.add_brokers(KAFKA_TEST_INSTANCE);
    Metadata metadata = producer.get_metadata();

    SECTION("brokers") {
        vector<BrokerMetadata> brokers = metadata.get_brokers();
        REQUIRE(brokers.size() == 1);
        const auto& broker = brokers[0];
        // TODO: resolve this
        //REQUIRE(broker.get_host() == get_kafka_host());
        CHECK(broker.get_port() == get_kafka_port());
    }

    SECTION("topics") {
        unordered_set<string> topic_names = { KAFKA_TOPICS[0], KAFKA_TOPICS[1] };
        size_t found_topics = 0;

        const vector<TopicMetadata>& topics = metadata.get_topics();
        CHECK(topics.size() >= 2);

        for (const auto& topic : topics) {
            if (topic_names.count(topic.get_name()) == 1) {
                const vector<PartitionMetadata>& partitions = topic.get_partitions();
                REQUIRE(partitions.size() == KAFKA_NUM_PARTITIONS);
                set<int32_t> expected_ids;
                for (int i = 0; i < KAFKA_NUM_PARTITIONS; expected_ids.emplace(i++));
                for (const PartitionMetadata& partition : partitions) {
                    REQUIRE(expected_ids.erase(partition.get_id()) == 1);
                    for (int32_t replica : partition.get_replicas()) {
                        REQUIRE(replica == 0);
                    }
                    for (int32_t isr : partition.get_in_sync_replica_brokers()) {
                        REQUIRE(isr == 0);
                    }
                }
                found_topics++;
            }
        }
        CHECK(found_topics == topic_names.size());

        // Find by names
        CHECK(metadata.get_topics(topic_names).size() == topic_names.size());
        // Find by prefix
        CHECK(metadata.get_topics_prefixed("cppkafka_").size() == topic_names.size());

        // Now get the whole metadata only for this topic
        Topic topic = producer.get_topic(KAFKA_TOPICS[0]);
        CHECK(producer.get_metadata(topic).get_name() == KAFKA_TOPICS[0]);
    }
}

TEST_CASE("consumer groups", "[handle_base]") {
    string consumer_group = "kafka_handle_test";
    string client_id = "my_client_id";

    Configuration config = make_config();
    config.set("group.id", consumer_group);
    config.set("client.id", client_id);
    config.set("enable.auto.commit", false);

    // Build consumer
    Consumer consumer(config);
    consumer.subscribe({ KAFKA_TOPICS[0] });
    ConsumerRunner runner(consumer, 0, 3);
    runner.try_join();

    GroupInformation information = consumer.get_consumer_group(consumer_group);
    CHECK(information.get_name() == consumer_group);
    CHECK(information.get_protocol_type() == "consumer");
    CHECK(information.get_members().size() == 1);

    auto member = information.get_members()[0];
    CHECK(member.get_client_id() == client_id);

    MemberAssignmentInformation assignment = member.get_member_assignment();
    CHECK(assignment.get_version() == 0);
    TopicPartitionList expected_topic_partitions;
    for (int i = 0; i < KAFKA_NUM_PARTITIONS; expected_topic_partitions.emplace_back(KAFKA_TOPICS[0], i++));
    TopicPartitionList topic_partitions = assignment.get_topic_partitions();
    sort(topic_partitions.begin(), topic_partitions.end());
    CHECK(topic_partitions == expected_topic_partitions);
}
