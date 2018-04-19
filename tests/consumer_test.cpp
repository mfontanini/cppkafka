#include <vector>
#include <thread>
#include <set>
#include <mutex>
#include <chrono>
#include <iterator>
#include <condition_variable>
#include <gtest/gtest.h>
#include "cppkafka/consumer.h"
#include "cppkafka/producer.h"
#include "cppkafka/utils/consumer_dispatcher.h"
#include "cppkafka/utils/buffered_producer.h"
#include "test_utils.h"

using std::vector;
using std::move;
using std::string;
using std::thread;
using std::set;
using std::mutex;
using std::tie;
using std::condition_variable;
using std::lock_guard;
using std::unique_lock;
using std::make_move_iterator;
using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::system_clock;

using namespace cppkafka;

class ConsumerTest : public testing::Test {
public:
    static const string KAFKA_TOPIC;

    Configuration make_producer_config() {
        Configuration config;
        config.set("metadata.broker.list", KAFKA_TEST_INSTANCE);
        return config;
    }

    Configuration make_consumer_config(const string& group_id = "consumer_test") {
        Configuration config;
        config.set("metadata.broker.list", KAFKA_TEST_INSTANCE);
        config.set("enable.auto.commit", false);
        config.set("group.id", group_id);
        return config;
    }
};

const string ConsumerTest::KAFKA_TOPIC = "cppkafka_test1";

TEST_F(ConsumerTest, AssignmentCallback) {
    TopicPartitionList assignment;
    int partition = 0;

    // Create a consumer and subscribe to the topic
    Consumer consumer(make_consumer_config());
    consumer.set_assignment_callback([&](const TopicPartitionList& topic_partitions) {
        assignment = topic_partitions;
    });
    consumer.subscribe({ KAFKA_TOPIC });
    ConsumerRunner runner(consumer, 1, 3);

    // Produce a message just so we stop the consumer
    Producer producer(make_producer_config());
    string payload = "Hello world!";
    producer.produce(MessageBuilder(KAFKA_TOPIC).partition(partition).payload(payload));
    runner.try_join();

    // All 3 partitions should be ours
    EXPECT_EQ(3, assignment.size());
    set<int> partitions = { 0, 1, 2 }; 
    for (const auto& topic_partition : assignment) {
        EXPECT_EQ(KAFKA_TOPIC, topic_partition.get_topic());
        EXPECT_TRUE(partitions.erase(topic_partition.get_partition()));
    }
    EXPECT_EQ(1, runner.get_messages().size());

    EXPECT_EQ(vector<string>{ KAFKA_TOPIC }, consumer.get_subscription());

    assignment = consumer.get_assignment();
    EXPECT_EQ(3, assignment.size());

    int64_t low;
    int64_t high;
    tie(low, high) = consumer.get_offsets({ KAFKA_TOPIC, partition });
    EXPECT_GT(high, low);
    EXPECT_EQ(high, runner.get_messages().back().get_offset() + 1);
}

TEST_F(ConsumerTest, Rebalance) {
    TopicPartitionList assignment1;
    TopicPartitionList assignment2;
    bool revocation_called = false;
    int partition = 0;

    // Create a consumer and subscribe to the topic
    Consumer consumer1(make_consumer_config());
    consumer1.set_assignment_callback([&](const TopicPartitionList& topic_partitions) {
        assignment1 = topic_partitions;
    });
    consumer1.set_revocation_callback([&](const TopicPartitionList&) {
        revocation_called = true;
    });
    consumer1.subscribe({ KAFKA_TOPIC });
    ConsumerRunner runner1(consumer1, 1, 3);

    // Create a second consumer and subscribe to the topic
    Consumer consumer2(make_consumer_config());
    consumer2.set_assignment_callback([&](const TopicPartitionList& topic_partitions) {
        assignment2 = topic_partitions;
    });
    consumer2.subscribe({ KAFKA_TOPIC });
    ConsumerRunner runner2(consumer2, 1, 1);

    EXPECT_TRUE(revocation_called);

    // Produce a message just so we stop the consumer
    Producer producer(make_producer_config());
    string payload = "Hello world!";
    producer.produce(MessageBuilder(KAFKA_TOPIC).partition(partition).payload(payload));
    runner1.try_join();
    runner2.try_join();

    // All 3 partitions should be assigned
    EXPECT_EQ(3, assignment1.size() + assignment2.size());
    set<int> partitions = { 0, 1, 2 }; 
    for (const auto& topic_partition : assignment1) {
        EXPECT_EQ(KAFKA_TOPIC, topic_partition.get_topic());
        EXPECT_TRUE(partitions.erase(topic_partition.get_partition()));
    }
    for (const auto& topic_partition : assignment2) {
        EXPECT_EQ(KAFKA_TOPIC, topic_partition.get_topic());
        EXPECT_TRUE(partitions.erase(topic_partition.get_partition()));
    }
    EXPECT_EQ(1, runner1.get_messages().size() + runner2.get_messages().size());
}

TEST_F(ConsumerTest, OffsetCommit) {
    int partition = 0;
    int64_t message_offset = 0;
    bool offset_commit_called = false;

    // Create a consumer and subscribe to the topic
    Configuration config = make_consumer_config("offset_commit");
    config.set_offset_commit_callback([&](Consumer&, Error error,
                                          const TopicPartitionList& topic_partitions) {
        offset_commit_called = true;
        EXPECT_FALSE(error);
        ASSERT_EQ(1, topic_partitions.size());
        EXPECT_EQ(KAFKA_TOPIC, topic_partitions[0].get_topic());
        EXPECT_EQ(0, topic_partitions[0].get_partition());
        EXPECT_EQ(message_offset + 1, topic_partitions[0].get_offset());
    });
    Consumer consumer(config);
    consumer.assign({ { KAFKA_TOPIC, 0 } });
    ConsumerRunner runner(consumer, 1, 1);

    // Produce a message just so we stop the consumer
    Producer producer(make_producer_config());
    string payload = "Hello world!";
    producer.produce(MessageBuilder(KAFKA_TOPIC).partition(partition).payload(payload));
    runner.try_join();

    ASSERT_EQ(1, runner.get_messages().size());
    const Message& msg = runner.get_messages()[0];
    message_offset = msg.get_offset();
    consumer.commit(msg);
    for (size_t i = 0; i < 3 && !offset_commit_called; ++i) {
        consumer.poll();
    }
    EXPECT_TRUE(offset_commit_called);
}

TEST_F(ConsumerTest, Throttle) {
    int partition = 0;

    // Create a consumer and subscribe to the topic
    Configuration config = make_consumer_config("offset_commit");
    Consumer consumer(config);
    consumer.assign({ { KAFKA_TOPIC, 0 } });

    {
        ConsumerRunner runner(consumer, 0, 1);
        runner.try_join();
    }

    // Produce a message just so we stop the consumer
    BufferedProducer<string> producer(make_producer_config());
    string payload = "Hello world!";
    producer.produce(MessageBuilder(KAFKA_TOPIC).partition(partition).payload(payload));
    producer.flush();

    size_t callback_executed_count = 0;

    ConsumerDispatcher dispatcher(consumer);
    dispatcher.run(
        [&](Message msg) {
            callback_executed_count++;
            if (callback_executed_count == 3) {
                return Message();
            }
            return move(msg);
        },
        [&](ConsumerDispatcher::Timeout) {
            if (callback_executed_count == 3) {
                dispatcher.stop();
            }
        }
    );

    EXPECT_EQ(3, callback_executed_count);
}

TEST_F(ConsumerTest, ConsumeBatch) {
    int partition = 0;

    // Create a consumer and subscribe to the topic
    Configuration config = make_consumer_config("test");
    Consumer consumer(config);
    consumer.assign({ { KAFKA_TOPIC, 0 } });

    {
        ConsumerRunner runner(consumer, 0, 1);
        runner.try_join();
    }

    // Produce a message just so we stop the consumer
    BufferedProducer<string> producer(make_producer_config());
    string payload = "Hello world!";
    // Produce it twice
    producer.produce(MessageBuilder(KAFKA_TOPIC).partition(partition).payload(payload));
    producer.produce(MessageBuilder(KAFKA_TOPIC).partition(partition).payload(payload));
    producer.flush();

    vector<Message> all_messages;
    int i = 0;
    while (i < 5 && all_messages.size() != 2) {
        vector<Message> messages = consumer.poll_batch(2);
        all_messages.insert(all_messages.end(), make_move_iterator(messages.begin()),
                            make_move_iterator(messages.end()));
        ++i;
    }
    ASSERT_EQ(2, all_messages.size());
    EXPECT_EQ(payload, all_messages[0].get_payload());
    EXPECT_EQ(payload, all_messages[1].get_payload());
}
