#include <thread>
#include <mutex>
#include <chrono>
#include <set>
#include <condition_variable>
#include <gtest/gtest.h>
#include "cppkafka/producer.h"
#include "cppkafka/consumer.h"

using std::string;
using std::to_string;
using std::set;
using std::tie;
using std::move;
using std::thread;
using std::mutex;
using std::unique_lock;
using std::lock_guard;
using std::condition_variable;

using std::chrono::system_clock;
using std::chrono::seconds;
using std::chrono::milliseconds;

using namespace cppkafka;

class ConsumerRunner {
public:
    ConsumerRunner(Consumer& consumer, size_t expected, size_t partitions) 
    : consumer_(consumer) {
        bool booted = false;
        mutex mtx;
        condition_variable cond;
        thread_ = thread([&, expected, partitions]() {
            consumer_.set_timeout(milliseconds(500));
            size_t number_eofs = 0;
            auto start = system_clock::now();
            while (system_clock::now() - start < seconds(10) && messages_.size() < expected) {
                Message msg = consumer_.poll();
                if (msg && number_eofs != partitions && msg.get_error() == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    number_eofs++;
                    if (number_eofs == partitions) {
                        lock_guard<mutex> _(mtx);
                        booted = true;
                        cond.notify_one();
                    }
                }
                else if (msg && msg.get_error() == 0) {
                    messages_.push_back(move(msg));
                }
            }
        });

        unique_lock<mutex> lock(mtx);
        while (!booted) {
            cond.wait(lock);
        }
    }



    ConsumerRunner(const ConsumerRunner&) = delete;
    ConsumerRunner& operator=(const ConsumerRunner&) = delete;

    ~ConsumerRunner() {
        try_join();
    }

    const std::vector<Message>& get_messages() const {
        return messages_;
    }

    void try_join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }
private:
    Consumer& consumer_;
    thread thread_;
    std::vector<Message> messages_;
};

class ProducerTest : public testing::Test {
public:
    static const string KAFKA_TOPIC;

    Configuration make_producer_config() {
        Configuration config;
        config.set("metadata.broker.list", KAFKA_TEST_INSTANCE);
        return config;
    }

    Configuration make_consumer_config() {
        Configuration config;
        config.set("metadata.broker.list", KAFKA_TEST_INSTANCE);
        config.set("enable.auto.commit", false);
        config.set("group.id", "producer_test");
        return config;
    }
};

const string ProducerTest::KAFKA_TOPIC = "cppkafka_test1";

TEST_F(ProducerTest, OneMessageOnFixedPartition) {
    int partition = 0;

    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPIC, partition) });
    ConsumerRunner runner(consumer, 1, 1);

    // Now create a producer and produce a message
    Producer producer(make_producer_config());
    Topic topic = producer.get_topic(KAFKA_TOPIC);
    string payload = "Hello world! 1";
    producer.produce(topic, partition, Buffer(payload.data(), payload.size()));
    runner.try_join();

    const auto& messages = runner.get_messages();
    ASSERT_EQ(1, messages.size());
    const auto& message = messages[0];
    EXPECT_EQ(payload, message.get_payload().as_string());
    EXPECT_EQ("", message.get_key().as_string());
    EXPECT_EQ(KAFKA_TOPIC, message.get_topic());
    EXPECT_EQ(partition, message.get_partition());
    EXPECT_EQ(0, message.get_error());

    int64_t low;
    int64_t high;
    tie(low, high) = producer.query_offsets(KAFKA_TOPIC, partition);
    EXPECT_GT(high, low);
}

TEST_F(ProducerTest, OneMessageUsingKey) {
    int partition = 0;

    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPIC, partition) });
    ConsumerRunner runner(consumer, 1, 1);

    // Now create a producer and produce a message
    Producer producer(make_producer_config());
    Topic topic = producer.get_topic(KAFKA_TOPIC);
    string payload = "Hello world! 2";
    string key = "such key";
    producer.produce(topic, partition, Buffer(payload.data(), payload.size()),
                     Buffer(key.data(), key.size()));
    runner.try_join();

    const auto& messages = runner.get_messages();
    ASSERT_EQ(1, messages.size());
    const auto& message = messages[0];
    EXPECT_EQ(payload, message.get_payload().as_string());
    EXPECT_EQ(key, message.get_key().as_string());
    EXPECT_EQ(KAFKA_TOPIC, message.get_topic());
    EXPECT_EQ(partition, message.get_partition());
    EXPECT_EQ(0, message.get_error());
}

TEST_F(ProducerTest, MultipleMessagesUnassignedPartitions) {
    size_t message_count = 10;
    int partitions = 3;
    set<string> payloads;

    // Create a consumer and subscribe to this topic
    Consumer consumer(make_consumer_config());
    consumer.subscribe({ KAFKA_TOPIC });
    ConsumerRunner runner(consumer, message_count, partitions);

    // Now create a producer and produce a message
    Producer producer(make_producer_config());
    Topic topic = producer.get_topic(KAFKA_TOPIC);
    string payload_base = "Hello world ";
    for (size_t i = 0; i < message_count; ++i) {
        string payload = payload_base + to_string(i);
        payloads.insert(payload);
        producer.produce(topic, {} /*unassigned partition*/, 
                         Buffer(payload.data(), payload.size()));
    }
    runner.try_join();

    const auto& messages = runner.get_messages();
    ASSERT_EQ(message_count, messages.size());
    for (const auto& message : messages) {
        EXPECT_EQ(KAFKA_TOPIC, message.get_topic());
        EXPECT_EQ(1, payloads.erase(message.get_payload().as_string()));
        EXPECT_EQ(0, message.get_error());
        EXPECT_EQ("", message.get_key().as_string());
        EXPECT_GE(message.get_partition(), 0);
        EXPECT_LT(message.get_partition(), 3);
    }
}

TEST_F(ProducerTest, Callbacks) {
    int partition = 0;

    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPIC, partition) });
    ConsumerRunner runner(consumer, 1, 1);

    // Now create a producer and produce a message
    string payload = "Hello world! 3";
    string key = "hehe";
    bool deliver_report_called = false;
    Configuration config = make_producer_config();
    config.set_delivery_report_callback([&](Producer&, const Message& msg) {
        EXPECT_EQ(payload, msg.get_payload().as_string());
        deliver_report_called = true;
    });

    TopicConfiguration topic_config;
    topic_config.set_partitioner_callback([&](const Topic& topic, const Buffer& msg_key,
                                              int32_t partition_count) {
        EXPECT_EQ(key, msg_key.as_string());
        EXPECT_EQ(3, partition_count);
        EXPECT_EQ(KAFKA_TOPIC, topic.get_name());
        return 0;
    });

    Producer producer(move(config));
    Topic topic = producer.get_topic(KAFKA_TOPIC, topic_config);
    producer.produce(topic, {}, Buffer(payload.data(), payload.size()),
                     Buffer(key.data(), key.size()));
    producer.poll();
    runner.try_join();

    const auto& messages = runner.get_messages();
    ASSERT_EQ(1, messages.size());
    const auto& message = messages[0];
    EXPECT_EQ(payload, message.get_payload().as_string());
    EXPECT_EQ(key, message.get_key().as_string());
    EXPECT_EQ(KAFKA_TOPIC, message.get_topic());
    EXPECT_EQ(partition, message.get_partition());
    EXPECT_EQ(0, message.get_error());
    EXPECT_TRUE(deliver_report_called);
}

TEST_F(ProducerTest, PartitionerCallbackOnDefaultTopicConfig) {
    int partition = 0;

    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPIC, partition) });
    ConsumerRunner runner(consumer, 1, 1);

    // Now create a producer and produce a message
    string payload = "Hello world! 4";
    string key = "hehe";
    bool callback_called = false;

    Configuration config = make_producer_config();
    TopicConfiguration topic_config;
    topic_config.set_partitioner_callback([&](const Topic& topic, const Buffer& msg_key,
                                              int32_t partition_count) {
        EXPECT_EQ(key, msg_key.as_string());
        EXPECT_EQ(3, partition_count);
        EXPECT_EQ(KAFKA_TOPIC, topic.get_name());
        callback_called = true;
        return 0;
    });
    config.set_default_topic_configuration(topic_config);

    Producer producer(move(config));
    Topic topic = producer.get_topic(KAFKA_TOPIC);
    producer.produce(topic, {}, Buffer(payload.data(), payload.size()),
                     Buffer(key.data(), key.size()));
    producer.poll();
    runner.try_join();

    const auto& messages = runner.get_messages();
    ASSERT_EQ(1, messages.size());
    const auto& message = messages[0];
    EXPECT_EQ(partition, message.get_partition());
    EXPECT_TRUE(callback_called);   
}

#ifdef CPPKAFKA_HAVE_ZOOKEEPER

#include "zookeeper/zookeeper_pool.h"

TEST_F(ProducerTest, ConnectUsingZookeeper) {
    int partition = 0;

    Configuration producer_config;
    producer_config.set("zookeeper", ZOOKEEPER_TEST_INSTANCE);

    Configuration consumer_config = producer_config;
    consumer_config.set("enable.auto.commit", false);
    consumer_config.set("group.id", "producer_test");

    // Create a consumer and assign this topic/partition
    Consumer consumer(consumer_config);
    consumer.assign({ TopicPartition(KAFKA_TOPIC, partition) });
    ConsumerRunner runner(consumer, 1, 1);

    // Now create a producer and produce a message
    Producer producer(producer_config);
    Topic topic = producer.get_topic(KAFKA_TOPIC);
    string payload = "Hello world! 2";
    string key = "such key";
    producer.produce(topic, partition, Buffer(payload.data(), payload.size()),
                     Buffer(key.data(), key.size()));
    runner.try_join();

    const auto& messages = runner.get_messages();
    ASSERT_EQ(1, messages.size());
    const auto& message = messages[0];
    EXPECT_EQ(payload, message.get_payload().as_string());
    EXPECT_EQ(key, message.get_key().as_string());
    EXPECT_EQ(KAFKA_TOPIC, message.get_topic());
    EXPECT_EQ(partition, message.get_partition());
    EXPECT_EQ(0, message.get_error());

    // We should have 2 watchers
    EXPECT_EQ(2, ZookeeperPool::instance().get_subscriber_count(ZOOKEEPER_TEST_INSTANCE));
}

#endif // CPPKAFKA_HAVE_ZOOKEEPER
