#include <thread>
#include <mutex>
#include <chrono>
#include <set>
#include <condition_variable>
#include <catch.hpp>
#include "cppkafka/producer.h"
#include "cppkafka/consumer.h"
#include "cppkafka/utils/buffered_producer.h"
#include "test_utils.h"

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

static const string KAFKA_TOPIC = "cppkafka_test1";

static Configuration make_producer_config() {
    Configuration config = {
        { "metadata.broker.list", KAFKA_TEST_INSTANCE },
        { "queue.buffering.max.ms", 0 },
        { "api.version.request", true },
        { "queue.buffering.max.ms", 50 }
    };
    return config;
}

static Configuration make_consumer_config() {
    Configuration config = {
        { "metadata.broker.list", KAFKA_TEST_INSTANCE },
        { "enable.auto.commit", false },
        { "group.id", "producer_test" },
        { "api.version.request", true }
    };
    return config;
}

TEST_CASE("simple production", "[producer]") {
    int partition = 0;

    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPIC, partition) });
    ConsumerRunner runner(consumer, 1, 1);

    Configuration config = make_producer_config();
    SECTION("message with no key") {
        // Now create a producer and produce a message
        const string payload = "Hello world! 1";
        Producer producer(config);
        producer.produce(MessageBuilder(KAFKA_TOPIC).partition(partition).payload(payload));
        runner.try_join();

        const auto& messages = runner.get_messages();
        REQUIRE(messages.size() == 1);
        const auto& message = messages[0];
        CHECK(message.get_payload() == payload);
        CHECK(!!message.get_key() == false);
        CHECK(message.get_topic() == KAFKA_TOPIC);
        CHECK(message.get_partition() == partition);
        CHECK(!!message.get_error() == false);

        int64_t low;
        int64_t high;
        tie(low, high) = producer.query_offsets({ KAFKA_TOPIC, partition });
        CHECK(high > low);
    }

    SECTION("message with key") {
        const string payload = "Hello world! 2";
        const string key = "such key";
        const milliseconds timestamp{15};
        Producer producer(config);
        producer.produce(MessageBuilder(KAFKA_TOPIC).partition(partition)
                                                     .key(key)
                                                     .payload(payload)
                                                     .timestamp(timestamp));
        runner.try_join();

        const auto& messages = runner.get_messages();
        REQUIRE(messages.size() == 1);
        const auto& message = messages[0];
        CHECK(message.get_payload() == payload);
        CHECK(message.get_key() == key);
        CHECK(message.get_topic() == KAFKA_TOPIC);
        CHECK(message.get_partition() == partition);
        CHECK(!!message.get_error() == false);
        REQUIRE(!!message.get_timestamp() == true);
        CHECK(message.get_timestamp()->get_timestamp() == timestamp);
    }

    SECTION("callbacks") {
        // Now create a producer and produce a message
        const string payload = "Hello world! 3";
        const string key = "hehe";
        bool delivery_report_called = false;
        config.set_delivery_report_callback([&](Producer&, const Message& msg) {
            CHECK(msg.get_payload() == payload);
            delivery_report_called = true;
        });

        TopicConfiguration topic_config;
        topic_config.set_partitioner_callback([&](const Topic& topic, const Buffer& msg_key,
                                                  int32_t partition_count) {
            CHECK(msg_key == key);
            CHECK(partition_count == 3);
            CHECK(topic.get_name() == KAFKA_TOPIC);
            return 0;
        });
        config.set_default_topic_configuration(topic_config);

        Producer producer(config);
        producer.produce(MessageBuilder(KAFKA_TOPIC).key(key).payload(payload));
        while (producer.get_out_queue_length() > 0) {
            producer.poll();
        }
        runner.try_join();

        const auto& messages = runner.get_messages();
        REQUIRE(messages.size() == 1);
        const auto& message = messages[0];
        CHECK(message.get_payload() == payload);
        CHECK(message.get_key() == key);
        CHECK(message.get_topic() == KAFKA_TOPIC);
        CHECK(message.get_partition() == partition);
        CHECK(!!message.get_error() == false);
        CHECK(delivery_report_called == true);
    }

    SECTION("partitioner callback") {
        // Now create a producer and produce a message
        const string payload = "Hello world! 4";
        const string key = "hehe";
        bool callback_called = false;

        TopicConfiguration topic_config;
        topic_config.set_partitioner_callback([&](const Topic& topic, const Buffer& msg_key,
                                                  int32_t partition_count) {
            CHECK(msg_key == key);
            CHECK(partition_count == 3);
            CHECK(topic.get_name() == KAFKA_TOPIC);
            callback_called = true;
            return 0;
        });
        config.set_default_topic_configuration(topic_config);
        Producer producer(config);

        producer.produce(MessageBuilder(KAFKA_TOPIC).key(key).payload(payload));
        producer.poll();
        runner.try_join();

        const auto& messages = runner.get_messages();
        REQUIRE(messages.size() == 1);
        const auto& message = messages[0];
        CHECK(message.get_partition() == partition);
        CHECK(callback_called == true);
    }
}

TEST_CASE("multiple messages", "[producer]") {
    size_t message_count = 10;
    int partitions = 3;
    set<string> payloads;

    // Create a consumer and subscribe to this topic
    Consumer consumer(make_consumer_config());
    consumer.subscribe({ KAFKA_TOPIC });
    ConsumerRunner runner(consumer, message_count, partitions);

    // Now create a producer and produce a message
    Producer producer(make_producer_config());
    const string payload_base = "Hello world ";
    for (size_t i = 0; i < message_count; ++i) {
        const string payload = payload_base + to_string(i);
        payloads.insert(payload);
        producer.produce(MessageBuilder(KAFKA_TOPIC).payload(payload));
    }
    runner.try_join();

    const auto& messages = runner.get_messages();
    REQUIRE(messages.size() == message_count);
    for (const auto& message : messages) {
        CHECK(message.get_topic() == KAFKA_TOPIC);
        CHECK(payloads.erase(message.get_payload()) == 1);
        CHECK(!!message.get_error() == false);
        CHECK(!!message.get_key() == false);
        CHECK(message.get_partition() >= 0);
        CHECK(message.get_partition() < 3);
    }
}

TEST_CASE("buffered producer", "[producer]") {
    int partition = 0;

    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPIC, partition) });
    ConsumerRunner runner(consumer, 3, 1);

    // Now create a buffered producer and produce two messages
    BufferedProducer<string> producer(make_producer_config());
    const string payload = "Hello world! 2";
    const string key = "such key";
    producer.add_message(MessageBuilder(KAFKA_TOPIC).partition(partition)
                                                    .key(key)
                                                    .payload(payload));
    producer.add_message(producer.make_builder(KAFKA_TOPIC).partition(partition).payload(payload));
    producer.flush();
    producer.produce(MessageBuilder(KAFKA_TOPIC).partition(partition).payload(payload));
    producer.wait_for_acks();
    // Add another one but then clear it
    producer.add_message(producer.make_builder(KAFKA_TOPIC).partition(partition).payload(payload));
    producer.clear();
    runner.try_join();

    const auto& messages = runner.get_messages();
    REQUIRE(messages.size() == 3);
    const auto& message = messages[0];
    CHECK(message.get_key() == key);
    CHECK(message.get_topic() == KAFKA_TOPIC);
    CHECK(message.get_partition() == partition);
    CHECK(!!message.get_error() == false);

    CHECK(!!messages[1].get_key() == false);
    CHECK(!!messages[2].get_key() == false);
    for (const auto& message : messages) {
        CHECK(message.get_payload() == payload);
    }
}
