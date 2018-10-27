#include <vector>
#include <thread>
#include <set>
#include <mutex>
#include <chrono>
#include <iterator>
#include <condition_variable>
#include <catch.hpp>
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

static Configuration make_producer_config() {
    Configuration config;
    config.set("metadata.broker.list", KAFKA_TEST_INSTANCE);
    return config;
}

static Configuration make_consumer_config(const string& group_id = make_consumer_group_id()) {
    Configuration config;
    config.set("metadata.broker.list", KAFKA_TEST_INSTANCE);
    config.set("enable.auto.commit", false);
    config.set("group.id", group_id);
    return config;
}

TEST_CASE("message consumption", "[consumer]") {
    TopicPartitionList assignment;
    int partition = 0;

    // Create a consumer and subscribe to the topic
    Consumer consumer(make_consumer_config());
    consumer.set_assignment_callback([&](const TopicPartitionList& topic_partitions) {
        assignment = topic_partitions;
    });
    consumer.subscribe({ KAFKA_TOPICS[0] });
    ConsumerRunner runner(consumer, 1, KAFKA_NUM_PARTITIONS);

    // Produce a message just so we stop the consumer
    Producer producer(make_producer_config());
    string payload = "Hello world!";
    producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition).payload(payload));
    runner.try_join();

    // All partitions should be ours
    REQUIRE(assignment.size() == KAFKA_NUM_PARTITIONS);
    set<int> partitions;
    for (int i = 0; i < KAFKA_NUM_PARTITIONS; partitions.emplace(i++));
    for (const auto& topic_partition : assignment) {
        CHECK(topic_partition.get_topic() == KAFKA_TOPICS[0]);
        CHECK(partitions.erase(topic_partition.get_partition()) == true);
    }
    REQUIRE(runner.get_messages().size() == 1);
    CHECK(consumer.get_subscription() == vector<string>{ KAFKA_TOPICS[0] });

    assignment = consumer.get_assignment();
    CHECK(assignment.size() == KAFKA_NUM_PARTITIONS);

    int64_t low;
    int64_t high;
    tie(low, high) = consumer.get_offsets({ KAFKA_TOPICS[0], partition });
    CHECK(high > low);
    CHECK(runner.get_messages().back().get_offset() + 1 == high);
}

TEST_CASE("consumer rebalance", "[consumer]") {
    TopicPartitionList assignment1;
    TopicPartitionList assignment2;
    const string group_id = make_consumer_group_id();
    bool revocation_called = false;
    int partition = 0;

    // Create a consumer and subscribe to the topic
    Consumer consumer1(make_consumer_config(group_id));
    consumer1.set_assignment_callback([&](const TopicPartitionList& topic_partitions) {
        assignment1 = topic_partitions;
    });
    consumer1.set_revocation_callback([&](const TopicPartitionList&) {
        revocation_called = true;
    });
    consumer1.subscribe({ KAFKA_TOPICS[0] });
    ConsumerRunner runner1(consumer1, 1, KAFKA_NUM_PARTITIONS);

    // Create a second consumer and subscribe to the topic
    Consumer consumer2(make_consumer_config(group_id));
    consumer2.set_assignment_callback([&](const TopicPartitionList& topic_partitions) {
        assignment2 = topic_partitions;
    });
    consumer2.subscribe({ KAFKA_TOPICS[0] });
    ConsumerRunner runner2(consumer2, 1, 1);

    CHECK(revocation_called == true);

    // Produce a message just so we stop the consumer
    Producer producer(make_producer_config());
    string payload = "Hello world!";
    producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition).payload(payload));
    runner1.try_join();
    runner2.try_join();

    // All partitions should be assigned
    CHECK(assignment1.size() + assignment2.size() == KAFKA_NUM_PARTITIONS);
    set<int> partitions;
    for (int i = 0; i < KAFKA_NUM_PARTITIONS; partitions.emplace(i++));
    for (const auto& topic_partition : assignment1) {
        CHECK(topic_partition.get_topic() == KAFKA_TOPICS[0]);
        CHECK(partitions.erase(topic_partition.get_partition()) == true);
    }
    for (const auto& topic_partition : assignment2) {
        CHECK(topic_partition.get_topic() == KAFKA_TOPICS[0]);
        CHECK(partitions.erase(topic_partition.get_partition()) == true);
    }
    CHECK(runner1.get_messages().size() + runner2.get_messages().size() == 1);
}

TEST_CASE("consumer offset commit", "[consumer]") {
    int partition = 0;
    int64_t message_offset = 0;
    bool offset_commit_called = false;

    // Create a consumer and subscribe to the topic
    Configuration config = make_consumer_config("offset_commit");
    config.set_offset_commit_callback([&](Consumer&, Error error,
                                          const TopicPartitionList& topic_partitions) {
        offset_commit_called = true;
        CHECK(!!error == false);
        REQUIRE(topic_partitions.size() == 1);
        CHECK(topic_partitions[0].get_topic() == KAFKA_TOPICS[0]);
        CHECK(topic_partitions[0].get_partition() == 0);
        CHECK(topic_partitions[0].get_offset() == message_offset + 1);
    });
    Consumer consumer(config);
    consumer.assign({ { KAFKA_TOPICS[0], 0 } });
    ConsumerRunner runner(consumer, 1, 1);

    // Produce a message just so we stop the consumer
    Producer producer(make_producer_config());
    string payload = "Hello world!";
    producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition).payload(payload));
    runner.try_join();

    REQUIRE(runner.get_messages().size() == 1);
    const Message& msg = runner.get_messages()[0];
    message_offset = msg.get_offset();
    consumer.commit(msg);
    for (size_t i = 0; i < 3 && !offset_commit_called; ++i) {
        consumer.poll();
    }
    CHECK(offset_commit_called == true);
}

TEST_CASE("consumer throttle", "[consumer]") {
    int partition = 0;

    // Create a consumer and subscribe to the topic
    Configuration config = make_consumer_config("offset_commit");
    Consumer consumer(config);
    consumer.assign({ { KAFKA_TOPICS[0], 0 } });

    {
        ConsumerRunner runner(consumer, 0, 1);
        runner.try_join();
    }

    // Produce a message just so we stop the consumer
    BufferedProducer<string> producer(make_producer_config());
    string payload = "Hello world!";
    producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition).payload(payload));
    producer.flush();

    size_t callback_executed_count = 0;

    ConsumerDispatcher dispatcher(consumer);
    dispatcher.run(
        [&](Message msg) {
            callback_executed_count++;
            if (callback_executed_count == 3) {
                return Message();
            }
            return msg;
        },
        [&](ConsumerDispatcher::Timeout) {
            if (callback_executed_count == 3) {
                dispatcher.stop();
            }
        }
    );

    CHECK(callback_executed_count == 3);
}

TEST_CASE("consume batch", "[consumer]") {
    int partition = 0;

    // Create a consumer and subscribe to the topic
    Configuration config = make_consumer_config("test");
    Consumer consumer(config);
    consumer.assign({ { KAFKA_TOPICS[0], 0 } });

    {
        ConsumerRunner runner(consumer, 0, 1);
        runner.try_join();
    }

    // Produce a message just so we stop the consumer
    BufferedProducer<string> producer(make_producer_config());
    string payload = "Hello world!";
    // Produce it twice
    producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition).payload(payload));
    producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition).payload(payload));
    producer.flush();

    MessageList all_messages;
    int i = 0;
    while (i < 5 && all_messages.size() != 2) {
        MessageList messages = consumer.poll_batch(2);
        all_messages.insert(all_messages.end(), make_move_iterator(messages.begin()),
                            make_move_iterator(messages.end()));
        ++i;
    }
    REQUIRE(all_messages.size() == 2);
    CHECK(all_messages[0].get_payload() == payload);
    CHECK(all_messages[1].get_payload() == payload);
}

// This test may fail due to what seems to be an rdkafka bug. Skip it for now until we're 
// certain of what to do
TEST_CASE("Event consumption", "[!hide][consumer]") {
    // Create a consumer and subscribe to the topic
    Consumer consumer(make_consumer_config());
    consumer.subscribe({ KAFKA_TOPICS[0] });

    vector<rd_kafka_event_type_t> types = {
        RD_KAFKA_EVENT_NONE
    };
    Queue queue = consumer.get_main_queue();
    for (const auto type : types) {
        const Event event = queue.next_event();
        CHECK(event.get_type() == type);
    }
}
