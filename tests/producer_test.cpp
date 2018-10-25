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
using std::vector;
using std::tie;
using std::move;
using std::thread;
namespace this_thread = std::this_thread;
using std::mutex;
using std::unique_lock;
using std::lock_guard;
using std::condition_variable;
using std::chrono::system_clock;
using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::time_point;
using std::chrono::duration_cast;
using std::ref;

using namespace cppkafka;

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
        { "group.id", make_consumer_group_id() },
        { "api.version.request", true }
    };
    return config;
}

void producer_run(BufferedProducer<string>& producer,
                  int& exit_flag, condition_variable& clear,
                  int num_messages,
                  int partition) {
    MessageBuilder builder(KAFKA_TOPICS[0]);
    string key("wassup?");
    string payload("nothing much!");
    
    builder.partition(partition).key(key).payload(payload);
    for (int i = 0; i < num_messages; ++i) {
        if (i == num_messages/2) {
            clear.notify_one();
        }
        producer.add_message(builder);
        this_thread::sleep_for(milliseconds(10));
    }
    exit_flag = 1;
}

void flusher_run(BufferedProducer<string>& producer,
                 int& exit_flag,
                 int num_flush) {
    while (!exit_flag) {
        if (producer.get_buffer_size() >= (size_t)num_flush) {
            producer.flush();
        }
        this_thread::sleep_for(milliseconds(10));
    }
    producer.flush();
}

void async_flusher_run(BufferedProducer<string>& producer,
                       int& exit_flag,
                       int num_flush) {
    while (!exit_flag) {
        if (producer.get_buffer_size() >= (size_t)num_flush) {
            producer.async_flush();
        }
        this_thread::sleep_for(milliseconds(10));
    }
    producer.async_flush();
    producer.wait_for_acks();
}

void clear_run(BufferedProducer<string>& producer,
               condition_variable& clear) {
    mutex m;
    unique_lock<mutex> lock(m);
    clear.wait(lock);
    producer.clear();
}

vector<int> dr_data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
void dr_callback(const Message& message) {
    static int i = 0;
    if (!message || message.is_eof()) return;
    CHECK(message.get_user_data() == &dr_data[i]);
    CHECK(*static_cast<int*>(message.get_user_data()) == dr_data[i]);
    ++i;
}

bool dr_failure_callback(const Message& message) {
    if (!message || message.is_eof()) return true;
    CHECK(message.get_user_data() == &dr_data[0]);
    CHECK(*static_cast<int*>(message.get_user_data()) == dr_data[0]);
    return true; //always retry
}

template <typename B>
class ErrorProducer : public BufferedProducer<B>
{
public:
    ErrorProducer(Configuration config,
                  typename BufferedProducer<B>::TestParameters params) :
        BufferedProducer<B>(config),
        params_(params) {
        this->set_test_parameters(&params_);
    }
private:
    typename BufferedProducer<B>::TestParameters params_;
};

TEST_CASE("simple production", "[producer]") {
    int partition = 0;

    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPICS[0], partition) });
    ConsumerRunner runner(consumer, 1, 1);

    Configuration config = make_producer_config();
    SECTION("message with no key") {
        // Now create a producer and produce a message
        const string payload = "Hello world! 1";
        Producer producer(config);
        producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition).payload(payload));
        runner.try_join();

        const auto& messages = runner.get_messages();
        REQUIRE(messages.size() == 1);
        const auto& message = messages[0];
        CHECK(message.get_payload() == payload);
        CHECK(!!message.get_key() == false);
        CHECK(message.get_topic() == KAFKA_TOPICS[0]);
        CHECK(message.get_partition() == partition);
        CHECK(!!message.get_error() == false);

        int64_t low;
        int64_t high;
        tie(low, high) = producer.query_offsets({ KAFKA_TOPICS[0], partition });
        CHECK(high > low);
    }

    SECTION("message with key") {
        const string payload = "Hello world! 2";
        const string key = "such key";
        auto timestamp = system_clock::now();
        Producer producer(config);
        producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition)
                                                     .key(key)
                                                     .payload(payload)
                                                     .timestamp(timestamp));
        runner.try_join();

        const auto& messages = runner.get_messages();
        REQUIRE(messages.size() == 1);
        const auto& message = messages[0];
        CHECK(message.get_payload() == payload);
        CHECK(message.get_key() == key);
        CHECK(message.get_topic() == KAFKA_TOPICS[0]);
        CHECK(message.get_partition() == partition);
        CHECK(!!message.get_error() == false);
        REQUIRE(!!message.get_timestamp() == true);
        CHECK(message.get_timestamp()->get_timestamp() == duration_cast<milliseconds>(timestamp.time_since_epoch()));
    }
    
#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
    SECTION("message with key and move-able headers") {
        using Hdr = MessageBuilder::HeaderType;
        const string payload = "Hello world! 2";
        const string key = "such key";
        const string header1, header2 = "", header3 = "header3";
        
        const milliseconds timestamp{15};
        Producer producer(config);
        producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition)
                                                     .key(key)
                                                     .payload(payload)
                                                     .timestamp(timestamp)
                                                     .header(Hdr{})
                                                     .header(Hdr{"", header2})
                                                     .header(Hdr{"header3", header3}));
        runner.try_join();

        const auto& messages = runner.get_messages();
        REQUIRE(messages.size() == 1);
        const auto& message = messages[0];
        CHECK(message.get_payload() == payload);
        CHECK(message.get_key() == key);
        CHECK(message.get_topic() == KAFKA_TOPICS[0]);
        CHECK(message.get_partition() == partition);
        CHECK(!!message.get_error() == false);
        REQUIRE(!!message.get_timestamp() == true);
        CHECK(message.get_timestamp()->get_timestamp() == timestamp);
        //validate headers
        REQUIRE(!!message.get_header_list());
        REQUIRE(message.get_header_list().size() == 3);
        CHECK(message.get_header_list().front() == Hdr{});
        CHECK(message.get_header_list().at(1) == Hdr{"", header2});
        CHECK(message.get_header_list().back() == Hdr{"header3", header3});
    }
#endif //RD_KAFKA_HEADERS_SUPPORT_VERSION
    
    SECTION("message without message builder") {
        const string payload = "Goodbye cruel world!";
        const string key = "replay key";
        const milliseconds timestamp{15};
        Producer producer(config);
        producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition)
                                                     .key(key)
                                                     .payload(payload)
                                                     .timestamp(timestamp));
        runner.try_join();
        ConsumerRunner runner2(consumer, 1, 1);
        
        const auto& replay_messages = runner.get_messages();
        REQUIRE(replay_messages.size() == 1);
        const auto& replay_message = replay_messages[0];
        
        //produce the same message again
        producer.produce(replay_message);
        runner2.try_join();
        
        const auto& messages = runner2.get_messages();
        REQUIRE(messages.size() == 1);
        const auto& message = messages[0];
        CHECK(message.get_payload() == payload);
        CHECK(message.get_key() == key);
        CHECK(message.get_topic() == KAFKA_TOPICS[0]);
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
            CHECK(partition_count == KAFKA_NUM_PARTITIONS);
            CHECK(topic.get_name() == KAFKA_TOPICS[0]);
            return 0;
        });
        config.set_default_topic_configuration(topic_config);

        Producer producer(config);
        producer.produce(MessageBuilder(KAFKA_TOPICS[0]).key(key).payload(payload));
        while (producer.get_out_queue_length() > 0) {
            producer.poll();
        }
        runner.try_join();

        const auto& messages = runner.get_messages();
        REQUIRE(messages.size() == 1);
        const auto& message = messages[0];
        CHECK(message.get_payload() == payload);
        CHECK(message.get_key() == key);
        CHECK(message.get_topic() == KAFKA_TOPICS[0]);
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
            CHECK(partition_count == KAFKA_NUM_PARTITIONS);
            CHECK(topic.get_name() == KAFKA_TOPICS[0]);
            callback_called = true;
            return 0;
        });
        config.set_default_topic_configuration(topic_config);
        Producer producer(config);

        producer.produce(MessageBuilder(KAFKA_TOPICS[0]).key(key).payload(payload));
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
    set<string> payloads;

    // Create a consumer and subscribe to this topic
    Consumer consumer(make_consumer_config());
    consumer.subscribe({ KAFKA_TOPICS[0] });
    ConsumerRunner runner(consumer, message_count, KAFKA_NUM_PARTITIONS);

    // Now create a producer and produce a message
    Producer producer(make_producer_config());
    const string payload_base = "Hello world ";
    for (size_t i = 0; i < message_count; ++i) {
        const string payload = payload_base + to_string(i);
        payloads.insert(payload);
        producer.produce(MessageBuilder(KAFKA_TOPICS[0]).payload(payload));
    }
    runner.try_join();

    const auto& messages = runner.get_messages();
    REQUIRE(messages.size() == message_count);
    for (const auto& message : messages) {
        CHECK(message.get_topic() == KAFKA_TOPICS[0]);
        CHECK(payloads.erase(message.get_payload()) == 1);
        CHECK(!!message.get_error() == false);
        CHECK(!!message.get_key() == false);
        CHECK(message.get_partition() >= 0);
        CHECK(message.get_partition() < KAFKA_NUM_PARTITIONS);
    }
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
TEST_CASE("multiple messages with copy-able headers", "[producer][headers]") {
    using Hdr = MessageBuilder::HeaderType;
    size_t message_count = 2;
    string payload = "Hello world with headers";
    const string header1, header2 = "", header3 = "header3";

    // Create a consumer and subscribe to this topic
    Consumer consumer(make_consumer_config());
    consumer.subscribe({ KAFKA_TOPICS[0] });
    ConsumerRunner runner(consumer, message_count, KAFKA_NUM_PARTITIONS);

    // Now create a producer and produce a message
    Producer producer(make_producer_config());
    MessageBuilder builder(KAFKA_TOPICS[0]);
    builder.payload(payload)
             .header(Hdr{})
             .header(Hdr{"", header2})
             .header(Hdr{"header3", header3});
    producer.produce(builder);
    producer.produce(builder);
    
    //Check we still have the messages after production
    CHECK(!!builder.header_list());
    CHECK(builder.header_list().size() == 3);
    
    runner.try_join();

    const auto& messages = runner.get_messages();
    REQUIRE(messages.size() == message_count);
    const auto& message = messages[0];
    CHECK(message.get_payload() == payload);
    CHECK(!!message.get_error() == false);
    //validate headers
    REQUIRE(!!message.get_header_list());
    REQUIRE(message.get_header_list().size() == 3);
    CHECK(message.get_header_list().front() == Hdr{});
    CHECK(message.get_header_list().at(1) == Hdr{"", header2});
    CHECK(message.get_header_list().back() == Hdr{"header3", header3});
    
    //validate second message
    CHECK(messages[0].get_header_list() == messages[1].get_header_list());
    CHECK(messages[0].get_header_list().get_handle() != messages[1].get_header_list().get_handle());
}
#endif //RD_KAFKA_HEADERS_SUPPORT_VERSION

TEST_CASE("multiple sync messages", "[producer][buffered_producer][sync]") {
    size_t message_count = 10;
    set<string> payloads;

    // Create a consumer and subscribe to this topic
    Consumer consumer(make_consumer_config());
    consumer.subscribe({ KAFKA_TOPICS[0] });
    ConsumerRunner runner(consumer, message_count, KAFKA_NUM_PARTITIONS);

    // Now create a producer and produce a message
    BufferedProducer<string> producer(make_producer_config());
    producer.set_produce_success_callback(dr_callback);
    const string payload_base = "Hello world ";
    for (size_t i = 0; i < message_count; ++i) {
        const string payload = payload_base + to_string(i);
        payloads.insert(payload);
        producer.sync_produce(MessageBuilder(KAFKA_TOPICS[0]).payload(payload).user_data(&dr_data[i]));
    }
    runner.try_join();

    const auto& messages = runner.get_messages();
    REQUIRE(messages.size() == message_count);
    for (size_t i = 0; i < messages.size(); ++i) {
        const auto& message = messages[i];
        CHECK(message.get_topic() == KAFKA_TOPICS[0]);
        CHECK(payloads.erase(message.get_payload()) == 1);
        CHECK(!!message.get_error() == false);
        CHECK(!!message.get_key() == false);
        CHECK(message.get_partition() >= 0);
        CHECK(message.get_partition() < KAFKA_NUM_PARTITIONS);
    }
}

TEST_CASE("replay sync messages with errors", "[producer][buffered_producer][sync]") {
    size_t num_retries = 4;

    // Create a consumer and subscribe to this topic
    Consumer consumer(make_consumer_config());
    consumer.subscribe({ KAFKA_TOPICS[0] });
    ConsumerRunner runner(consumer, 2*(num_retries+1), KAFKA_NUM_PARTITIONS);

    // Now create a producer and produce a message
    ErrorProducer<string> producer(make_producer_config(), BufferedProducer<string>::TestParameters{true, false});
    producer.set_produce_failure_callback(dr_failure_callback);
    producer.set_max_number_retries(num_retries);
    string payload = "Hello world";
    MessageBuilder builder(KAFKA_TOPICS[0]);
    builder.payload(payload).user_data(&dr_data[0]);
    
    //Produce the same message twice
    producer.sync_produce(builder);
    producer.sync_produce(builder);
    runner.try_join();

    const auto& messages = runner.get_messages();
    REQUIRE(messages.size() == 2*(num_retries+1));
    for (size_t i = 0; i < messages.size(); ++i) {
        const auto& message = messages[i];
        CHECK(message.get_topic() == KAFKA_TOPICS[0]);
        CHECK(message.get_payload() == payload);
        CHECK(!!message.get_error() == false);
        CHECK(!!message.get_key() == false);
        CHECK(message.get_partition() >= 0);
        CHECK(message.get_partition() < KAFKA_NUM_PARTITIONS);
    }
}

TEST_CASE("replay async messages with errors", "[producer][buffered_producer][async]") {
    size_t num_retries = 4;
    int exit_flag = 0;

    // Now create a producer and produce a message
    ErrorProducer<string> producer(make_producer_config(),
                                   BufferedProducer<string>::TestParameters{false, true});
    producer.set_max_number_retries(num_retries);
    thread flusher_thread(async_flusher_run, ref(producer), ref(exit_flag), 0);
    string payload = "Hello world";
    producer.produce(MessageBuilder(KAFKA_TOPICS[0]).payload(payload));
    this_thread::sleep_for(milliseconds(2000));
    exit_flag = 1;
    flusher_thread.join();
    REQUIRE(producer.get_total_messages_produced() == 0);
    CHECK(producer.get_total_messages_dropped() == 1);
}

TEST_CASE("buffered producer", "[producer][buffered_producer]") {
    int partition = 0;

    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPICS[0], partition) });
    ConsumerRunner runner(consumer, 3, 1);

    // Now create a buffered producer and produce two messages
    BufferedProducer<string> producer(make_producer_config());
    const string payload = "Hello world! 2";
    const string key = "such key";
    producer.add_message(MessageBuilder(KAFKA_TOPICS[0]).partition(partition)
                                                    .key(key)
                                                    .payload(payload));
    producer.add_message(producer.make_builder(KAFKA_TOPICS[0]).partition(partition).payload(payload));
    producer.flush();
    producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(partition).payload(payload));
    producer.wait_for_acks();
    // Add another one but then clear it
    producer.add_message(producer.make_builder(KAFKA_TOPICS[0]).partition(partition).payload(payload));
    producer.clear();
    runner.try_join();

    const auto& messages = runner.get_messages();
    REQUIRE(messages.size() == 3);
    const auto& message = messages[0];
    CHECK(message.get_key() == key);
    CHECK(message.get_topic() == KAFKA_TOPICS[0]);
    CHECK(message.get_partition() == partition);
    CHECK(!!message.get_error() == false);

    CHECK(!!messages[1].get_key() == false);
    CHECK(!!messages[2].get_key() == false);
    for (const auto& message : messages) {
        CHECK(message.get_payload() == payload);
    }
}

TEST_CASE("buffered producer with limited buffer", "[producer]") {
    int partition = 0;
    int num_messages = 4;
    
    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPICS[0], partition) });
    ConsumerRunner runner(consumer, 3, 1);

    // Now create a buffered producer and produce two messages
    BufferedProducer<string> producer(make_producer_config());
    const string payload = "Hello world! 2";
    const string key = "such key";
    REQUIRE(producer.get_buffer_size() == 0);
    REQUIRE(producer.get_max_buffer_size() == -1);
    
    // Limit the size of the internal buffer
    producer.set_max_buffer_size(num_messages-1);
    while (num_messages--) {
        producer.add_message(MessageBuilder(KAFKA_TOPICS[0]).partition(partition).key(key).payload(payload));
    }
    REQUIRE(producer.get_buffer_size() == 1);
    
    // Finish the runner
    runner.try_join();

    // Validate messages received
    const auto& messages = runner.get_messages();
    REQUIRE(messages.size() == producer.get_max_buffer_size());
}

TEST_CASE("multi-threaded buffered producer", "[producer][buffered_producer]") {
    int partition = 0;
    vector<thread> threads;
    int num_messages = 50;
    int num_flush = 10;
    int exit_flag = 0;
    condition_variable clear;

    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPICS[0], partition) });
    ConsumerRunner runner(consumer, num_messages, 1);
    
    BufferedProducer<string> producer(make_producer_config());
    
    threads.push_back(thread(producer_run, ref(producer), ref(exit_flag), ref(clear), num_messages, partition));
    threads.push_back(thread(flusher_run, ref(producer), ref(exit_flag), num_flush));
    
    // Wait for completion
    runner.try_join();
    for (auto&& thread : threads) {
        thread.join();
    }
    const auto& messages = runner.get_messages();
    REQUIRE(messages.size() == num_messages);
    REQUIRE(producer.get_flushes_in_progress() == 0);
    REQUIRE(producer.get_pending_acks() == 0);
    REQUIRE(producer.get_total_messages_produced() == num_messages);
    REQUIRE(producer.get_buffer_size() == 0);
}

TEST_CASE("clear multi-threaded buffered producer", "[producer][buffered_producer]") {
    int partition = 0;
    vector<thread> threads;
    int num_messages = 50;
    int exit_flag = 0;
    condition_variable clear;
    
    BufferedProducer<string> producer(make_producer_config());
    
    threads.push_back(thread(producer_run, ref(producer), ref(exit_flag), ref(clear), num_messages, partition));
    threads.push_back(thread(clear_run, ref(producer), ref(clear)));
    
    // Wait for completion
    for (auto&& thread : threads) {
        thread.join();
    }
    
    REQUIRE(producer.get_total_messages_produced() == 0);
    REQUIRE(producer.get_flushes_in_progress() == 0);
    REQUIRE(producer.get_pending_acks() == 0);
    REQUIRE(producer.get_buffer_size() < num_messages);
}
