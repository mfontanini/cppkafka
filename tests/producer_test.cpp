#include <thread>
#include <mutex>
#include <chrono>
#include <condition_variable>
#include <gtest/gtest.h>
#include "cppkafka/producer.h"
#include "cppkafka/consumer.h"

using std::string;
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
    ConsumerRunner(Consumer& consumer, size_t expected) 
    : consumer_(consumer) {
        bool booted = false;
        mutex mtx;
        condition_variable cond;
        thread_ = thread([&, expected]() {
            consumer_.set_timeout(milliseconds(100));
            bool found_eof = false;
            auto start = system_clock::now();
            while (system_clock::now() - start < seconds(10) && messages_.size() < expected) {
                Message msg = consumer_.poll();
                if (msg && !found_eof && msg.get_error() == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    found_eof = true;
                    lock_guard<mutex> _(mtx);
                    booted = true;
                    cond.notify_one();
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
        config.set("enable.auto.commit", "false");
        config.set("group.id", "producer_test");
        return config;
    }
};

const string ProducerTest::KAFKA_TOPIC = "cppkafka_test1";

TEST_F(ProducerTest, test1) {
    int partition = 0;

    // Create a consumer and assign this topic/partition
    Consumer consumer(make_consumer_config());
    consumer.assign({ TopicPartition(KAFKA_TOPIC, partition) });
    ConsumerRunner runner(consumer, 1);

    // Now create a producer and produce a message
    Producer producer(make_producer_config());
    Topic topic = producer.get_topic(KAFKA_TOPIC);
    string payload = "Hello world!";
    producer.produce(topic, partition, Buffer(payload.begin(), payload.end()));
    runner.try_join();

    const auto& messages = runner.get_messages();
    ASSERT_EQ(1, messages.size());
    EXPECT_EQ(payload, messages[0].get_payload().as_string());
}
