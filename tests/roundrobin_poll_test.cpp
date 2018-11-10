#include <vector>
#include <thread>
#include <set>
#include <mutex>
#include <chrono>
#include <iterator>
#include <condition_variable>
#include <catch.hpp>
#include <memory>
#include <stdexcept>
#include "cppkafka/cppkafka.h"
#include "test_utils.h"

using std::vector;
using std::move;
using std::string;
using std::exception;
using std::thread;
using std::set;
using std::mutex;
using std::tie;
using std::condition_variable;
using std::lock_guard;
using std::unique_lock;
using std::unique_ptr;
using std::make_move_iterator;
using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::system_clock;

using namespace cppkafka;

//==================================================================================
//                           Helper functions
//==================================================================================
static Configuration make_producer_config() {
    Configuration config = {    
        { "metadata.broker.list", KAFKA_TEST_INSTANCE },
    };
    return config;
}

static Configuration make_consumer_config(const string& group_id = make_consumer_group_id()) {
    Configuration config = {
        { "metadata.broker.list", KAFKA_TEST_INSTANCE },
        { "enable.auto.commit", false },
        { "group.id", group_id },
    };
    return config;
}

static vector<int> make_roundrobin_partition_vector(int total_messages) {
    vector<int> partition_order;
    for (int i = 0, partition = 0; i < total_messages+1; ++i) {
        if ((i % KAFKA_NUM_PARTITIONS) == 0) {
            partition = 0;
        }
        partition_order.push_back(partition++);
    }
    return partition_order;
}

//========================================================================
//                              TESTS
//========================================================================

TEST_CASE("roundrobin consumer test", "[roundrobin consumer]") {
    TopicPartitionList assignment;
    int messages_per_partition = 3;
    int total_messages = KAFKA_NUM_PARTITIONS * messages_per_partition;

    // Create a consumer and subscribe to the topic
    PollStrategyAdapter consumer(make_consumer_config());
    consumer.subscribe({ KAFKA_TOPICS[0] });
    consumer.add_polling_strategy(unique_ptr<PollInterface>(new RoundRobinPollStrategy(consumer)));
    
    PollConsumerRunner runner(consumer, total_messages, KAFKA_NUM_PARTITIONS);

    // Produce messages so we stop the consumer
    BufferedProducer<string> producer(make_producer_config());
    string payload = "RoundRobin";
    
    // push 3 messages in each partition
    for (int i = 0; i < total_messages; ++i) {
        producer.produce(MessageBuilder(KAFKA_TOPICS[0])
                            .partition(i % KAFKA_NUM_PARTITIONS)
                            .payload(payload));
    }
    producer.flush();

    runner.try_join();
    
    // Check that we have all messages
    REQUIRE(runner.get_messages().size() == total_messages);
    
    // Check that we have one message from each partition in desired order
    vector<int> partition_order = make_roundrobin_partition_vector(total_messages+KAFKA_NUM_PARTITIONS);
    int partition_idx;
    for (int i = 0; i < total_messages; ++i) {
        if (i == 0) {
            // find first polled partition index
            partition_idx = runner.get_messages()[i].get_partition();
        }
        REQUIRE(runner.get_messages()[i].get_partition() == partition_order[i+partition_idx]);
        REQUIRE((string)runner.get_messages()[i].get_payload() == payload);
    }
    
    //============ resume original poll strategy =============//
    
    //validate that once the round robin strategy is deleted, normal poll works as before
    consumer.delete_polling_strategy();
    
    ConsumerRunner serial_runner(consumer, total_messages, KAFKA_NUM_PARTITIONS);
    
    payload = "SerialPolling";
    // push 3 messages in each partition
    for (int i = 0; i < total_messages; ++i) {
        producer.produce(MessageBuilder(KAFKA_TOPICS[0]).partition(i%KAFKA_NUM_PARTITIONS).payload(payload));
    }
    producer.flush();
    serial_runner.try_join();
    
    // Check that we have all messages
    REQUIRE(serial_runner.get_messages().size() == total_messages);
    
    for (int i = 0; i < total_messages; ++i) {
        REQUIRE((string)serial_runner.get_messages()[i].get_payload() == payload);
    }
}

