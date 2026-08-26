#include <chrono>
#include <catch.hpp>
#include "cppkafka/consumer.h"
#include "cppkafka/exceptions.h"

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

using namespace cppkafka;

namespace {

// Needs no KAFKA_TEST_INSTANCE: nothing accepts connections on port 1, so the group never gets a
// coordinator, librdkafka enqueues no commit reply, and the deadline is the only thing that can end
// the call. The blocking overloads instead return `_WAIT_COORD` one session.timeout.ms later.
Configuration make_config() {
    Configuration config;
    config.set("metadata.broker.list", "127.0.0.1:1");
    config.set("group.id", "cppkafka_commit_timeout_test");
    config.set("enable.auto.commit", "false");
    config.set("enable.auto.offset.store", "false");
    config.set("socket.timeout.ms", "1000");
    return config;
}

template <typename Commit>
void check_the_deadline_returns(const Commit& commit) {
    const auto start = steady_clock::now();
    try {
        commit();
        FAIL("the commit returned instead of reaching the deadline");
    }
    catch (const HandleException& ex) {
        // Any other code means something answered first, and the elapsed-time bound below would then
        // hold without the deadline being what returned.
        CHECK(ex.get_error().get_error() == RD_KAFKA_RESP_ERR__TIMED_OUT);
    }
    CHECK(duration_cast<milliseconds>(steady_clock::now() - start).count() < 15000);
}

} // namespace

TEST_CASE("commit bounded by a timeout", "[consumer][commit_timeout]") {
    const TopicPartitionList offsets{TopicPartition("cppkafka_commit_timeout_test", 0, 42)};

    SECTION("current assignment") {
        Consumer consumer(make_config());
        // A commit carrying no offset is answered `_NO_OFFSET` before the queue is polled, so a
        // consumer holding no stored offset cannot reach the deadline.
        consumer.assign(offsets);
        consumer.store_offsets(offsets);

        check_the_deadline_returns([&] { consumer.commit(milliseconds(3000)); });
    }

    SECTION("explicit offsets") {
        Consumer consumer(make_config());

        check_the_deadline_returns([&] { consumer.commit(offsets, milliseconds(3000)); });
    }

    SECTION("consumed message") {
        Consumer consumer(make_config());

        // rd_kafka_topic_new is the deprecated per-topic-handle API; it is here only so the
        // fabricated message has a topic name to commit — no broker is contacted.
        rd_kafka_topic_t * topic = rd_kafka_topic_new(consumer.get_handle(),
                                                      "cppkafka_commit_timeout_test", nullptr);
        REQUIRE(topic != nullptr);
        rd_kafka_message_t bare_handle{};
        bare_handle.err = RD_KAFKA_RESP_ERR_NO_ERROR;
        bare_handle.rkt = topic;
        bare_handle.partition = 0;
        bare_handle.offset = 42;
        Message message = Message::make_non_owning(&bare_handle);

        check_the_deadline_returns([&] { consumer.commit(message, milliseconds(3000)); });

        rd_kafka_topic_destroy(topic);
    }
}
