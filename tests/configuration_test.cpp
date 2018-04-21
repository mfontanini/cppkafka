#include <catch.hpp>
#include "cppkafka/configuration.h"
#include "cppkafka/exceptions.h"

using namespace cppkafka;

using std::string;

TEST_CASE("normal config", "[config]") {
    Configuration config;

    SECTION("get existing") {
        config.set("group.id", "foo").set("metadata.broker.list", "asd:9092");
        CHECK(config.get("group.id") == "foo");
        CHECK(config.get("metadata.broker.list") == "asd:9092");
        CHECK(config.get<string>("group.id") == "foo");
    }

    SECTION("get non existent") {
        REQUIRE_THROWS_AS(config.get("asd"), ConfigOptionNotFound);
    }

    SECTION("set overloads") {
        config.set("enable.auto.commit", true);
        config.set("auto.commit.interval.ms", 100);

        CHECK(config.get("enable.auto.commit") == "true");
        CHECK(config.get("auto.commit.interval.ms") == "100");
        CHECK(config.get<int>("auto.commit.interval.ms") == 100);
    }

    SECTION("set multiple") {
        config = {
            { "group.id", "foo" },
            { "metadata.broker.list", string("asd:9092") },
            { "message.max.bytes", 2000 },
            { "topic.metadata.refresh.sparse", true }
        };

        CHECK(config.get("group.id") == "foo");
        CHECK(config.get("metadata.broker.list") == "asd:9092");
        CHECK(config.get<int>("message.max.bytes") == 2000);
        CHECK(config.get<bool>("topic.metadata.refresh.sparse") == true);
    }

    SECTION("default topic config") {
        config.set_default_topic_configuration({{ "request.required.acks", 2 }});

        const auto& topic_config = config.get_default_topic_configuration();
        CHECK(!!topic_config == true);
        CHECK(topic_config->get<int>("request.required.acks") == 2);
    }

    SECTION("get all") {
        config.set("enable.auto.commit", false);
        auto option_map = config.get_all();
        CHECK(option_map.at("enable.auto.commit") == "false");
    }
}

TEST_CASE("topic config", "[config]") {
    TopicConfiguration config;

    SECTION("get existing") {
        config.set("auto.commit.enable", true).set("offset.store.method", "broker");
        CHECK(config.get("auto.commit.enable") == "true");
        CHECK(config.get("offset.store.method") == "broker");
        CHECK(config.get<bool>("auto.commit.enable") == true);
    }

    SECTION("get non existent") {
        REQUIRE_THROWS_AS(config.get("asd"), ConfigOptionNotFound);
    }

    SECTION("set multiple") {
        config = {
            { "compression.codec", "none" },
            { "offset.store.method", string("file") },
            { "request.required.acks", 2 },
            { "produce.offset.report", true }
        };
        CHECK(config.get("compression.codec") == "none");
        CHECK(config.get("offset.store.method") == "file");
        CHECK(config.get<int>("request.required.acks") == 2);
        CHECK(config.get<bool>("produce.offset.report") == true);
    }

    SECTION("get all") {
        config.set("auto.commit.enable", false);
        auto option_map = config.get_all();
        CHECK(option_map.at("auto.commit.enable") == "false");
    }
}
