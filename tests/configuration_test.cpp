#include <catch.hpp>
#include "cppkafka/configuration.h"
#include "cppkafka/exceptions.h"

using namespace cppkafka;

using std::string;

TEST_CASE("get/set", "[config]") {
    Configuration config;

    SECTION("get existing") {
        config.set("group.id", "foo").set("metadata.broker.list", "asd:9092");
        REQUIRE(config.get("group.id") == "foo");
        REQUIRE(config.get("metadata.broker.list") == "asd:9092");
        REQUIRE(config.get<string>("group.id") == "foo");
    }

    SECTION("get non existent") {
        REQUIRE_THROWS_AS(config.get("asd"), ConfigOptionNotFound);
    }

    SECTION("set overloads") {
        config.set("enable.auto.commit", true);
        config.set("auto.commit.interval.ms", 100);

        REQUIRE(config.get("enable.auto.commit") == "true");
        REQUIRE(config.get("auto.commit.interval.ms") == "100");
        REQUIRE(config.get<int>("auto.commit.interval.ms") == 100);
    }

    SECTION("set multiple") {
        config = {
            { "group.id", "foo" },
            { "metadata.broker.list", string("asd:9092") },
            { "message.max.bytes", 2000 },
            { "topic.metadata.refresh.sparse", true }
        };

        REQUIRE(config.get("group.id") == "foo");
        REQUIRE(config.get("metadata.broker.list") == "asd:9092");
        REQUIRE(config.get<int>("message.max.bytes") == 2000);
        REQUIRE(config.get<bool>("topic.metadata.refresh.sparse") == true);
    }

    SECTION("default topic config") {
        config.set_default_topic_configuration({{ "request.required.acks", 2 }});

        const auto& topic_config = config.get_default_topic_configuration();
        REQUIRE(!!topic_config == true);
        REQUIRE(topic_config->get<int>("request.required.acks") == 2);
    }

    SECTION("get all") {
        config.set("enable.auto.commit", false);
        auto option_map = config.get_all();
        REQUIRE(option_map.at("enable.auto.commit") == "false");
    }
}

TEST_CASE("topic config get/set", "[config]") {
    TopicConfiguration config;

    SECTION("get existing") {
        config.set("auto.commit.enable", true).set("offset.store.method", "broker");
        REQUIRE(config.get("auto.commit.enable") == "true");
        REQUIRE(config.get("offset.store.method") == "broker");
        REQUIRE(config.get<bool>("auto.commit.enable") == true);
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
        REQUIRE(config.get("compression.codec") == "none");
        REQUIRE(config.get("offset.store.method") == "file");
        REQUIRE(config.get<int>("request.required.acks") == 2);
        REQUIRE(config.get<bool>("produce.offset.report") == true);
    }

    SECTION("get all") {
        config.set("auto.commit.enable", false);
        auto option_map = config.get_all();
        REQUIRE(option_map.at("auto.commit.enable") == "false");
    }
}
