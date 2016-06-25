#include <gtest/gtest.h>
#include "cppkafka/configuration.h"
#include "cppkafka/exceptions.h"

using namespace cppkafka;

using std::string;

class ConfigurationTest : public testing::Test {
public:
    
};

TEST_F(ConfigurationTest, GetSetConfig) {
    Configuration config;
    config.set("group.id", "foo").set("metadata.broker.list", "asd:9092");
    EXPECT_EQ("foo", config.get("group.id"));
    EXPECT_EQ("asd:9092", config.get("metadata.broker.list"));
    EXPECT_EQ("foo", config.get<string>("group.id"));

    EXPECT_THROW(config.get("asd"), ConfigOptionNotFound);
}

TEST_F(ConfigurationTest, GetSetTopicConfig) {
    TopicConfiguration config;
    config.set("auto.commit.enable", true).set("offset.store.method", "broker");
    EXPECT_EQ("true", config.get("auto.commit.enable"));
    EXPECT_EQ("broker", config.get("offset.store.method"));
    EXPECT_EQ(true, config.get<bool>("auto.commit.enable"));

    EXPECT_THROW(config.get("asd"), ConfigOptionNotFound);
}

TEST_F(ConfigurationTest, ConfigSetMultiple) {
    Configuration config = {
        { "group.id", "foo" },
        { "metadata.broker.list", string("asd:9092") },
        { "message.max.bytes", 2000 },
        { "topic.metadata.refresh.sparse", true }
    };
    EXPECT_EQ("foo", config.get("group.id"));
    EXPECT_EQ("asd:9092", config.get("metadata.broker.list"));
    EXPECT_EQ(2000, config.get<int>("message.max.bytes"));
    EXPECT_EQ(true, config.get<bool>("topic.metadata.refresh.sparse"));
}

TEST_F(ConfigurationTest, TopicConfigSetMultiple) {
    TopicConfiguration config = {
        { "compression.codec", "none" },
        { "offset.store.method", string("file") },
        { "request.required.acks", 2 },
        { "produce.offset.report", true }
    };
    EXPECT_EQ("none", config.get("compression.codec"));
    EXPECT_EQ("file", config.get("offset.store.method"));
    EXPECT_EQ(2, config.get<int>("request.required.acks"));
    EXPECT_EQ(true, config.get<bool>("produce.offset.report"));
}

TEST_F(ConfigurationTest, SetDefaultTopicConfiguration) {
    Configuration config;
    config.set_default_topic_configuration({{ "request.required.acks", 2 }});

    const auto& topic_config = config.get_default_topic_configuration();
    EXPECT_TRUE(topic_config);
    EXPECT_EQ(2, topic_config->get<int>("request.required.acks"));
}

TEST_F(ConfigurationTest, SetOverloads) {
    Configuration config;
    config.set("enable.auto.commit", true);
    config.set("auto.commit.interval.ms", 100);

    EXPECT_EQ("true", config.get("enable.auto.commit"));
    EXPECT_EQ("100", config.get("auto.commit.interval.ms"));
    EXPECT_EQ(100, config.get<int>("auto.commit.interval.ms"));
}

TEST_F(ConfigurationTest, GetAll) {
    Configuration config;
    config.set("enable.auto.commit", false);
    auto option_map = config.get_all();
    EXPECT_EQ("false", option_map.at("enable.auto.commit"));
}

TEST_F(ConfigurationTest, TopicGetAll) {
    TopicConfiguration config;
    config.set("auto.commit.enable", false);
    auto option_map = config.get_all();
    EXPECT_EQ("false", option_map.at("auto.commit.enable"));
}
