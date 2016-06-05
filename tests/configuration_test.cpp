#include <gtest/gtest.h>
#include "cppkafka/configuration.h"
#include "cppkafka/exceptions.h"

using namespace cppkafka;

class ConfigurationTest : public testing::Test {
public:
    
};

TEST_F(ConfigurationTest, GetSetConfig) {
    Configuration config;
    config.set("group.id", "foo");
    EXPECT_EQ("foo", config.get("group.id"));

    EXPECT_THROW(config.get("asd"), ConfigOptionNotFound);
}

TEST_F(ConfigurationTest, GetSetTopicConfig) {
    TopicConfiguration config;
    config.set("auto.commit.enable", "true");
    EXPECT_EQ("true", config.get("auto.commit.enable"));

    EXPECT_THROW(config.get("asd"), ConfigOptionNotFound);
}

TEST_F(ConfigurationTest, SetOverloads) {
    Configuration config;
    config.set("enable.auto.commit", true);
    config.set("auto.commit.interval.ms", 100);

    EXPECT_EQ("true", config.get("enable.auto.commit"));
    EXPECT_EQ("100", config.get("auto.commit.interval.ms"));
}