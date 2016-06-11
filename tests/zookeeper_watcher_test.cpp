#include <gtest/gtest.h>
#include "cppkafka/zookeeper/zookeeper_watcher.h"

using std::string;

using namespace cppkafka;

class ZookeeperWatcherTest : public testing::Test {
public:
 
};

TEST_F(ZookeeperWatcherTest, GetBrokers) {
    ZookeeperWatcher watcher(ZOOKEEPER_TEST_INSTANCE);
    string brokers = watcher.get_brokers();
    EXPECT_EQ(KAFKA_TEST_INSTANCE, brokers);
}
