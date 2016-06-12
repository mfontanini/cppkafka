#include <gtest/gtest.h>
#include "cppkafka/zookeeper/zookeeper_watcher.h"
#include "cppkafka/exceptions.h"

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

TEST_F(ZookeeperWatcherTest, InvalidEndpointThrows) {
    ZookeeperWatcher watcher("127.0.0.1:1212");
    EXPECT_THROW(watcher.get_brokers(), ZookeeperException);
}

TEST_F(ZookeeperWatcherTest, SubscribeUnsubscribe) {
    ZookeeperWatcher watcher(ZOOKEEPER_TEST_INSTANCE);
    string id = watcher.subscribe([](const string&) { });
    EXPECT_EQ(1, watcher.get_subscriber_count());
    watcher.unsubscribe(id);
    EXPECT_EQ(0, watcher.get_subscriber_count());
}
