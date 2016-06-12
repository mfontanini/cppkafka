#include "zookeeper/zookeeper_subscriber.h"
#include "zookeeper/zookeeper_pool.h"

using std::string;

namespace cppkafka {

ZookeeperSubscriber::ZookeeperSubscriber(string endpoint, string subscription_id) 
: endpoint_(move(endpoint)), subscription_id_(move(subscription_id)) {

}

ZookeeperSubscriber::~ZookeeperSubscriber() {
    ZookeeperPool::instance().unsubscribe(*this);
}

const string& ZookeeperSubscriber::get_endpoint() const {
    return endpoint_;
}

const string& ZookeeperSubscriber::get_subscription_id() const {
    return subscription_id_;
}

} // cppkafka
