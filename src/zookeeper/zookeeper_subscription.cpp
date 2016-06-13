#include "zookeeper/zookeeper_subscription.h"
#include "zookeeper/zookeeper_pool.h"

using std::string;

namespace cppkafka {

ZookeeperSubscription::ZookeeperSubscription(string endpoint, string subscription_id) 
: endpoint_(move(endpoint)), subscription_id_(move(subscription_id)) {

}

ZookeeperSubscription::~ZookeeperSubscription() {
    ZookeeperPool::instance().unsubscribe(*this);
}

const string& ZookeeperSubscription::get_endpoint() const {
    return endpoint_;
}

const string& ZookeeperSubscription::get_subscription_id() const {
    return subscription_id_;
}

} // cppkafka
