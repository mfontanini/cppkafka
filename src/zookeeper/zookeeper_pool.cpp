/*
 * Copyright (c) 2016, Matias Fontanini
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following disclaimer
 *   in the documentation and/or other materials provided with the
 *   distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include "zookeeper/zookeeper_pool.h"
#include "zookeeper/zookeeper_subscription.h"
#include "exceptions.h"

using std::string;
using std::mutex;
using std::lock_guard;
using std::forward_as_tuple;
using std::piecewise_construct;
using std::chrono::milliseconds;

namespace cppkafka {

ZookeeperPool& ZookeeperPool::instance() {
    static ZookeeperPool the_instance;
    return the_instance;
}

ZookeeperSubscription ZookeeperPool::subscribe(const string& endpoint,
                                             milliseconds receive_timeout,
                                             ZookeeperWatcher::WatcherCallback callback) {
    lock_guard<mutex> _(watchers_mutex_);
    auto iter = watchers_.find(endpoint);
    if (iter == watchers_.end()) {
        iter = watchers_.emplace(piecewise_construct, forward_as_tuple(endpoint),
                                 forward_as_tuple(endpoint, receive_timeout)).first;
    }
    string id = iter->second.subscribe(move(callback));
    return ZookeeperSubscription(endpoint, id);
}

void ZookeeperPool::unsubscribe(const ZookeeperSubscription& subscriber) {
    lock_guard<mutex> _(watchers_mutex_);
    auto iter = watchers_.find(subscriber.get_endpoint());
    if (iter != watchers_.end()) {
        iter->second.unsubscribe(subscriber.get_subscription_id());
    }
}

string ZookeeperPool::get_brokers(const string& endpoint) {
    lock_guard<mutex> _(watchers_mutex_);
    auto iter = watchers_.find(endpoint);
    if (iter == watchers_.end()) {
        throw ZookeeperException("No zookeeper watcher for given endpoint");
    }
    return iter->second.get_brokers();
}

size_t ZookeeperPool::get_subscriber_count(const string& endpoint) const {
    lock_guard<mutex> _(watchers_mutex_);
    auto iter = watchers_.find(endpoint);
    if (iter == watchers_.end()) {
        return 0;
    }
    else {
        return iter->second.get_subscriber_count();
    }
}

} // cppkafka
