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

#ifndef CPPKAFKA_ZOOKEEPER_POOL_H
#define CPPKAFKA_ZOOKEEPER_POOL_H

#include <map>
#include <string>
#include <chrono>
#include <mutex>
#include "zookeeper/zookeeper_watcher.h"

namespace cppkafka {

class ZookeeperSubscription;

/**
 * \brief Pool of zookeeper handles
 *
 * This class is used internally by cppkafka.
 */
class ZookeeperPool {
public:
    /**
     * Get singleton instance
     */
    static ZookeeperPool& instance();

    /**
     * Subscribe to the given endpoint
     *
     * \param endpoint The zookeeper endpoint to subscribe to
     * \param receive_timeout The zookeeper receive timeout
     * \param callback The callback to be executed on updates
     *
     * \return A ZookeeperSubscription that will auto-unsubscribe upon destruction
     */
    ZookeeperSubscription subscribe(const std::string& endpoint,
                                    std::chrono::milliseconds receive_timeout,
                                    ZookeeperWatcher::WatcherCallback callback);

    /**
     * Unsubscribes from a previous subscription
     *
     * \param subscriber The subscriber return by a previous call to ZookeeperPool::subscribe
     */
    void unsubscribe(const ZookeeperSubscription& subscriber);

    /**
     * \brief Gets the broker list for the given zookeeper endpoint
     *
     * Requires having previously called subscribe for this endpoint at least once.
     *
     * \param endpoint The endpoint for which to get the broker list
     */
    std::string get_brokers(const std::string& endpoint);

    /** 
     * Gets the amount of subscribers for the given zookeeper endpoint
     */ 
    size_t get_subscriber_count(const std::string& endpoint) const;
private:
    using WatchersMap = std::map<std::string, ZookeeperWatcher>;

    WatchersMap watchers_;
    mutable std::mutex watchers_mutex_;
};

} // cppkafka

#endif // CPPKAFKA_ZOOKEEPER_POOL_H
