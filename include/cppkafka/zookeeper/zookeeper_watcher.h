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

#ifndef CPPKAFKA_ZOOKEEPER_WATCHER_H
#define CPPKAFKA_ZOOKEEPER_WATCHER_H

#include <memory>
#include <string>
#include <chrono>
#include <map>
#include <functional>
#include <mutex>
#include <zookeeper/zookeeper.h>

namespace cppkafka {

/**
 * \cond
 */
class ZookeeperWatcher {
public:
    static const std::chrono::milliseconds DEFAULT_RECEIVE_TIMEOUT;

    using WatcherCallback = std::function<void(const std::string& brokers)>;

    ZookeeperWatcher(const std::string& endpoint);
    ZookeeperWatcher(const std::string& endpoint, std::chrono::milliseconds receive_timeout);

    void setup_watcher();

    std::string subscribe(WatcherCallback callback); 
    void unsubscribe(const std::string& id);

    std::string get_brokers();
    size_t get_subscriber_count() const;
private:
    static const std::string BROKERS_PATH;

    using HandlePtr = std::unique_ptr<zhandle_t, decltype(&zookeeper_close)>;
    using CallbackMap = std::map<std::string, WatcherCallback>;

    static void handle_event_proxy(zhandle_t* zh, int type, int state, const char* path,
                                   void* ctx);
    void handle_event(int type, int state, const char* path);
    std::string generate_id();

    HandlePtr handle_;
    CallbackMap callbacks_;
    mutable std::mutex callbacks_mutex_;
    size_t id_counter_{0};
};

/**
 * \endcond
 */

} // cppkafka

#endif // CPPKAFKA_ZOOKEEPER_WATCHER_H
