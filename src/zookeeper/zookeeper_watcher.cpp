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

#include <stdexcept>
#include <sstream>
#include <picojson.h>
#include "zookeeper/zookeeper_watcher.h"
#include "exceptions.h"

using std::string;
using std::unique_ptr;
using std::ostringstream;
using std::runtime_error;
using std::chrono::milliseconds;

using picojson::value;
using picojson::object;

namespace cppkafka {

const milliseconds ZookeeperWatcher::DEFAULT_RECEIVE_TIMEOUT{10000};
const string ZookeeperWatcher::BROKERS_PATH = "/brokers/ids";

ZookeeperWatcher::ZookeeperWatcher(const string& endpoint)
: ZookeeperWatcher(endpoint, DEFAULT_RECEIVE_TIMEOUT) {

}

ZookeeperWatcher::ZookeeperWatcher(const string& endpoint, milliseconds receive_timeout)
: handle_(nullptr, nullptr) {
    auto raw_handle = zookeeper_init(endpoint.data(), &ZookeeperWatcher::handle_event_proxy,
                                     receive_timeout.count(), nullptr, this, 0);
    if (!raw_handle) {
        // TODO: make this a proper exception
        throw runtime_error("Failed to create zookeeper handle");
    }
    handle_ = HandlePtr(raw_handle, &zookeeper_close);
}

string ZookeeperWatcher::get_brokers() {
    using VectorPtr = unique_ptr<String_vector, decltype(&deallocate_String_vector)>;
    String_vector brokers;
    if (zoo_get_children(handle_.get(), BROKERS_PATH.data(), 1, &brokers) != ZOK) {
        throw ZookeeperException("Failed to get broker list from zookeeper");
    }
    // RAII up this pointer
    ostringstream oss;
    VectorPtr _(&brokers, &deallocate_String_vector);
    for (int i = 0; i < brokers.count; i++) {
        char config_line[1024];
        string path = "/brokers/ids/" + string(brokers.data[i]);
        int config_len = sizeof(config_line);
        zoo_get(handle_.get(), path.data(), 0, config_line, &config_len, NULL);
        if (config_len > 0) {
            config_line[config_len] = 0;

            value root;
            string error = picojson::parse(root, config_line);
            if (!error.empty() || !root.is<object>()) {
                throw ZookeeperException("Failed to parse zookeeper json: " + error);
            }
            const value::object& json_object = root.get<object>();
            const value& host_json = json_object.at("host");
            const value& port_json = json_object.at("port");
            if (!host_json.is<string>() || !port_json.is<double>()) {
                throw ZookeeperException("Invalid JSON received from zookeeper");
            }
            string host = host_json.get<string>();
            int port = port_json.get<double>();
            if (i != 0) {
                oss << ",";
            }
            oss << host << ":" << port;
        }
    }
    return oss.str();
}

void ZookeeperWatcher::handle_event_proxy(zhandle_t*, int type, int state, const char* path,
                                          void* ctx) {
    auto self = static_cast<ZookeeperWatcher*>(ctx);
    self->handle_event(type, state, path);
}

void ZookeeperWatcher::handle_event(int type, int state, const char* path) {
    if (type == ZOO_CHILD_EVENT && path == BROKERS_PATH) {
        string brokers = get_brokers();
        // TODO: Callback!
    }   
}

} // cppkafka
