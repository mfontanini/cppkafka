/*
 * Copyright (c) 2017, Matias Fontanini
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

namespace cppkafka {

// Callback proxies

template <typename PRODUCER>
void delivery_report_callback_proxy(rd_kafka_t*, const rd_kafka_message_t* msg, void *opaque) {
    PRODUCER* handle = static_cast<PRODUCER*>(opaque);
    Message message = Message::make_non_owning((rd_kafka_message_t*)msg);
    const auto& callback = handle->get_configuration().get_delivery_report_callback();
    if (callback) {
        callback(*handle, message);
    }
}

template <typename CONSUMER>
void offset_commit_callback_proxy(rd_kafka_t*, rd_kafka_resp_err_t err,
                                  rd_kafka_topic_partition_list_t *offsets, void *opaque) {
    CONSUMER* handle = static_cast<CONSUMER*>(opaque);
    TopicPartitionList list = offsets ? convert(offsets) : TopicPartitionList{};
    const auto& callback = handle->get_configuration().get_offset_commit_callback();
    if (callback) {
        callback(*handle, err, list);
    }
}

template <typename HANDLE_BASE>
void error_callback_proxy(rd_kafka_t*, int err, const char *reason, void *opaque) {
    HANDLE_BASE* handle = static_cast<HANDLE_BASE*>(opaque);
    const auto& callback = handle->get_configuration().get_error_callback();
    if (callback) {
        callback(*handle, err, reason);
    }
}

template <typename HANDLE_BASE>
void throttle_callback_proxy(rd_kafka_t*, const char* broker_name,
                              int32_t broker_id, int throttle_time_ms, void *opaque) {
    HANDLE_BASE* handle = static_cast<HANDLE_BASE*>(opaque);
    const auto& callback = handle->get_configuration().get_throttle_callback();
    if (callback) {
        callback(*handle, broker_name, broker_id, milliseconds(throttle_time_ms));
    }
}

template <typename HANDLE_BASE>
void log_callback_proxy(const rd_kafka_t* h, int level,
                        const char* facility, const char* message) {
    HANDLE_BASE* handle = static_cast<HANDLE_BASE*>(rd_kafka_opaque(h));
    const auto& callback = handle->get_configuration().get_log_callback();
    if (callback) {
        callback(*handle, level, facility, message);
    }
}

template <typename HANDLE_BASE>
int stats_callback_proxy(rd_kafka_t*, char *json, size_t json_len, void *opaque) {
    HANDLE_BASE* handle = static_cast<HANDLE_BASE*>(opaque);
    const auto& callback = handle->get_configuration().get_stats_callback();
    if (callback) {
        callback(*handle, string(json, json + json_len));
    }
    return 0;
}

template <typename HANDLE_BASE>
int socket_callback_proxy(int domain, int type, int protocol, void* opaque) {
    HANDLE_BASE* handle = static_cast<HANDLE_BASE*>(opaque);
    const auto& callback = handle->get_configuration().get_socket_callback();
    if (callback) {
        return callback(domain, type, protocol);
    }
    else {
        return -1;
    }
}

// HandleConfig

template <typename Traits>
HandleConfig<Traits>::HandleConfig()
: handle_(make_handle(rd_kafka_conf_new()))  {
}

template <typename Traits>
HandleConfig<Traits>::HandleConfig(const vector<ConfigurationOption>& options)
: HandleConfig() {
    set(options);
}


template <typename Traits>
HandleConfig<Traits>::HandleConfig(const std::initializer_list<ConfigurationOption>& options)
: HandleConfig() {
    set(options);
}

template <typename Traits>
HandleConfig<Traits>& HandleConfig<Traits>::set(const string& name, const string& value) {
    if (!ConfigurationCache::instance()->validate_configuration_option(name, traits_type::scope)) {
        throw ConfigException(name, "invalid scope");
    }
    char error_buffer[512];
    rd_kafka_conf_res_t result;
    result = rd_kafka_conf_set(handle_.get(), name.data(), value.data(), error_buffer,
                               sizeof(error_buffer));
    if (result != RD_KAFKA_CONF_OK) {
        throw ConfigException(name, error_buffer);
    }
    return *this;
}

template <typename Traits>
template <typename T, typename>
HandleConfig<Traits>& HandleConfig<Traits>::set_delivery_report_callback(DeliveryReportCallback callback) {
    delivery_report_callback_ = move(callback);
    rd_kafka_conf_set_dr_msg_cb(handle_.get(), &delivery_report_callback_proxy<ProducerHandle<traits_type>>);
    return *this;
}

template <typename Traits>
template <typename T, typename>
HandleConfig<Traits>& HandleConfig<Traits>::set_offset_commit_callback(OffsetCommitCallback callback) {
    offset_commit_callback_ = move(callback);
    rd_kafka_conf_set_offset_commit_cb(handle_.get(), &offset_commit_callback_proxy<ConsumerHandle<traits_type>>);
    return *this;
}

template <typename Traits>
HandleConfig<Traits>& HandleConfig<Traits>::set_error_callback(ErrorCallback callback) {
    error_callback_ = move(callback);
    rd_kafka_conf_set_error_cb(handle_.get(), &error_callback_proxy<handle_base_type>);
    return *this;
}

template <typename Traits>
HandleConfig<Traits>& HandleConfig<Traits>::set_throttle_callback(ThrottleCallback callback) {
    throttle_callback_ = move(callback);
    rd_kafka_conf_set_throttle_cb(handle_.get(), &throttle_callback_proxy<handle_base_type>);
    return *this;
}

template <typename Traits>
HandleConfig<Traits>& HandleConfig<Traits>::set_log_callback(LogCallback callback) {
    log_callback_ = move(callback);
    rd_kafka_conf_set_log_cb(handle_.get(), &log_callback_proxy<handle_base_type>);
    return *this;
}

template <typename Traits>
HandleConfig<Traits>& HandleConfig<Traits>::set_stats_callback(StatsCallback callback) {
    stats_callback_ = move(callback);
    rd_kafka_conf_set_stats_cb(handle_.get(), &stats_callback_proxy<handle_base_type>);
    return *this;
}

template <typename Traits>
HandleConfig<Traits>& HandleConfig<Traits>::set_socket_callback(SocketCallback callback) {
    socket_callback_ = move(callback);
    rd_kafka_conf_set_socket_cb(handle_.get(), &socket_callback_proxy<handle_base_type>);
    return *this;
}

template <typename Traits>
HandleConfig<Traits>&
HandleConfig<Traits>::set_default_topic_configuration(typename Traits::topic_config_type config) {
    default_topic_config_ = std::move(config);
    return *this;
}

template <typename Traits>
bool HandleConfig<Traits>::has_property(const string& name) const {
    size_t size = 0;
    return rd_kafka_conf_get(handle_.get(), name.data(), nullptr, &size) == RD_KAFKA_CONF_OK;
}

template <typename Traits>
rd_kafka_conf_t* HandleConfig<Traits>::get_handle() const {
    return handle_.get();
}

template <typename Traits>
string HandleConfig<Traits>::get(const string& name) const {
    size_t size = 0;
    auto result = rd_kafka_conf_get(handle_.get(), name.data(), nullptr, &size);
    if (result != RD_KAFKA_CONF_OK) {
        throw ConfigOptionNotFound(name);
    }
    vector<char> buffer(size);
    rd_kafka_conf_get(handle_.get(), name.data(), buffer.data(), &size);
    return string(buffer.data());
}

template <typename Traits>
map<string, string> HandleConfig<Traits>::get_all() const {
    size_t count = 0;
    const char** all = rd_kafka_conf_dump(handle_.get(), &count);
    map<string, string> output = base_type::parse_dump(all, count);
    rd_kafka_conf_dump_free(all, count);
    return output;
}

template <typename Traits>
template <typename T, typename>
const typename HandleConfig<Traits>::DeliveryReportCallback&
HandleConfig<Traits>::get_delivery_report_callback() const {
    return delivery_report_callback_;
}

template <typename Traits>
template <typename T, typename>
const typename HandleConfig<Traits>::OffsetCommitCallback&
HandleConfig<Traits>::get_offset_commit_callback() const {
    return offset_commit_callback_;
}

template <typename Traits>
const typename HandleConfig<Traits>::ErrorCallback&
HandleConfig<Traits>::get_error_callback() const {
    return error_callback_;
}

template <typename Traits>
const typename HandleConfig<Traits>::ThrottleCallback&
HandleConfig<Traits>::get_throttle_callback() const {
    return throttle_callback_;
}

template <typename Traits>
const typename HandleConfig<Traits>::LogCallback&
HandleConfig<Traits>::get_log_callback() const {
    return log_callback_;
}

template <typename Traits>
const typename HandleConfig<Traits>::StatsCallback&
HandleConfig<Traits>::get_stats_callback() const {
    return stats_callback_;
}

template <typename Traits>
const typename HandleConfig<Traits>::SocketCallback&
HandleConfig<Traits>::get_socket_callback() const {
    return socket_callback_;
}

template <typename Traits>
const optional<typename Traits::topic_config_type>&
HandleConfig<Traits>::get_default_topic_configuration() const {
    return default_topic_config_;
}

template <typename Traits>
optional<typename Traits::topic_config_type>&
HandleConfig<Traits>::get_default_topic_configuration() {
    return default_topic_config_;
}

template <typename Traits>
typename HandleConfig<Traits>::HandlePtr
HandleConfig<Traits>::make_handle(rd_kafka_conf_t* ptr) {
    return HandlePtr(ptr, &rd_kafka_conf_destroy, &rd_kafka_conf_dup);
}     

} // cppkafka
