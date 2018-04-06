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

template <typename TOPIC_CONFIG>
int32_t partitioner_callback_proxy(const rd_kafka_topic_t* handle, const void *key_ptr,
                                   size_t key_size, int32_t partition_count,
                                   void* topic_opaque, void* message_opaque) {
    const TOPIC_CONFIG* config = static_cast<TOPIC_CONFIG*>(topic_opaque);
    const auto& callback = config->get_partitioner_callback();
    if (callback) {
        Topic topic = Topic::make_non_owning(const_cast<rd_kafka_topic_t*>(handle));
        Buffer key(static_cast<const char*>(key_ptr), key_size);
        return callback(topic, key, partition_count);
    }
    else {
        return rd_kafka_msg_partitioner_consistent_random(handle, key_ptr, key_size, 
                                                          partition_count, topic_opaque,
                                                          message_opaque);
    }
}

template <typename Traits>
TopicConfig<Traits>::TopicConfig()
: handle_(make_handle(rd_kafka_topic_conf_new())) {

}

template <typename Traits>
TopicConfig<Traits>::TopicConfig(const vector<ConfigurationOption>& options)
: TopicConfig() {
    set(options);
}

template <typename Traits>
TopicConfig<Traits>::TopicConfig(const std::initializer_list<ConfigurationOption>& options)
: TopicConfig() {
    set(options);
}

template <typename Traits>
TopicConfig<Traits>& TopicConfig<Traits>::set(const string& name, const string& value) {
    if (!ConfigurationCache::instance()->validate_configuration_option(name, traits_type::topic_scope)) {
        throw ConfigException(name, "invalid scope");
    }
    char error_buffer[512];
    rd_kafka_conf_res_t result;
    result = rd_kafka_topic_conf_set(handle_.get(), name.data(), value.data(), error_buffer,
                                     sizeof(error_buffer));
    if (result != RD_KAFKA_CONF_OK) {
        throw ConfigException(name, error_buffer);
    }
    return *this;
}

template <typename Traits>
template <typename T, typename>
TopicConfig<Traits>& TopicConfig<Traits>::set_partitioner_callback(PartitionerCallback callback) {
    partitioner_callback_ = move(callback);
    rd_kafka_topic_conf_set_partitioner_cb(handle_.get(), &partitioner_callback_proxy<topic_config_type>);
    return *this;
}

template <typename Traits>
TopicConfig<Traits>& TopicConfig<Traits>::set_as_opaque() {
    rd_kafka_topic_conf_set_opaque(handle_.get(), this);
    return *this;
}

template <typename Traits>
template <typename T, typename>
const typename TopicConfig<Traits>::PartitionerCallback&
TopicConfig<Traits>::get_partitioner_callback() const {
    return partitioner_callback_;
}

template <typename Traits>
bool TopicConfig<Traits>::has_property(const string& name) const {
    size_t size = 0;
    return rd_kafka_topic_conf_get(handle_.get(), name.data(), nullptr, &size) == RD_KAFKA_CONF_OK;
}

template <typename Traits>
string TopicConfig<Traits>::get(const string& name) const {
    size_t size = 0;
    auto result = rd_kafka_topic_conf_get(handle_.get(), name.data(), nullptr, &size);
    if (result != RD_KAFKA_CONF_OK) {
        throw ConfigOptionNotFound(name);
    }
    vector<char> buffer(size);
    rd_kafka_topic_conf_get(handle_.get(), name.data(), buffer.data(), &size);
    return string(buffer.data());
}

template <typename Traits>
map<string, string> TopicConfig<Traits>::get_all() const {
    size_t count = 0;
    const char** all = rd_kafka_topic_conf_dump(handle_.get(), &count);
    map<string, string> output = base_type::parse_dump(all, count);
    rd_kafka_conf_dump_free(all, count);
    return output;
}

template <typename Traits>
rd_kafka_topic_conf_t* TopicConfig<Traits>::get_handle() const {
    return handle_.get();
}

template <typename Traits>
typename TopicConfig<Traits>::HandlePtr
TopicConfig<Traits>::make_handle(rd_kafka_topic_conf_t* ptr) {
    return HandlePtr(ptr, &rd_kafka_topic_conf_destroy, &rd_kafka_topic_conf_dup);
}     

} // cppkafka
