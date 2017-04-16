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

#include "kafka_handle_base.h"
#include "metadata.h"
#include "exceptions.h"
#include "topic.h"
#include "topic_partition_list.h"

using std::string;
using std::vector;
using std::move;
using std::make_tuple;
using std::lock_guard;
using std::mutex;
using std::exception;
using std::chrono::milliseconds;

namespace cppkafka {

const milliseconds KafkaHandleBase::DEFAULT_TIMEOUT{1000};

KafkaHandleBase::KafkaHandleBase(Configuration config) 
: handle_(nullptr, nullptr), timeout_ms_(DEFAULT_TIMEOUT), config_(move(config)) {
    auto& maybe_config = config_.get_default_topic_configuration();
    if (maybe_config) {
        maybe_config->set_as_opaque();
        auto conf_handle = rd_kafka_topic_conf_dup(maybe_config->get_handle());
        rd_kafka_conf_set_default_topic_conf(config_.get_handle(), conf_handle);
    }
}

void KafkaHandleBase::pause_partitions(const TopicPartitionList& topic_partitions) {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_pause_partitions(get_handle(), 
                                                          topic_list_handle.get());
    check_error(error);
}

void KafkaHandleBase::resume_partitions(const TopicPartitionList& topic_partitions) {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_resume_partitions(get_handle(), 
                                                           topic_list_handle.get());
    check_error(error);
}

void KafkaHandleBase::set_timeout(milliseconds timeout) {
    timeout_ms_ = timeout;
}

void KafkaHandleBase::add_brokers(const string& brokers) {
    rd_kafka_brokers_add(handle_.get(), brokers.data());
}

rd_kafka_t* KafkaHandleBase::get_handle() const {
    return handle_.get();
}

Topic KafkaHandleBase::get_topic(const string& name) {
    save_topic_config(name, TopicConfiguration{});
    return get_topic(name, nullptr);
}

Topic KafkaHandleBase::get_topic(const string& name, TopicConfiguration config) {
    auto handle = config.get_handle();
    save_topic_config(name, move(config));
    return get_topic(name, rd_kafka_topic_conf_dup(handle));
}

KafkaHandleBase::OffsetTuple
KafkaHandleBase::query_offsets(const TopicPartition& topic_partition) const {
    int64_t low;
    int64_t high;
    const string& topic = topic_partition.get_topic();
    const int partition = topic_partition.get_partition();
    rd_kafka_resp_err_t result = rd_kafka_query_watermark_offsets(handle_.get(), topic.data(),
                                                                  partition, &low, &high,
                                                                  timeout_ms_.count());
    check_error(result);
    return make_tuple(low, high);
}

Metadata KafkaHandleBase::get_metadata(bool all_topics) const {
    return get_metadata(all_topics, nullptr);
}

TopicMetadata KafkaHandleBase::get_metadata(const Topic& topic) const {
    Metadata md = get_metadata(false, topic.get_handle());
    auto topics = md.get_topics();
    if (topics.empty()) {
        throw Exception("Failed to find metadata for topic");
    }
    return topics.front();
}

#ifdef CPPKAFKA_HAVE_OFFSET_FOR_TIMES
TopicPartitionList
KafkaHandleBase::get_offsets_for_times(const TopicPartitionsTimestampsMap& queries) const {
    TopicPartitionList topic_partitions;
    for (const auto& query : queries) {
        const TopicPartition& topic_partition = query.first;
        topic_partitions.emplace_back(topic_partition.get_topic(), topic_partition.get_partition(),
                                      query.second.count());
    }
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t result = rd_kafka_offsets_for_times(handle_.get(), topic_list_handle.get(),
                                                            timeout_ms_.count());
    check_error(result);
    return convert(topic_list_handle);
}
#endif // CPPKAFKA_HAVE_OFFSET_FOR_TIMES

string KafkaHandleBase::get_name() const {
    return rd_kafka_name(handle_.get());
}

milliseconds KafkaHandleBase::get_timeout() const {
    return timeout_ms_;
}

const Configuration& KafkaHandleBase::get_configuration() const {
    return config_;
}

int KafkaHandleBase::get_out_queue_length() const {
    return rd_kafka_outq_len(handle_.get());
}

void KafkaHandleBase::set_handle(rd_kafka_t* handle) {
    handle_ = HandlePtr(handle, &rd_kafka_destroy);
}

Topic KafkaHandleBase::get_topic(const string& name, rd_kafka_topic_conf_t* conf) {
    rd_kafka_topic_t* topic = rd_kafka_topic_new(get_handle(), name.data(), conf);
    if (!topic) {
        throw HandleException(rd_kafka_errno2err(errno));
    }
    return Topic(topic);
}

Metadata KafkaHandleBase::get_metadata(bool all_topics, rd_kafka_topic_t* topic_ptr) const {
    const rd_kafka_metadata_t* metadata;
    rd_kafka_resp_err_t error = rd_kafka_metadata(get_handle(), !!all_topics, 
                                                  topic_ptr, &metadata, timeout_ms_.count());
    check_error(error);
    return Metadata(metadata);
}

void KafkaHandleBase::save_topic_config(const string& topic_name, TopicConfiguration config) {
    lock_guard<mutex> _(topic_configurations_mutex_);
    auto iter = topic_configurations_.emplace(topic_name, move(config)).first;
    iter->second.set_as_opaque();
}

void KafkaHandleBase::check_error(rd_kafka_resp_err_t error) const {
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw HandleException(error);
    }
}

rd_kafka_conf_t* KafkaHandleBase::get_configuration_handle() {
    return config_.get_handle();
}

} // cppkafka
