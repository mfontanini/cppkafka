/*
 * Copyright (c) 2018, Matias Fontanini
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

#include "admin/create_topic_operation.h"

using std::string;
using std::vector;

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {

CreateTopicOperation::CreateTopicOperation(const string& name,
                                           unsigned partitions,
                                           unsigned replication_factor)
: handle_(nullptr, nullptr) {
    init(name, partitions, static_cast<int>(replication_factor));
}

CreateTopicOperation::CreateTopicOperation(const string& name,
                                           unsigned partitions)
: handle_(nullptr, nullptr) {
    init(name, partitions, -1);
}

void CreateTopicOperation::set_replica_assignment(int partition,
                                                  const vector<int32_t>& broker_ids) {
    char error_buffer[512] = { 0 };
    const Error result = rd_kafka_NewTopic_set_replica_assignment(
        handle_.get(),
        partition,
        const_cast<int32_t*>(broker_ids.data()),
        broker_ids.size(),
        error_buffer,
        sizeof(error_buffer)
    );
    if (result) {
        throw AdminOperationException(error_buffer);
    }
}

void CreateTopicOperation::set_config(const ConfigurationOption& config_option) {
    const Error result = rd_kafka_NewTopic_set_config(
        handle_.get(),
        config_option.get_key().data(),
        config_option.get_value().data()
    );
    if (result) {
        throw AdminOperationException(result.to_string());
    }
}

CreateTopicOperation::HandleType* CreateTopicOperation::get_handle() const {
    return handle_.get();
}

void CreateTopicOperation::do_execute(KafkaHandleBase& kafka_handle,
                                      Queue& queue,
                                      const OperationOptions* options) {
    rd_kafka_NewTopic_t* new_topic_handle = handle_.get();
    rd_kafka_CreateTopics(
        kafka_handle.get_handle(),
        &new_topic_handle,
        1 /*number of topics*/,
        options ? options->get_handle() : nullptr,
        queue.get_handle()
    );
}

void CreateTopicOperation::init(const string& topic,
                                unsigned partitions,
                                int replication_factor) {
    char error_buffer[512] = { 0 };
    auto rdkafka_handle = rd_kafka_NewTopic_new(
        topic.data(),
        static_cast<int>(partitions),
        replication_factor,
        error_buffer,
        sizeof(error_buffer)
    );
    if (!rdkafka_handle) {
        throw AdminOperationException(error_buffer);
    }
    handle_ = HandlePtr(rdkafka_handle, rd_kafka_NewTopic_destroy);
}

} // admin
} // cppkafka

#endif // Admin API
