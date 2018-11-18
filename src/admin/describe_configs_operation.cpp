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

#include "admin/describe_configs_operation.h"
#include "exceptions.h"

using std::string;

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {

DescribeConfigsOperation::DescribeConfigsOperation(ResourceType type, const string& name)
: handle_(nullptr, nullptr) {
    auto ptr = rd_kafka_ConfigResource_new(static_cast<rd_kafka_ResourceType_t>(type),
                                           name.data());
    if (!ptr) {
        throw AdminOperationException("Failed to create describe config handle");
    }
    handle_ = HandlePtr(ptr, rd_kafka_ConfigResource_destroy);
}

void DescribeConfigsOperation::add_config(const string& key) {
    const Error result = rd_kafka_ConfigResource_set_config(handle_.get(), key.data(), nullptr);
    if (!result) {
        throw AdminOperationException(result.to_string());
    }
}

void DescribeConfigsOperation::do_execute(KafkaHandleBase& kafka_handle,
                                          Queue& queue,
                                          const OperationOptions* options) {
    run_operation(kafka_handle, queue, options, handle_.get(), &rd_kafka_DescribeConfigs);
}

} // admin
} // cppkafka

#endif // Admin API
