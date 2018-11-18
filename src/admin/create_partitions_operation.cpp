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

#include "admin/create_partitions_operation.h"
#include "exceptions.h"

using std::string;
using std::vector;

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {

CreatePartitionsOperation::CreatePartitionsOperation(string topic, size_t partitions)
: handle_(nullptr, nullptr) {
    char error_buffer[512] = { 0 };
    auto rdkafka_handle = rd_kafka_NewPartitions_new(
        topic.data(),
        partitions,
        error_buffer,
        sizeof(error_buffer)
    );
    if (!rdkafka_handle) {
        throw AdminOperationException(error_buffer);
    }
    handle_ = HandlePtr(rdkafka_handle, rd_kafka_NewPartitions_destroy);
}

void CreatePartitionsOperation::set_replica_assignment(int new_partition_index,
                                                       vector<int32_t> brokers) {
    char error_buffer[512] = { 0 };
    const Error result = rd_kafka_NewPartitions_set_replica_assignment(
        handle_.get(),
        new_partition_index,
        brokers.data(),
        brokers.size(),
        error_buffer,
        sizeof(error_buffer)
    );
    if (result) {
        throw AdminOperationException(error_buffer);
    }
}

void CreatePartitionsOperation::do_execute(KafkaHandleBase& kafka_handle,
                                           Queue& queue,
                                           const OperationOptions* options) {
    run_operation(kafka_handle, queue, options, handle_.get(), &rd_kafka_CreatePartitions);
}

} // admin
} // cppkafka

#endif // Admin API
