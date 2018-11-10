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

#include "admin/operation_options.h"

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {

OperationOptions::OperationOptions(KafkaHandleBase& kafka_handle, rd_kafka_admin_op_t type)
: handle_(rd_kafka_AdminOptions_new(kafka_handle.get_handle(), type),
          rd_kafka_AdminOptions_destroy),
  kafka_handle_(&kafka_handle) {

}

void OperationOptions::set_request_timeout(std::chrono::milliseconds timeout) {
    char error_bufer[512] = { 0 };
    const Error result = rd_kafka_AdminOptions_set_request_timeout(
        handle_.get(),
        timeout.count(),
        error_bufer,
        sizeof(error_bufer)
    );
    if (result) {
        throw AdminOperationOptionsException(error_bufer);
    }
}

void OperationOptions::set_operation_timeout(std::chrono::milliseconds timeout) {
    char error_bufer[512] = { 0 };
    const Error result = rd_kafka_AdminOptions_set_operation_timeout(
        handle_.get(),
        timeout.count(),
        error_bufer,
        sizeof(error_bufer)
    );
    if (result) {
        throw AdminOperationOptionsException(error_bufer);
    }
}

void OperationOptions::set_validate_only(bool validate) {
    char error_bufer[512] = { 0 };
    const Error result = rd_kafka_AdminOptions_set_validate_only(
        handle_.get(),
        validate,
        error_bufer,
        sizeof(error_bufer)
    );
    if (result) {
        throw AdminOperationOptionsException(error_bufer);
    }
}

void OperationOptions::set_operation_timeout(uint32_t broker_id) {
    char error_bufer[512] = { 0 };
    const Error result = rd_kafka_AdminOptions_set_broker(
        handle_.get(),
        broker_id,
        error_bufer,
        sizeof(error_bufer)
    );
    if (result) {
        throw AdminOperationOptionsException(error_bufer);
    }
}

rd_kafka_AdminOptions_t* OperationOptions::get_handle() const {
    return handle_.get();
}

} // admin
} // cppkafka

#endif
