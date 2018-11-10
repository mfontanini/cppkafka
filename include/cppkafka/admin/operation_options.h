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

#ifndef CPPKAFKA_ADMIN_OPERATION_OPTIONS_H
#define CPPKAFKA_ADMIN_OPERATION_OPTIONS_H

#include <chrono>
#include <cstdint>
#include <memory>
#include <librdkafka/rdkafka.h>
#include "../error.h"
#include "../kafka_handle_base.h"
#include "../macros.h"

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {

class CPPKAFKA_API OperationOptions {
public:
    /**
     * \brief Constructs an instance of OperationOptions
     *
     * Note that this instance is tied to the kafka handle and will fail if 
     * it's used on an operation that's applied on a different handle.
     *
     * \param kafka_handle The kafka handle to create this operation options in
     * \param type The type of operation this applies to
     */
    OperationOptions(KafkaHandleBase& kafka_handle, rd_kafka_admin_op_t type);

    /**
     * \brief Sets the overall request timeout
     *
     * This calls rd_kafka_AdminOptions_set_request_timeout under the hood
     * using the provided timeout.
     *
     * \param timeout The timeout to be set for the request
     */
    void set_request_timeout(std::chrono::milliseconds timeout);

    /**
     * \brief Sets the overall operation timeout
     *
     * This calls rd_kafka_AdminOptions_set_operation_timeout under the hood
     * using the provided timeout.
     *
     * \param timeout The timeout to be set for the request
     */
    void set_operation_timeout(std::chrono::milliseconds timeout);

    /**
     * \brief Indicates whether the operation should only be validated
     *
     * This calls rd_kafka_AdminOptions_set_validate_only under the hood
     * using the provided value.
     *
     * \param validate Whether to only validate the request
     */
    void set_validate_only(bool validate);

    /**
     * \brief Override what broker the Admin request will be sent to.
     *
     * This calls rd_kafka_AdminOptions_set_broker under the hood
     * using the provided broker id.
     *
     * \param broker_id The broker id to be set for the request
     */
    void set_operation_timeout(uint32_t broker_id);

    /**
     * \brief Gets the internal rd_kafka_AdminOptions_t pointer
     */
    rd_kafka_AdminOptions_t* get_handle() const;
private:
    using HandlePtr = std::unique_ptr<rd_kafka_AdminOptions_t,
                                      decltype(&rd_kafka_AdminOptions_destroy)>;

    HandlePtr handle_;
    // Kept only for validation when the OperationOptions is used
    const KafkaHandleBase* kafka_handle_;
};

} // admin
} // cppkafka

#endif // Admin API
#endif // CPPKAFKA_ADMIN_OPERATION_OPTIONS_H
