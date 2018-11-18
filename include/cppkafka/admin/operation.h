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

#ifndef CPPKAFKA_ADMIN_OPERATION_H
#define CPPKAFKA_ADMIN_OPERATION_H

#include "operation_options.h"
#include "../kafka_handle_base.h"
#include "../macros.h"

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {

/**
 * \brief Represents an admin operation to be applied on a kafka handle
 */
class CPPKAFKA_API Operation {
public:
    virtual ~Operation() = default;

    /**
     * \brief Execute this operation on the given handle
     *
     * The operation will be asynchronously executed on the given KafkaHandleBase and
     * the result of it will be written into the provided Queue.
     *
     * \param kafka_handle The kafka handle to run this operation on
     * \param queue The queue in which to post the result of this operation
     * \param options The options to be used for this operation
     */
    void execute(KafkaHandleBase& kafka_handle, Queue& queue, const OperationOptions& options);

    /**
     * \brief Execute this operation on the given handle
     *
     * The operation will be asynchronously executed on the given KafkaHandleBase and
     * the result of it will be written into the provided Queue.
     *
     * This will use the default options for this operation.
     *
     * \param kafka_handle The kafka handle to run this operation on
     * \param queue The queue in which to post the result of this operation
     */
    void execute(KafkaHandleBase& kafka_handle, Queue& queue);
protected:
    template <typename T>
    using RunOperationType = void(*)(rd_kafka_t*,
                                  T**,
                                  size_t,
                                  const rd_kafka_AdminOptions_t*,
                                  rd_kafka_queue_t*);

    virtual void do_execute(KafkaHandleBase& kafka_handle,
                            Queue& queue,
                            const OperationOptions* options) = 0;

    template <typename T>
    void run_operation(KafkaHandleBase& kafka_handle,
                       Queue& queue,
                       const OperationOptions* options,
                       T* operation_handle,
                       RunOperationType<T> functor) {
        functor(
            kafka_handle.get_handle(),
            &operation_handle,
            1 /*number of operations*/,
            options ? options->get_handle() : nullptr,
            queue.get_handle()
        );
    }
};

} // admin
} // cppkafka

#endif // Admin API
#endif // CPPKAFKA_ADMIN_OPERATION_H
