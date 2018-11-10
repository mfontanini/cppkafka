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

#include <vector>
#include "operation.h"

#ifndef CPPKAFKA_ADMIN_COMPOUND_OPERATION_H
#define CPPKAFKA_ADMIN_COMPOUND_OPERATION_H

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {
namespace detail {

template <typename Operation>
using CompoundOperationFunction = void(*)(rd_kafka_t*,
                                          typename Operation::HandleType**,
                                          size_t,
                                          const rd_kafka_AdminOptions_t*,
                                          rd_kafka_queue_t*);

} // details

/**
 * \brief Bundles several operations of the same type and executes them in a single call
 *
 * This can bundle operations (say CreateTopicOperation) and execute them on a single call
 * rather than having to execute each of them individually.
 *
 * This class should typically be used via the type alias defined in other files, 
 * e.g. CreateTopicsOperation
 */
template <typename OperationType, detail::CompoundOperationFunction<OperationType> Function>
class CompoundOperation : public Operation {
public:
    /**
     * \brief Default constructs a  compound operation
     */
    CompoundOperation() = default;

    /**
     * \brief Constructs a compound operation using a list of sub operations
     *
     * \param operations The list of sub operations to use
     */
    CompoundOperation(std::vector<OperationType> operations);

    /**
     * \brief Adds an sub operation to this compound operation
     *
     * \param operation The sub operation to be added
     */
    void add_operation(OperationType operation);
private:
    void do_execute(KafkaHandleBase& kafka_handle,
                    Queue& queue,
                    const OperationOptions* options) override;

    std::vector<OperationType> operations_;
};

template <typename T, detail::CompoundOperationFunction<T> E>
CompoundOperation<T, E>::CompoundOperation(std::vector<T> operations)
: operations_(std::move(operations)) {
}

template <typename T, detail::CompoundOperationFunction<T> E>
void CompoundOperation<T, E>::add_operation(T operation) {
    operations_.emplace_back(std::move(operation));
}

template <typename T, detail::CompoundOperationFunction<T> E>
void CompoundOperation<T, E>::do_execute(KafkaHandleBase& kafka_handle,
                                         Queue& queue,
                                         const OperationOptions* options) {
    std::vector<typename T::HandleType*> handles;
    for (auto& operation : operations_) {
        handles.emplace_back(operation.get_handle());
    }
    E(
        kafka_handle.get_handle(),
        handles.data(),
        handles.size(),
        options ? options->get_handle() : nullptr,
        queue.get_handle()
    );
}


} // admin
} // cppkafka

#endif // Admin API
#endif // CPPKAFKA_ADMIN_COMPOUND_OPERATION_H
