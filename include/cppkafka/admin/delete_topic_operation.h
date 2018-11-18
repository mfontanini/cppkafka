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

#ifndef CPPKAFKA_ADMIN_DELETE_TOPIC_OPERATION_H
#define CPPKAFKA_ADMIN_DELETE_TOPIC_OPERATION_H

#include <memory>
#include "compound_operation.h"
#include "operation.h"

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {

/**
 * \brief Deletes a topic
 */
class DeleteTopicOperation : public Operation {
public:
    /**
     * The rdkafka type used for this operation's handle
     */
    using HandleType = rd_kafka_DeleteTopic_t;

    /**
     * \brief Constructs an instance given a topic name
     *
     * \param name The name of the topic to be deleted
     */
    DeleteTopicOperation(const std::string& name);

    /**
     * \brief Gets the underlying rdkafka handle
     */
    HandleType* get_handle() const;
private:
    using HandlePtr = std::unique_ptr<rd_kafka_DeleteTopic_t,
                                      decltype(&rd_kafka_DeleteTopic_destroy)>;

    void do_execute(KafkaHandleBase& kafka_handle,
                    Queue& queue,
                    const OperationOptions* options) override;

    HandlePtr handle_;
};

/**
 * \brief Deletes several topics
 */
using CompoundDeleteTopicOperation = CompoundOperation<DeleteTopicOperation,
                                                       &rd_kafka_DeleteTopics>;

} // admin
} // cppkafka

#endif // Admin API
#endif // CPPKAFKA_ADMIN_DELETE_TOPIC_OPERATION_H
