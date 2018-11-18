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

#ifndef CPPKAFKA_CREATE_PARTITIONS_OPERATION_H
#define CPPKAFKA_CREATE_PARTITIONS_OPERATION_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "operation.h"
#include "compound_operation.h"
#include "../configuration_option.h"

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {

class CreatePartitionsOperation : public Operation {
public:
    /**
     * The rdkafka type used for this operation's handle
     */
    using HandleType = rd_kafka_NewPartitions_t;

    /**
     * \brief Constructs a create partitions operation
     *
     * \param topic The name of the topic to modify
     * \param partitions The desired number of partitions to set on the topic
     */
    CreatePartitionsOperation(std::string topic, size_t partitions);

    /**
     * \brief Sets the replica assignment for a partition
     *
     * This calls rd_kafka_NewPartitions_set_replica_assignment under the hood
     *
     * The replica assignment has to be set for either all partitions or none of them.
     * This method has to be called consecutively for all indexes from 0 to the partitions
     * constructor parameter - 1.
     *
     * Note that the provided parameter is the new partition index rather than the
     * partition itself.
     *
     * \param new_partition_index The index of the new partition to be configured
     * \param broker_ids The list of broker ids that will replicate this partition
     */
    void set_replica_assignment(int new_partition_index, std::vector<int32_t> brokers);
private:
    void do_execute(KafkaHandleBase& kafka_handle,
                    Queue& queue,
                    const OperationOptions* options) override;

    using HandlePtr = std::unique_ptr<HandleType,
                                      decltype(&rd_kafka_NewPartitions_destroy)>;

    HandlePtr handle_;
};

/**
 * \brief Sets the partition count for multiple topics
 */
using CompoundCreatePartitionsOperation = CompoundOperation<CreatePartitionsOperation,
                                                            &rd_kafka_CreatePartitions>;

} // admin
} // cppkafka

#endif // Admin API
#endif // CPPKAFKA_CREATE_PARTITIONS_OPERATION_H
