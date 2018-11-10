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

#ifndef CPPKAFKA_CREATE_TOPIC_OPERATION_H
#define CPPKAFKA_CREATE_TOPIC_OPERATION_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "operation.h"
#include "../configuration_option.h"

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {

class CreateTopicOperation : public Operation {
public:
    /**
     * \brief Constructs an instance of a CreateTopicOperation
     *
     * Note that calling set_replica_assignment is invalid when using this constructor.
     * Use the other one which doesn't take a replication factor if you want to manually
     * assign the replica assignment to each partition
     *
     * \param name The name of the topic to be created
     * \param partitions The number of partitions to be created for this topic
     * \param replication_factor The topic's replication factor
     */
    CreateTopicOperation(const std::string& name,
                         unsigned partitions,
                         unsigned replication_factor);

    /**
     * \brief Constructs an instance of a CreateTopicOperation
     *
     * When calling this constructor, the user *must* call set_replica_assignment for each
     * of the partitions in ascending order. This is an API restriction imposed by librdkafka.
     *
     * \param name The name of the topic to be created
     * \param partitions The number of partitions to be created for this topic
     */
    CreateTopicOperation(const std::string& name,
                         unsigned partitions);

    /**
     * \brief Sets the replica assignment for a particular partition
     *
     * This calls rd_kafka_NewTopic_set_replica_assignment under the hood
     *
     * This method can only be called if the constructor called was the one that doesn't take
     * a replication factor.
     *
     * This method must be called for every each partition in this topic (starting from 0)
     * in ascending order. This is an API restriction imposed by librdkafka.
     *
     *
     * \param partition The partition for which to set the replica assignment
     * \param broker_ids The list of broker ids that will replicate this partition
     */
    void set_replica_assignment(int partition, const std::vector<int32_t>& broker_ids);

    /**
     * \brief Sets a server side configuration option
     *
     * \param config_option The configuration option to be set
     */
    void set_config(const ConfigurationOption& config_option);

private:
    void do_execute(KafkaHandleBase& kafka_handle,
                    Queue& queue,
                    const OperationOptions* options) override;

    using HandlePtr = std::unique_ptr<rd_kafka_NewTopic_t,
                                      decltype(&rd_kafka_NewTopic_destroy)>;

    void init(const std::string& topic, unsigned partitions, int replication_factor);

    HandlePtr handle_;
};

} // admin
} // cppkafka

#endif // Admin API
#endif // CPPKAFKA_CREATE_TOPIC_OPERATION_H
