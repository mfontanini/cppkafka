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

#ifndef CPPKAFKA_ADMIN_ALTER_CONFIGS_OPERATION_H
#define CPPKAFKA_ADMIN_ALTER_CONFIGS_OPERATION_H

#include <memory>
#include "compound_operation.h"
#include "operation.h"
#include "../configuration_option.h"

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION

namespace cppkafka {
namespace admin {

/**
 * \brief Modifies the configuration for a cluster, broker or topic
 */
class AlterConfigsOperation : public Operation {
public:
    /**
     * The rdkafka type used for this operation's handle
     */
    using HandleType = rd_kafka_ConfigResource_t;

    /**
     * \brief Represents the resource type being modified
     */
    enum class ResourceType {
        Unknown = RD_KAFKA_RESOURCE_UNKNOWN,
        Any = RD_KAFKA_RESOURCE_ANY,
        Topic = RD_KAFKA_RESOURCE_TOPIC,
        Group = RD_KAFKA_RESOURCE_GROUP,
        Broker = RD_KAFKA_RESOURCE_BROKER
    };

    /**
     * \brief Constructs an alter configs operation
     *
     * The name depends on the type being used. For example, when modifying a topic
     * type, this will be the name of the topic to be modifies.
     *
     * See KIP-133 for more information.
     *
     * \param type The type of the resource to be modified
     * \param name The name of the resource to be modified
     */
    AlterConfigsOperation(ResourceType type, const std::string& name);

    /**
     * \brief Sets a config option
     *
     * \param config_option The configuration option to be set
     */
    void set_config(const ConfigurationOption& config_option);

    /**
     * \brief Removes a configuration option.
     *
     * Note that this implies removing it from the server side, not on list
     * of configs added to this alter configs operation via AlterConfigsOperation::set_config
     *
     * \param key The config key to be removed
     */
    void remove_config(const std::string& key);
private:
    using HandlePtr = std::unique_ptr<HandleType,
                                      decltype(&rd_kafka_ConfigResource_destroy)>;

    void do_execute(KafkaHandleBase& kafka_handle,
                    Queue& queue,
                    const OperationOptions* options) override;

    HandlePtr handle_;
};

/**
 * \brief Alters the config for multiple resources
 */
using CompoundAlterConfigsOperation = CompoundOperation<AlterConfigsOperation,
                                                        &rd_kafka_AlterConfigs>;

} // admin
} // cppkafka

#endif // Admin API

#endif // CPPKAFKA_ADMIN_ALTER_CONFIGS_OPERATION_H
