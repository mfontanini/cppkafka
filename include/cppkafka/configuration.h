/*
 * Copyright (c) 2017, Matias Fontanini
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

#ifndef CPPKAFKA_CONFIGURATION_H
#define CPPKAFKA_CONFIGURATION_H

#include <memory>
#include <string>
#include <functional>
#include <initializer_list>
#include <chrono>
#include <vector>
#include <boost/optional.hpp>
#include <librdkafka/rdkafka.h>
#include "types.h"
#include "topic_partition_list.h"
#include "topic_configuration.h"
#include "clonable_ptr.h"
#include "configuration_base.h"
#include "macros.h"
#include "configuration_cache.h"
#include "exceptions.h"
#include "message.h"

using std::string;
using std::map;
using std::move;
using std::vector;
using std::initializer_list;
using boost::optional;
using std::chrono::milliseconds;

namespace cppkafka {

class Message;
class Error;
template <typename T> class ProducerHandle;
template <typename T> class ConsumerHandle;
template <typename T> class HandleBase;

/**
 * \brief Represents a global configuration (rd_kafka_conf_t).
 *
 * This wraps an rdkafka configuration handle. It can safely be copied (will use 
 * rd_kafka_conf_dup under the hood) and moved.
 *
 * Some other overloads for HandleConfig::set are given via ConfigBase.
 */
template <typename Traits>
class CPPKAFKA_API HandleConfig : public ConfigBase<HandleConfig<Traits>> {
public:
    using traits_type = Traits;
    using config_type = HandleConfig<traits_type>;
    using topic_config_type = typename traits_type::topic_config_type;
    using handle_base_type = HandleBase<traits_type>;
    using base_type = ConfigBase<config_type>;
    
    using DeliveryReportCallback = std::function<void(ProducerHandle<Traits>& ProduceTraits, const Message&)>;
    using OffsetCommitCallback = std::function<void(ConsumerHandle<Traits>& ConsumerTraits, Error,
                                                    const TopicPartitionList& topic_partitions)>;
    using ErrorCallback = std::function<void(handle_base_type& handle, int error,
                                             const std::string& reason)>;
    using ThrottleCallback = std::function<void(handle_base_type& handle,
                                                const std::string& broker_name,
                                                int32_t broker_id,
                                                std::chrono::milliseconds throttle_time)>;
    using LogCallback = std::function<void(handle_base_type& handle, int level,
                                           const std::string& facility,
                                           const std::string& message)>;
    using StatsCallback = std::function<void(handle_base_type& handle, const std::string& json)>;
    using SocketCallback = std::function<int(int domain, int type, int protocol)>;

    using base_type::set;
    using base_type::get;

    /**
     * Default constructs a HandleConfig object
     */
    HandleConfig();

    /**
     * Constructs a HandleConfig object using a list of options
     */
    HandleConfig(const std::vector<ConfigurationOption>& options);
    HandleConfig(const std::initializer_list<ConfigurationOption>& options);

    /**
     * \brief Sets an attribute.
     *
     * This will call rd_kafka_conf_set under the hood.
     *
     * \param name The name of the attribute
     * \param value The value of the attribute
     */
    config_type& set(const std::string& name, const std::string& value) override;

    /**
     * Sets the delivery report callback (invokes rd_kafka_conf_set_dr_msg_cb)
     */
    template <typename T = traits_type, typename = std::enable_if_t<has_producer_traits<config_type>::value>>
    config_type& set_delivery_report_callback(DeliveryReportCallback callback);

    /**
     * Sets the offset commit callback (invokes rd_kafka_conf_set_offset_commit_cb)
     */
    template <typename T = traits_type, typename = std::enable_if_t<has_consumer_traits<config_type>::value>>
    config_type& set_offset_commit_callback(OffsetCommitCallback callback);

    /** 
     * Sets the error callback (invokes rd_kafka_conf_set_error_cb)
     */
    config_type& set_error_callback(ErrorCallback callback);

    /** 
     * Sets the throttle callback (invokes rd_kafka_conf_set_throttle_cb)
     */
    config_type& set_throttle_callback(ThrottleCallback callback);

    /** 
     * Sets the log callback (invokes rd_kafka_conf_set_log_cb)
     */
    config_type& set_log_callback(LogCallback callback);

    /** 
     * Sets the stats callback (invokes rd_kafka_conf_set_stats_cb)
     */
    config_type& set_stats_callback(StatsCallback callback);

    /** 
     * Sets the socket callback (invokes rd_kafka_conf_set_socket_cb)
     */
    config_type& set_socket_callback(SocketCallback callback);

    /** 
     * Sets the default topic configuration
     */
    config_type& set_default_topic_configuration(topic_config_type config);

    /**
     * Returns true if the given property name has been set
     */
    bool has_property(const std::string& name) const;

    /** 
     * Gets the rdkafka configuration handle
     */
    rd_kafka_conf_t* get_handle() const;

    /**
     * Gets an option value
     *
     * \throws ConfigOptionNotFound if the option is not present
     */
    std::string get(const std::string& name) const override;

    /**
     * Gets all options, including default values which are set by rdkafka
     */
    std::map<std::string, std::string> get_all() const;

    /**
     * Gets the delivery report callback
     */
    template <typename T = traits_type, typename = std::enable_if_t<has_producer_traits<config_type>::value>>
    const DeliveryReportCallback& get_delivery_report_callback() const;

    /**
     * Gets the offset commit callback
     */
    template <typename T = traits_type, typename = std::enable_if_t<has_consumer_traits<config_type>::value>>
    const OffsetCommitCallback& get_offset_commit_callback() const;

    /**
     * Gets the error callback
     */
    const ErrorCallback& get_error_callback() const;

    /**
     * Gets the throttle callback
     */
    const ThrottleCallback& get_throttle_callback() const;

    /**
     * Gets the log callback
     */
    const LogCallback& get_log_callback() const;

    /**
     * Gets the stats callback
     */
    const StatsCallback& get_stats_callback() const;

    /**
     * Gets the socket callback
     */
    const SocketCallback& get_socket_callback() const;

    /**
     * Gets the default topic configuration
     */
    const boost::optional<topic_config_type>& get_default_topic_configuration() const;

    /**
     * Gets the default topic configuration
     */
    boost::optional<topic_config_type>& get_default_topic_configuration();
    
private:
    using HandlePtr = ClonablePtr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy),
                                  decltype(&rd_kafka_conf_dup)>;
    
    static HandlePtr make_handle(rd_kafka_conf_t* ptr);

    HandlePtr handle_;
    boost::optional<topic_config_type> default_topic_config_;
    DeliveryReportCallback delivery_report_callback_;
    OffsetCommitCallback offset_commit_callback_;
    ErrorCallback error_callback_;
    ThrottleCallback throttle_callback_;
    LogCallback log_callback_;
    StatsCallback stats_callback_;
    SocketCallback socket_callback_;
};

} // cppkafka

#include "impl/configuration_impl.h"

#endif // CPPKAFKA_CONFIGURATION_H
