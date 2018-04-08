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

#ifndef CPPKAFKA_TOPIC_CONFIGURATION_H
#define CPPKAFKA_TOPIC_CONFIGURATION_H

#include <string>
#include <functional>
#include <initializer_list>
#include <librdkafka/rdkafka.h>
#include <vector>
#include "clonable_ptr.h"
#include "configuration_base.h"
#include "macros.h"
#include "configuration_cache.h"
#include "exceptions.h"
#include "topic.h"
#include "buffer.h"

using std::string;
using std::map;
using std::vector;
using std::initializer_list;

namespace cppkafka {

class Topic;
class Buffer;

/**
 * \brief Represents the topic configuration
 *
 * ConfigBase provides some extra overloads for set
 */
template <typename Traits>
class CPPKAFKA_API TopicConfig : public ConfigBase<TopicConfig<Traits>> {
public:
    using traits_type = Traits;
    using config_type = typename traits_type::config_type;
    using topic_config_type = TopicConfig<traits_type>;
    using base_type = ConfigBase<topic_config_type>;
    
    /**
     * \brief Partitioner callback
     *
     * This has the same requirements as rdkafka's partitioner calback:
     *   - *Must not* call any rd_kafka_*() functions except:
     *       rd_kafka_topic_partition_available(). This is done via Topic::is_partition_available
     *   - *Must not* block or execute for prolonged periods of time.
     *   - *Must* return a value between 0 and partition_count-1, or the
     *     special RD_KAFKA_PARTITION_UA value if partitioning
     *     could not be performed.
     */
    using PartitionerCallback = std::function<int32_t(const Topic&, const Buffer& key,
                                                      int32_t partition_count)>;
    
    using base_type::set;
    using base_type::get;

    /**
     * Default constructs a topic configuration object
     */
    TopicConfig();

    /**
     * Constructs a TopicConfig object using a list of options
     */
    TopicConfig(const std::vector<ConfigurationOption>& options);
    TopicConfig(const std::initializer_list<ConfigurationOption>& options);

    /**
     * Sets an option
     *
     * \param name The name of the option
     * \param value The value of the option
     */
    topic_config_type& set(const std::string& name, const std::string& value) override;

    /**
     * \brief Sets the partitioner callback
     *
     * This translates into a call to rd_kafka_topic_conf_set_partitioner_cb
     */
    template <typename T = traits_type, typename = std::enable_if_t<has_producer_traits<topic_config_type>::value>>
    topic_config_type& set_partitioner_callback(PartitionerCallback callback);

    /**
     * \brief Sets the "this" pointer as the opaque pointer for this handle
     *
     * This method will be called by consumers/producers when the topic configuration object
     * has been put in a persistent memory location. Users of cppkafka do not need to use this.
     */
    topic_config_type& set_as_opaque();

    /** 
     * Gets the partitioner callback
     */
    template <typename T = traits_type, typename = std::enable_if_t<has_producer_traits<topic_config_type>::value>>
    const PartitionerCallback& get_partitioner_callback() const;

    /**
     * Returns true iff the given property name has been set
     */
    bool has_property(const std::string& name) const;

    /**
     * Gets an option's value
     *
     * \param name The option's name
     */
    std::string get(const std::string& name) const override;

    /**
     * Gets all options, including default values which are set by rdkafka
     */
    std::map<std::string, std::string> get_all() const;

    /**
     * Gets the rdkafka handle
     */
    rd_kafka_topic_conf_t* get_handle() const;
    
private:
    using HandlePtr = ClonablePtr<rd_kafka_topic_conf_t,
                                  decltype(&rd_kafka_topic_conf_destroy),
                                  decltype(&rd_kafka_topic_conf_dup)>;
    
    static HandlePtr make_handle(rd_kafka_topic_conf_t* ptr);

    HandlePtr handle_;
    PartitionerCallback partitioner_callback_;
};

} // cppkafka

#include "impl/topic_configuration_impl.h"

#endif // CPPKAFKA_TOPIC_CONFIGURATION_H
