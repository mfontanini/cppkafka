/*
 * Copyright (c) 2016, Matias Fontanini
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

#ifndef CPPKAFKA_KAFKA_HANDLE_BASE_H
#define CPPKAFKA_KAFKA_HANDLE_BASE_H

#include <string>
#include <memory>
#include <chrono>
#include <unordered_map>
#include <mutex>
#include <tuple>
#include <librdkafka/rdkafka.h>
#include "topic_partition.h"
#include "topic_partition_list.h"
#include "topic_configuration.h"
#include "configuration.h"
#include "macros.h"

namespace cppkafka {

class Topic;
class Metadata;
class TopicMetadata;

/**
 * Base class for kafka consumer/producer
 */
class CPPKAFKA_API KafkaHandleBase {
public:
    using OffsetTuple = std::tuple<int64_t, int64_t>;

    virtual ~KafkaHandleBase() = default;
    KafkaHandleBase(const KafkaHandleBase&) = delete;
    KafkaHandleBase(KafkaHandleBase&&) = delete;
    KafkaHandleBase& operator=(const KafkaHandleBase&) = delete;
    KafkaHandleBase& operator=(KafkaHandleBase&&) = delete;

    /**
     * \brief Pauses consumption/production from the given topic/partition list
     *
     * This translates into a call to rd_kafka_pause_partitions
     *
     * \param topic_partitions The topic/partition list to pause consuming/producing from/to
     */
    void pause_partitions(const TopicPartitionList& topic_partitions);
    
    /**
     * \brief Resumes consumption/production from the given topic/partition list
     *
     * This translates into a call to  rd_kafka_resume_partitions
     *
     * \param topic_partitions The topic/partition list to resume consuming/producing from/to
     */
    void resume_partitions(const TopicPartitionList& topic_partitions);

    /**
     * \brief Sets the timeout for operations that require a timeout
     *
     * This timeout is applied to operations like polling, querying for offsets, etc
     *
     * \param timeout The timeout to be set
     */
    void set_timeout(std::chrono::milliseconds timeout);

    /**
     * \brief Adds one or more brokers to this handle's broker list
     *
     * This calls rd_kafka_brokers_add using the provided broker list.
     *
     * \param brokers The broker list endpoint string
     */
    void add_brokers(const std::string& brokers);

    /**
     * \brief Queries the offset for the given topic/partition
     *
     * This translates into a call to rd_kafka_query_watermark_offsets
     * 
     * \param topic_partition The topic/partition to be queried
     */ 
    OffsetTuple query_offsets(const TopicPartition& topic_partition) const;

    /**
     * Gets the rdkafka handle
     */
    rd_kafka_t* get_handle() const;

    /**
     * \brief Creates a topic handle
     *
     * This translates into a call to rd_kafka_topic_new. This will use the default topic 
     * configuration provided in the Configuration object for this consumer/producer handle, 
     * if any.
     *
     * \param name The name of the topic to be created 
     */
    Topic get_topic(const std::string& name);

    /**
     * \brief Creates a topic handle
     *
     * This translates into a call to rd_kafka_topic_new.
     *
     * \param name The name of the topic to be created 
     * \param config The configuration to be used for the new topic
     */
    Topic get_topic(const std::string& name, TopicConfiguration config);

    /**
     * \brief Gets metadata for brokers, topics, partitions, etc
     *
     * \param all_topics Whether to fetch metadata about all topics or only locally known ones
     *
     * This translates into a call to rd_kafka_metadata
     */
    Metadata get_metadata(bool all_topics = true) const;

    /**
     * \brief Gets general metadata but only fetches metadata for the given topic rather than 
     * all of them 
     *
     * This translates into a call to rd_kafka_metadata
     *
     * \param topic The topic to fetch information for
     */
    TopicMetadata get_metadata(const Topic& topic) const;

    /**
     * Returns the kafka handle name
     */
    std::string get_name() const;

    /**
     * Gets the configured timeout.
     *
     * \sa KafkaHandleBase::set_timeout
     */
    std::chrono::milliseconds get_timeout() const;

    /**
     * Gets the handle's configuration
     */ 
    const Configuration& get_configuration() const;

    /**
     * \brief Gets the length of the out queue 
     *
     * This calls rd_kafka_outq_len
     */
    int get_out_queue_length() const;
protected:
    KafkaHandleBase(Configuration config);

    void set_handle(rd_kafka_t* handle);
    void check_error(rd_kafka_resp_err_t error) const;
    rd_kafka_conf_t* get_configuration_handle();
private:
    static const std::chrono::milliseconds DEFAULT_TIMEOUT;

    using HandlePtr = std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>;
    using TopicConfigurationMap = std::unordered_map<std::string, TopicConfiguration>;

    Topic get_topic(const std::string& name, rd_kafka_topic_conf_t* conf);
    Metadata get_metadata(bool all_topics, rd_kafka_topic_t* topic_ptr) const;
    void save_topic_config(const std::string& topic_name, TopicConfiguration config);

    HandlePtr handle_;
    std::chrono::milliseconds timeout_ms_;
    Configuration config_;
    TopicConfigurationMap topic_configurations_;
    std::mutex topic_configurations_mutex_;
};

} // cppkafka

#endif // CPPKAFKA_KAFKA_HANDLE_BASE_H
