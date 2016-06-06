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

#ifndef CPPKAFKA_METADATA_H
#define CPPKAFKA_METADATA_H

#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <unordered_set>
#include <librdkafka/rdkafka.h>

namespace cppkafka {

class PartitionMetadata {
public:
    PartitionMetadata(const rd_kafka_metadata_partition& partition);

    uint32_t get_id() const;
    rd_kafka_resp_err_t get_error() const;
    int32_t get_leader() const;
    const std::vector<int32_t>& get_replicas() const;
    const std::vector<int32_t>& get_in_sync_replica_brokers() const;
private:
    int32_t id_;
    rd_kafka_resp_err_t error_;
    int32_t leader_;
    std::vector<int32_t> replicas_;
    std::vector<int32_t> isrs_;
};

class TopicMetadata {
public:
    TopicMetadata(const rd_kafka_metadata_topic& topic);

    const std::string& get_topic() const;
    rd_kafka_resp_err_t get_error() const;
    const std::vector<PartitionMetadata>& get_partitions() const;
private:
    std::string topic_;
    rd_kafka_resp_err_t error_;
    std::vector<PartitionMetadata> partitions_;
};

class BrokerMetadata {
public:
    BrokerMetadata(const rd_kafka_metadata_broker_t& broker);

    const std::string& get_host() const;
    int32_t get_id() const;
    uint16_t get_port() const;
private:
    const std::string host_;
    int32_t id_;
    uint16_t port_;
};

class Metadata {
public:
    Metadata(const rd_kafka_metadata_t* ptr);

    std::vector<BrokerMetadata> get_brokers() const;
    std::vector<TopicMetadata> get_topics() const;
    std::vector<TopicMetadata> get_topics(const std::unordered_set<std::string>& topics) const;
    std::vector<TopicMetadata> get_topics(const std::string& prefix) const;
private:
    using HandlePtr = std::unique_ptr<const rd_kafka_metadata_t, decltype(&rd_kafka_metadata_destroy)>;

    HandlePtr handle_;
};

} // cppkafka

#endif // CPPKAFKA_METADATA_H
