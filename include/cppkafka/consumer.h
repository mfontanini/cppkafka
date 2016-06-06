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

#ifndef CPP_KAFKA_CONSUMER_H
#define CPP_KAFKA_CONSUMER_H

#include <vector>
#include <string>
#include <chrono>
#include <functional>
#include "kafka_handle_base.h"
#include "message.h"

namespace cppkafka {

class TopicConfiguration;

class Consumer : public KafkaHandleBase {
public:
    using AssignmentCallback = std::function<void(TopicPartitionList&)>;
    using RevocationCallback = std::function<void(const TopicPartitionList&)>;
    using RebalanceErrorCallback = std::function<void(rd_kafka_resp_err_t)>;

    Consumer(Configuration config);
    Consumer(const Consumer&) = delete;
    Consumer(Consumer&) = delete;
    Consumer& operator=(const Consumer&) = delete;
    Consumer& operator=(Consumer&&) = delete;
    ~Consumer();

    void set_assignment_callback(AssignmentCallback callback);
    void set_revocation_callback(RevocationCallback callback);
    void set_rebalance_error_callback(RebalanceErrorCallback callback);

    void subscribe(const std::vector<std::string>& topics);
    void unsubscribe();

    void assign(const TopicPartitionList& topic_partitions);
    void unassign();
    void close();

    void commit(const Message& msg);
    void async_commit(const Message& msg);
    void commit(const TopicPartitionList& topic_partitions);
    void async_commit(const TopicPartitionList& topic_partitions);

    OffsetTuple get_offsets(const std::string& topic, int partition) const;

    TopicPartitionList get_offsets_committed(const TopicPartitionList& topic_partitions) const;
    TopicPartitionList get_offsets_position(const TopicPartitionList& topic_partitions) const;
    TopicPartitionList get_subscription() const;
    TopicPartitionList get_assignment() const;
    std::string get_member_id() const;

    Message poll();
private:
    static void rebalance_proxy(rd_kafka_t *handle, rd_kafka_resp_err_t error,
                                rd_kafka_topic_partition_list_t *partitions, void *opaque);

    void commit(const Message& msg, bool async);
    void commit(const TopicPartitionList& topic_partitions, bool async);
    void handle_rebalance(rd_kafka_resp_err_t err, TopicPartitionList& topic_partitions);

    AssignmentCallback assignment_callback_;
    RevocationCallback revocation_callback_;
    RebalanceErrorCallback rebalance_error_callback_;
};

} // cppkafka

#endif // CPP_KAFKA_CONSUMER_H
