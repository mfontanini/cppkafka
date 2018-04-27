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

#include "utils/roundrobin_poll_adapter.h"

using std::chrono::milliseconds;
using std::make_move_iterator;

namespace cppkafka {

RoundRobinPollAdapter::RoundRobinPollAdapter(Consumer& consumer)
: consumer_(consumer),
  assignment_callback_(consumer.get_assignment_callback()),
  revocation_callback_(consumer.get_revocation_callback()),
  rebalance_error_callback_(consumer.get_rebalance_error_callback()),
  consumer_queue_(consumer.get_consumer_queue()) {
    // take over the assignment callback
    consumer_.set_assignment_callback([this](TopicPartitionList& partitions) {
        on_assignment(partitions);
    });
    // take over the revocation callback
    consumer_.set_revocation_callback([this](const TopicPartitionList& partitions) {
        on_revocation(partitions);
    });
    // take over the rebalance error callback
    consumer_.set_rebalance_error_callback([this](Error error) {
        on_rebalance_error(error);
    });
    // make sure we don't have any active subscriptions
    if (!consumer_.get_subscription().empty()) {
        throw ConsumerException(RD_KAFKA_RESP_ERR__EXISTING_SUBSCRIPTION);
    }
}

RoundRobinPollAdapter::~RoundRobinPollAdapter() {
    restore_forwarding();
    //set the original callbacks
    consumer_.set_assignment_callback(assignment_callback_);
    consumer_.set_revocation_callback(revocation_callback_);
    consumer_.set_rebalance_error_callback(rebalance_error_callback_);
}

void RoundRobinPollAdapter::set_timeout(milliseconds timeout) {
    consumer_.set_timeout(timeout);
}

milliseconds RoundRobinPollAdapter::get_timeout() {
    return consumer_.get_timeout();
}

Message RoundRobinPollAdapter::poll() {
    return poll(consumer_.get_timeout());
}

Message RoundRobinPollAdapter::poll(milliseconds timeout) {
    bool empty_list = partition_queues_.ref().empty();
    // Poll group event queue first
    Message message = consumer_queue_.consume(empty_list ? timeout : milliseconds(0));
    if (message) {
        return message;
    }
    if (!empty_list) {
        //consume the next partition
        message = partition_queues_.next().consume(timeout);
    }
    return message;
}

MessageList RoundRobinPollAdapter::poll_batch(size_t max_batch_size) {
    return poll_batch(max_batch_size, consumer_.get_timeout());
}

MessageList RoundRobinPollAdapter::poll_batch(size_t max_batch_size, milliseconds timeout) {
    bool empty_list = partition_queues_.ref().empty();
    ssize_t remaining_count = max_batch_size;
    // batch from the group event queue first
    MessageList messages = consumer_queue_.consume_batch(remaining_count,
                                                         empty_list ? timeout : milliseconds(0));
    remaining_count -= messages.size();
    if ((remaining_count <= 0) || empty_list) {
        // the entire batch was filled
        return messages;
    }
    // batch from the next partition
    MessageList partition_messages = partition_queues_.next().consume_batch(remaining_count, timeout);
    if (messages.empty()) {
        return partition_messages;
    }
    if (partition_messages.empty()) {
        return messages;
    }
    // concatenate both lists
    messages.reserve(messages.size() + partition_messages.size());
    messages.insert(messages.end(),
                    make_move_iterator(partition_messages.begin()),
                    make_move_iterator(partition_messages.end()));
    return messages;
}

void RoundRobinPollAdapter::on_assignment(TopicPartitionList& partitions) {
    //populate partition queues
    for (const auto& partition : partitions) {
        partition_queues_.ref().push_back(consumer_.get_partition_queue(partition));
    }
    // call original consumer callback if any
    if (assignment_callback_) {
        assignment_callback_(partitions);
    }
}

void RoundRobinPollAdapter::on_revocation(const TopicPartitionList& partitions) {
    // put all partitions queues back to their initial state
    restore_forwarding();
    // empty the circular queue list
    partition_queues_.ref().clear();
    // reset the queue iterator
    partition_queues_.rewind();
    // call original consumer callback if any
    if (revocation_callback_) {
        revocation_callback_(partitions);
    }
}

void RoundRobinPollAdapter::on_rebalance_error(Error error) {
    // call original consumer callback if any
    if (rebalance_error_callback_) {
        rebalance_error_callback_(error);
    }
}

void RoundRobinPollAdapter::restore_forwarding() {
    // forward all partition queues
    for (const auto& queue : partition_queues_.ref()) {
        queue.forward_to_queue(consumer_queue_);
    }
}

} //cppkafka
