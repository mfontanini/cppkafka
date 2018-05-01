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

#ifndef CPPKAFKA_ROUNDROBIN_POLL_ADAPTER_H
#define CPPKAFKA_ROUNDROBIN_POLL_ADAPTER_H

#include <map>
#include <string>
#include "../exceptions.h"
#include "../consumer.h"
#include "../queue.h"

namespace cppkafka {

/**
 * \brief This adapter changes the default polling strategy of the Consumer into a fair round-robin
 *        polling mechanism.
 *
 * The default librdkafka (and cppkafka) poll() and poll_batch() behavior is to consume batches of
 * messages from each partition in turn. For performance reasons, librdkafka pre-fetches batches
 * of messages from the kafka broker (one batch from each partition), and stores them locally in
 * partition queues. Since all the internal partition queues are forwarded by default unto the
 * group consumer queue (one per consumer), these batches end up being polled and consumed in the
 * same sequence order.
 * This adapter allows fair round-robin polling of all assigned partitions, one message at a time
 * (or one batch at a time if poll_batch() is used). Note that poll_batch() has nothing to do with
 * the internal batching mechanism of librdkafka.
 *
 * Example code on how to use this:
 *
 * \code
 * // Create a consumer
 * Consumer consumer(...);
 * consumer.subscribe({ "my_topic" });
 *
 * // Optionally set the callbacks. This must be done *BEFORE* creating the adapter
 * consumer.set_assignment_callback(...);
 * consumer.set_revocation_callback(...);
 * consumer.set_rebalance_error_callback(...);
 *
 * // Create the adapter and use it for polling
 * RoundRobinPollAdapter adapter(consumer);
 *
 * while (true) {
 *     // Poll each partition in turn
 *     Message msg = adapter.poll();
 *     if (msg) {
 *         // process valid message
 *         }
 *     }
 * }
 * \endcode
 *
 * \warning Calling directly poll() or poll_batch() on the Consumer object while using this adapter will
 * lead to undesired results since the RoundRobinPollAdapter modifies the internal queuing mechanism of
 * the Consumer instance it owns.
 */
class RoundRobinPollAdapter
{
public:
    RoundRobinPollAdapter(Consumer& consumer);
    
    ~RoundRobinPollAdapter();
    
    /**
     * \brief Sets the timeout for polling functions
     *
     * This calls Consumer::set_timeout
     *
     * \param timeout The timeout to be set
     */
    void set_timeout(std::chrono::milliseconds timeout);
    
    /**
     * \brief Gets the timeout for polling functions
     *
     * This calls Consumer::get_timeout
     *
     * \return The timeout
     */
    std::chrono::milliseconds get_timeout();
    
    /**
     * \brief Polls all assigned partitions for new messages in round-robin fashion
     *
     * Each call to poll() will first consume from the global event queue and if there are
     * no pending events, will attempt to consume from all partitions until a valid message is found.
     * The timeout used on this call will be the one configured via RoundRobinPollAdapter::set_timeout.
     *
     * \return A message. The returned message *might* be empty. It's necessary to check
     * that it's a valid one before using it (see example above).
     *
     * \remark You need to call poll() or poll_batch() periodically as a keep alive mechanism,
     * otherwise the broker will think this consumer is down and will trigger a rebalance
     * (if using dynamic subscription)
     */
    Message poll();
    
    /**
     * \brief Polls for new messages
     *
     * Same as the other overload of RoundRobinPollAdapter::poll but the provided
     * timeout will be used instead of the one configured on this Consumer.
     *
     * \param timeout The timeout to be used on this call
     */
    Message poll(std::chrono::milliseconds timeout);

    /**
     * \brief Polls all assigned partitions for a batch of new messages in round-robin fashion
     *
     * Each call to poll_batch() will first attempt to consume from the global event queue
     * and if the maximum batch number has not yet been filled, will attempt to fill it by
     * reading the remaining messages from each partition.
     *
     * \param max_batch_size The maximum amount of messages expected
     *
     * \return A list of messages
     *
     * \remark You need to call poll() or poll_batch() periodically as a keep alive mechanism,
     * otherwise the broker will think this consumer is down and will trigger a rebalance
     * (if using dynamic subscription)
     */
    MessageList poll_batch(size_t max_batch_size);

    /**
     * \brief Polls all assigned partitions for a batch of new messages in round-robin fashion
     *
     * Same as the other overload of RoundRobinPollAdapter::poll_batch but the provided
     * timeout will be used instead of the one configured on this Consumer.
     *
     * \param max_batch_size The maximum amount of messages expected
     *
     * \param timeout The timeout for this operation
     *
     * \return A list of messages
     */
    MessageList poll_batch(size_t max_batch_size, std::chrono::milliseconds timeout);
    
private:
    void consume_batch(MessageList& messages, ssize_t& count, std::chrono::milliseconds timeout);
    
    class CircularBuffer {
    public:
        using QueueMap = std::map<TopicPartition, Queue>;
        QueueMap& get_queues() {
            return queues_;
        }
        Queue& get_next_queue() {
            if (queues_.empty()) {
                throw QueueException(RD_KAFKA_RESP_ERR__STATE);
            }
            if (++iter_ == queues_.end()) {
                iter_ = queues_.begin();
            }
            return iter_->second;
        }
        void rewind() { iter_ = queues_.begin(); }
    private:
        QueueMap            queues_;
        QueueMap::iterator  iter_{queues_.begin()};
    };
    
    void on_assignment(TopicPartitionList& partitions);
    void on_revocation(const TopicPartitionList& partitions);
    void on_rebalance_error(Error error);
    void restore_forwarding();
    
    // Members
    Consumer&                           consumer_;
    Consumer::AssignmentCallback        assignment_callback_;
    Consumer::RevocationCallback        revocation_callback_;
    Consumer::RebalanceErrorCallback    rebalance_error_callback_;
    Queue                               consumer_queue_;
    CircularBuffer                      partition_queues_;
};

} //cppkafka

#endif //CPPKAFKA_ROUNDROBIN_POLL_ADAPTER_H
