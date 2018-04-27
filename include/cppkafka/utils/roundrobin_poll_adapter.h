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

#include <list>
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
 * group consumer queue (one per consumer), these batches end up being queued in sequence or arrival.
 * For instance, a topic with 4 partitions (each containing N messages) will end up being queued as
 * N1|N2|N3|N4 in the consumer queue. This means that for the Consumer to process messages from the
 * 4th partition, it needs to consume 3xN messages. The larger the number of partitions, the more
 * starvation occurs. While this behavior is acceptable for some applications, real-time applications
 * sensitive to timing or those where messages must be processed more or less in the same order as
 * they're being produced, the default librdkafka behavior is unacceptable.
 * Fortunately, librdkafka exposes direct access to its partition queues which means that various
 * polling strategies can be implemented to suit needs.
 * This adapter allows fair round-robin polling of all assigned partitions, one message at a time
 * (or one batch at a time if poll_batch() is used). Note that poll_batch() has nothing to do with
 * the internal batching mechanism of librdkafka.
 *
 * Example code on how to use this:
 *
 * \code
 * // Create a consumer
 * Consumer consumer(...);
 *
 * // Optionally set the callbacks. This must be done *BEFORE* creating the adapter
 * consumer.set_assignment_callback(...);
 * consumer.set_revocation_callback(...);
 * consumer.set_rebalance_error_callback(...);
 *
 * // Create the adapter and use it for polling
 * RoundRobinPollAdapter adapter(consumer);
 *
 * // Subscribe *AFTER* the adapter has been created
 * consumer.subscribe({ "my_topic" });
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
     * Each call to poll() will result in another partition being polled. Aside from
     * the partition, this function will also poll the main queue for events. If an
     * event is found, it is immediately returned. As such the main queue has higher
     * priority than the partition queues. Because of this, you
     * need to call poll periodically as a keep alive mechanism, otherwise the broker
     * will think this consumer is down and will trigger a rebalance (if using dynamic
     * subscription).
     * The timeout used on this call will be the one configured via RoundRobinPollAdapter::set_timeout.
     *
     * \return A message. The returned message *might* be empty. It's necessary to check
     * that it's a valid one before using it (see example above).
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
     * Each call to poll() will result in another partition being polled. Aside from
     * the partition, this function will also poll the main queue for events. If a batch of
     * events is found, it is prepended to the returned message list. If after polling the
     * main queue the batch size has reached max_batch_size, it is immediately returned and
     * the partition is no longer polled. Otherwise the partition is polled for the remaining
     * messages up to the max_batch_size limit.
     * Because of this, you need to call poll periodically as a keep alive mechanism,
     * otherwise the broker will think this consumer is down and will trigger a rebalance
     * (if using dynamic subscription).
     *
     * \param max_batch_size The maximum amount of messages expected
     *
     * \return A list of messages
     */
    MessageList poll_batch(size_t max_batch_size);

    /**
     * \brief Polls for a batch of messages depending on the configured PollStrategy
     *
     * Same as the other overload of RoundRobinPollAdapter::poll_batch but the provided
     * timeout will be used instead of the one configured on this Consumer.
     *
     * \param max_batch_size The maximum amount of messages expected
     * \param timeout The timeout for this operation
     *
     * \return A list of messages
     */
    MessageList poll_batch(size_t max_batch_size, std::chrono::milliseconds timeout);
    
private:
    class CircularBuffer {
        using qlist = std::list<Queue>;
        using qiter = qlist::iterator;
    public:
        qlist& ref() { return queues_; }
        Queue& next() {
            if (queues_.empty()) {
                throw QueueException(RD_KAFKA_RESP_ERR__STATE);
            }
            if (++iter_ == queues_.end()) {
                iter_ = queues_.begin();
            }
            return *iter_;
        }
        void rewind() { iter_ = queues_.begin(); }
    private:
        qlist queues_;
        qiter iter_ = queues_.begin();
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
