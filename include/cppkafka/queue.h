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

#include <vector>
#include <memory>
#include <boost/optional.hpp>
#include <librdkafka/rdkafka.h>
#include "macros.h"
#include "message.h"

#ifndef CPPKAFKA_QUEUE_H
#define CPPKAFKA_QUEUE_H

namespace cppkafka {
/**
 * \brief Represents a rdkafka queue
 *
 * This is a simple wrapper over a rd_kafka_queue_t*
 */
class CPPKAFKA_API Queue {
public:
    /**
     * \brief Creates a Queue object that doesn't take ownership of the handle
     *
     * \param handle The handle to be used
     */
    static Queue make_non_owning(rd_kafka_queue_t* handle);
    
    /**
     * \brief Constructs an empty queue
     *
     * Note that using any methods except Queue::get_handle on an empty queue is undefined
     * behavior
     */
    Queue();

    /**
     * \brief Constructs a queue using a handle
     *
     * This will take ownership of the handle
     *
     * \param handle The handle to be used
     */
    Queue(rd_kafka_queue_t* handle);

    /**
     * Returns the rdkakfa handle
     */
    rd_kafka_queue_t* get_handle() const;
    
    /**
     * \brief Returns the length of the queue
     *
     * This translates to a call to rd_kafka_queue_length
     */
    size_t get_length() const;
    
    /**
     * \brief Forward to another queue
     *
     * This translates to a call to rd_kafka_queue_forward
     */
    void forward_to_queue(const Queue& forward_queue) const;
    
    /**
     * \brief Disable forwarding to another queue
     *
     * This translates to a call to rd_kafka_queue_forward(NULL)
     */
    void disable_queue_forwarding() const;
    
    /**
     * \brief Sets the timeout for consume operations
     *
     * This timeout is applied when calling consume()
     *
     * \param timeout The timeout to be set
     */
    void set_timeout(std::chrono::milliseconds timeout);
    
    /**
     * Gets the configured timeout.
     *
     * \sa Queue::set_timeout
     */
    std::chrono::milliseconds get_timeout() const;
    
    /**
     * \brief Consume a message from this queue
     *
     * This translates to a call to rd_kafka_consume_queue using the configured timeout for this object
     *
     * \return A message
     */
    Message consume() const;

    /**
     * \brief Consume a message from this queue
     *
     * Same as consume() but the specified timeout will be used instead of the configured one
     *
     * \param timeout The timeout to be used on this call
     *
     * \return A message
     */
    Message consume(std::chrono::milliseconds timeout) const;
    
    /**
     * \brief Consumes a batch of messages from this queue
     *
     * This translates to a call to rd_kafka_consume_batch_queue using the configured timeout for this object
     *
     * \param max_batch_size The max number of messages to consume if available
     *
     * \return A list of messages. Could be empty if there's nothing to consume
     */
    MessageList consume_batch(size_t max_batch_size) const;
    
    /**
     * \brief Consumes a batch of messages from this queue
     *
     * Same as Queue::consume_batch(size_t) but the specified timeout will be used instead of the configured one
     *
     * \param max_batch_size The max number of messages to consume if available
     *
     * \param timeout The timeout to be used on this call
     *
     * \return A list of messages. Could be empty if there's nothing to consume
     */
    MessageList consume_batch(size_t max_batch_size, std::chrono::milliseconds timeout) const;
    
    /**
     * Indicates whether this queue is valid (not null)
     */
    explicit operator bool() const {
        return handle_ != nullptr;
    }
    
private:
    static const std::chrono::milliseconds DEFAULT_TIMEOUT;
    
    using HandlePtr = std::unique_ptr<rd_kafka_queue_t, decltype(&rd_kafka_queue_destroy)>;

    struct NonOwningTag { };

    Queue(rd_kafka_queue_t* handle, NonOwningTag);

    // Members
    HandlePtr                   handle_;
    std::chrono::milliseconds   timeout_ms_;
};

using QueueList = std::vector<Queue>;

} // cppkafka

#endif //CPPKAFKA_QUEUE_H
