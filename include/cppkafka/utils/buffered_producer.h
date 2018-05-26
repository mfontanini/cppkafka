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

#ifndef CPPKAFKA_BUFFERED_PRODUCER_H
#define CPPKAFKA_BUFFERED_PRODUCER_H

#include <string>
#include <deque>
#include <cstdint>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <mutex>
#include <atomic>
#include <boost/optional.hpp>
#include "../producer.h"
#include "../message.h"

namespace cppkafka {

/**
 * \brief Allows producing messages and waiting for them to be acknowledged by kafka brokers
 *
 * This class allows buffering messages and flushing them synchronously while also allowing
 * to produce them just as you would using the Producer class.
 *
 * When calling either flush or wait_for_acks, the buffered producer will block until all
 * produced messages (either buffered or sent directly) are acknowledged by the kafka brokers.
 *
 * When producing messages, this class will handle cases where the producer's queue is full so it
 * will poll until the production is successful.
 *
 * \remark This class is thread safe
 *
 * \warning
 * Delivery Report Callback: This class makes internal use of this function and will overwrite anything
 * the user has supplied as part of the configuration options. Instead user should call
 * set_produce_success_callback() and set_produce_failure_callback() respectively.
 *
 * Payload Policy: For payload-owning BufferTypes such as std::string or std::vector<char> the default
 * policy is set to Producer::PayloadPolicy::COPY_PAYLOAD. For the specific non-payload owning type
 * cppkafka::Buffer the policy is Producer::PayloadPolicy::PASSTHROUGH_PAYLOAD. In this case, librdkafka
 * shall not make any internal copies of the message and it is the application's responsability to free
 * the messages *after* the ProduceSuccessCallback has reported a successful delivery to avoid memory
 * corruptions.
 */
template <typename BufferType>
class CPPKAFKA_API BufferedProducer {
public:
    /**
     * Concrete builder
     */
    using Builder = ConcreteMessageBuilder<BufferType>;
    
    /**
     * Callback to indicate a message was delivered to the broker
     */
    using ProduceSuccessCallback = std::function<void(const Message&)>;

    /**
     * Callback to indicate a message failed to be produced by the broker
     */
    using ProduceFailureCallback = std::function<bool(const Message&)>;
    
    /**
     * Callback to indicate a message failed to be flushed
     */
    using FlushFailureCallback = std::function<bool(const Builder&, Error error)>;

    /**
     * \brief Constructs a buffered producer using the provided configuration
     *
     * \param config The configuration to be used on the actual Producer object
     */
    BufferedProducer(Configuration config);

    /**
     * \brief Adds a message to the producer's buffer. 
     *
     * The message won't be sent until flush is called.
     *
     * \param builder The builder that contains the message to be added
     */
    void add_message(const MessageBuilder& builder);

    /**
     * \brief Adds a message to the producer's buffer. 
     *
     * The message won't be sent until flush is called.
     *
     * Using this overload, you can avoid copies and construct your builder using the type
     * you are actually using in this buffered producer.
     *
     * \param builder The builder that contains the message to be added
     */
    void add_message(Builder builder);

    /**
     * \brief Produces a message without buffering it
     *
     * The message will still be tracked so that a call to flush or wait_for_acks will actually
     * wait for it to be acknowledged.
     *
     * \param builder The builder that contains the message to be produced
     *
     * \remark This method throws cppkafka::HandleException on failure
     */
    void produce(const MessageBuilder& builder);
    
    /**
     * \brief Produces a message without buffering it
     *
     * The message will still be tracked so that a call to flush or wait_for_acks will actually
     * wait for it to be acknowledged.
     *
     * \param message The message to be produced
     *
     * \remark This method throws cppkafka::HandleException on failure
     */
    void produce(const Message& message);

    /**
     * \brief Flushes the buffered messages.
     *
     * This will send all messages and keep waiting until all of them are acknowledged (this is
     * done by calling wait_for_acks).
     *
     * \remark Although it is possible to call flush from multiple threads concurrently, better
     *         performance is achieved when called from the same thread or when serialized
     *         with respect to other threads.
     */
    void flush();

    /**
     * Waits for produced message's acknowledgements from the brokers
     */
    void wait_for_acks();

    /**
     * Clears any buffered messages
     */
    void clear();
    
    /**
     * \brief Sets the maximum amount of messages to be enqueued in the buffer.
     *
     * After 'max_buffer_size' is reached, flush() will be called automatically.
     *
     * \param size The max size of the internal buffer. Allowed values are:
     *             -1 : Unlimited buffer size. Must be flushed manually (default value)
     *              0 : Don't buffer anything. add_message() behaves like produce()
     *            > 0 : Max number of messages before flush() is called.
     *
     * \remark add_message() will block when 'max_buffer_size' is reached due to flush()
     */
    void set_max_buffer_size(ssize_t max_buffer_size);
    
    /**
     * \brief Return the maximum allowed buffer size.
     *
     * \return The max buffer size. A value of -1 indicates an unbounded buffer.
     */
    ssize_t get_max_buffer_size() const;
    
    /**
     * \brief Get the number of unsent messages in the buffer
     *
     * \return The number of messages
     */
    size_t get_buffer_size() const;
    
    /**
     * \brief Returns the total number of messages ack-ed by the broker since the beginning
     *
     * \return The number of messages
     */
    size_t get_total_messages_acked() const;

    /**
     * Gets the Producer object
     */
    Producer& get_producer();

    /**
     * Gets the Producer object
     */
    const Producer& get_producer() const;

    /**
     * Simple helper to construct a builder object
     */
    Builder make_builder(std::string topic);

    /**
     * \brief Sets the message produce failure callback
     *
     * This will be called when the delivery report callback is executed for a message having
     * an error. The callback should return true if the message should be re-sent, otherwise
     * false. Note that if the callback return false, then the message will be discarded.
     *
     * \param callback The callback to be set
     *
     * \remark It is *highly* recommended to set this callback as your message may be produced
     *         indefinitely if there's a remote error.
     *
     * \warning Do not call any method on the BufferedProducer while inside this callback.
     */
    void set_produce_failure_callback(ProduceFailureCallback callback);
    
    /**
     * \brief Sets the successful delivery callback
     *
     * The user can use this function to cleanup any application-owned message buffers.
     *
     * \param callback The callback to be set
     */
    void set_produce_success_callback(ProduceSuccessCallback callback);
    
    /**
     * \brief Sets the local message produce failure callback
     *
     * This callback will be called when local message production fails during a flush() operation.
     * Failure errors are typically payload too large, unknown topic or unknown partition.
     * Note that if the callback returns false, the message will be dropped from the buffer,
     * otherwise it will be re-enqueued for later retry.
     *
     * \param callback
     *
     * \warning Do not call any method on the BufferedProducer while inside this callback
     */
    void set_flush_failure_callback(FlushFailureCallback callback);
    
private:
    using QueueType = std::deque<Builder>;
    enum class MessagePriority { Low, High };

    template <typename BuilderType>
    void do_add_message(BuilderType&& builder, MessagePriority priority, bool do_flush);
    template <typename MessageType>
    void produce_message(const MessageType& message);
    Configuration prepare_configuration(Configuration config);
    void on_delivery_report(const Message& message);
    
    // Members
    Producer producer_;
    QueueType messages_;
    mutable std::mutex mutex_;
    ProduceSuccessCallback produce_success_callback_;
    ProduceFailureCallback produce_failure_callback_;
    FlushFailureCallback flush_failure_callback_;
    ssize_t max_buffer_size_{-1};
    std::atomic_ulong expected_acks_{0};
    std::atomic_ullong total_messages_acked_{0};
};

template <typename BufferType>
Producer::PayloadPolicy get_default_payload_policy() {
    return Producer::PayloadPolicy::COPY_PAYLOAD;
}

template <> inline
Producer::PayloadPolicy get_default_payload_policy<Buffer>() {
    return Producer::PayloadPolicy::PASSTHROUGH_PAYLOAD;
}

template <typename BufferType>
BufferedProducer<BufferType>::BufferedProducer(Configuration config)
: producer_(prepare_configuration(std::move(config))) {
    producer_.set_payload_policy(get_default_payload_policy<BufferType>());
}

template <typename BufferType>
void BufferedProducer<BufferType>::add_message(const MessageBuilder& builder) {
    do_add_message(builder, MessagePriority::Low, true);
}

template <typename BufferType>
void BufferedProducer<BufferType>::add_message(Builder builder) {
    do_add_message(move(builder), MessagePriority::Low, true);
}

template <typename BufferType>
void BufferedProducer<BufferType>::produce(const MessageBuilder& builder) {
    produce_message(builder);
}

template <typename BufferType>
void BufferedProducer<BufferType>::produce(const Message& message) {
    produce_message(message);
}

template <typename BufferType>
void BufferedProducer<BufferType>::flush() {
    QueueType flush_queue; // flush from temporary queue
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::swap(messages_, flush_queue);
    }
    while (!flush_queue.empty()) {
        try {
            produce_message(flush_queue.front());
        }
        catch (const HandleException& ex) {
            if (flush_failure_callback_ &&
                flush_failure_callback_(flush_queue.front(), ex.get_error())) {
                // retry again later
                do_add_message(std::move(flush_queue.front()), MessagePriority::Low, false);
            }
        }
        flush_queue.pop_front();
    }
    wait_for_acks();
}

template <typename BufferType>
void BufferedProducer<BufferType>::wait_for_acks() {
    while (expected_acks_ > 0) {
        try {
            producer_.flush();
        }
        catch (const HandleException& ex) {
            // If we just hit the timeout, keep going, otherwise re-throw
            if (ex.get_error() == RD_KAFKA_RESP_ERR__TIMED_OUT) {
                continue;
            }
            else {
                throw;
            }
        }
    }
}

template <typename BufferType>
void BufferedProducer<BufferType>::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    QueueType tmp;
    std::swap(tmp, messages_);
}

template <typename BufferType>
void BufferedProducer<BufferType>::set_max_buffer_size(ssize_t max_buffer_size) {
    if (max_buffer_size < -1) {
        throw Exception("Invalid buffer size.");
    }
    max_buffer_size_ = max_buffer_size;
}

template <typename BufferType>
ssize_t BufferedProducer<BufferType>::get_max_buffer_size() const {
    return max_buffer_size_;
}

template <typename BufferType>
size_t BufferedProducer<BufferType>::get_buffer_size() const {
    return messages_.size();
}

template <typename BufferType>
template <typename BuilderType>
void BufferedProducer<BufferType>::do_add_message(BuilderType&& builder,
                                                  MessagePriority priority,
                                                  bool do_flush) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (priority == MessagePriority::High) {
            messages_.emplace_front(std::move(builder));
        }
        else {
            messages_.emplace_back(std::move(builder));
        }
    }
    if (do_flush && (max_buffer_size_ >= 0) && (max_buffer_size_ <= (ssize_t)messages_.size())) {
        flush();
    }
}

template <typename BufferType>
Producer& BufferedProducer<BufferType>::get_producer() {
    return producer_;
}

template <typename BufferType>
const Producer& BufferedProducer<BufferType>::get_producer() const {
    return producer_;
}

template <typename BufferType>
size_t BufferedProducer<BufferType>::get_total_messages_acked() const {
    return total_messages_acked_;
}

template <typename BufferType>
typename BufferedProducer<BufferType>::Builder
BufferedProducer<BufferType>::make_builder(std::string topic) {
    return Builder(std::move(topic));
}

template <typename BufferType>
void BufferedProducer<BufferType>::set_produce_failure_callback(ProduceFailureCallback callback) {
    produce_failure_callback_ = std::move(callback);
}

template <typename BufferType>
void BufferedProducer<BufferType>::set_produce_success_callback(ProduceSuccessCallback callback) {
    produce_success_callback_ = std::move(callback);
}

template <typename BufferType>
void BufferedProducer<BufferType>::set_flush_failure_callback(FlushFailureCallback callback) {
    flush_failure_callback_ = std::move(callback);
}

template <typename BufferType>
template <typename MessageType>
void BufferedProducer<BufferType>::produce_message(const MessageType& message) {
    while (true) {
        try {
            producer_.produce(message);
            // Sent successfully
            ++expected_acks_;
            break;
        }
        catch (const HandleException& ex) {
            if (ex.get_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                // If the output queue is full, then just poll
                producer_.poll();
            }
            else {
                throw;
            }
        }
    }
}

template <typename BufferType>
Configuration BufferedProducer<BufferType>::prepare_configuration(Configuration config) {
    using std::placeholders::_2;
    auto callback = std::bind(&BufferedProducer<BufferType>::on_delivery_report, this, _2);
    config.set_delivery_report_callback(std::move(callback));
    return config;
}

template <typename BufferType>
void BufferedProducer<BufferType>::on_delivery_report(const Message& message) {
    // Decrement the expected acks
    --expected_acks_;
    assert(expected_acks_ != (unsigned long)-1); // Prevent underflow
    
    // We should produce this message again if it has an error and we either don't have a
    // produce failure callback or we have one but it returns true
    bool should_produce = message.get_error() &&
                          (!produce_failure_callback_ || produce_failure_callback_(message));
    if (should_produce) {
        // Re-enqueue for later retransmission with higher priority (i.e. front of the queue)
        do_add_message(Builder(message), MessagePriority::High, false);
    }
    else {
        // Successful delivery
        if (produce_success_callback_) {
            produce_success_callback_(message);
        }
        // Increment the total successful transmissions
        ++total_messages_acked_;
    }
}

} // cppkafka

#endif // CPPKAFKA_BUFFERED_PRODUCER_H
