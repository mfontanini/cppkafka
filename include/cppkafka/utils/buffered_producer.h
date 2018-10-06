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
#include <future>
#include <boost/optional.hpp>
#include "../producer.h"
#include "../detail/callback_invoker.h"
#include "../message_internal.h"

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
 * \remark This class is thread safe.
 *
 * \remark Releasing buffers: For high-performance applications preferring a zero-copy approach
 * (using PayloadPolicy::PASSTHROUGH_PAYLOAD - see warning below) it is very important to know when
 * to safely release owned message buffers. One way is to perform individual cleanup when
 * ProduceSuccessCallback is called. If the application produces messages in batches or has a
 * bursty behavior another way is to check when flush operations have fully completed with
 * get_buffer_size()==0 && get_flushes_in_progress()==0. Note that get_pending_acks()==0
 * is not always a guarantee as there is very small window when flush() starts where
 * get_buffer_size()==0 && get_pending_acks()==0 but messages have not yet been sent to the
 * remote broker. For applications producing messages w/o buffering, get_pending_acks()==0
 * is sufficient.
 *
 * \warning Delivery Report Callback: This class makes internal use of this function and will
 * overwrite anything the user has supplied as part of the configuration options. Instead user
 * should call set_produce_success_callback() and set_produce_failure_callback() respectively.
 *
 * \warning Payload Policy: For payload-owning BufferTypes such as std::string or std::vector<char>
 * the default policy is set to Producer::PayloadPolicy::COPY_PAYLOAD. For the specific non-payload owning type
 * cppkafka::Buffer the policy is Producer::PayloadPolicy::PASSTHROUGH_PAYLOAD. In this case, librdkafka
 * shall not make any internal copies of the message and it is the application's responsability to free
 * the messages *after* the ProduceSuccessCallback has reported a successful delivery to avoid memory
 * corruptions.
 */
template <typename BufferType>
class CPPKAFKA_API BufferedProducer {
public:
    enum class FlushMethod { Sync,    ///< Empty the buffer and wait for acks from the broker
                             Async }; ///< Empty the buffer and don't wait for acks
    /**
     * Concrete builder
     */
    using Builder = ConcreteMessageBuilder<BufferType>;
    
    /**
     * Callback to indicate a message was delivered to the broker
     */
    using ProduceSuccessCallback = std::function<void(const Message&)>;

    /**
     * Callback to indicate a message failed to be produced by the broker.
     *
     * The returned bool indicates whether the BufferedProducer should try to produce
     * the message again after each failure.
     */
    using ProduceFailureCallback = std::function<bool(const Message&)>;
    
    /**
     * Callback to indicate a message failed to be flushed
     */
    using FlushFailureCallback = std::function<bool(const MessageBuilder&, Error error)>;

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
     * \brief Produces a message asynchronously without buffering it
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
     * \brief Produces a message synchronously without buffering it
     *
     * In case of failure, the message will be replayed until 'max_number_retries' is reached
     * or until the user ProduceFailureCallback returns false.
     *
     * \param builder The builder that contains the message to be produced
     *
     * \remark This method throws cppkafka::HandleException on failure
     */
    void sync_produce(const MessageBuilder& builder);
    
    /**
     * \brief Produces a message asynchronously without buffering it
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
     * \brief Flushes all buffered messages and returns immediately.
     *
     * Similar to flush, it will send all messages but will not wait for acks to complete.
     */
    void async_flush();

    /**
     * \brief Flushes the buffered messages.
     *
     * This will send all messages and keep waiting until all of them are acknowledged (this is
     * done by calling wait_for_acks).
     *
     * \param preserve_order If set to True, each message in the queue will be flushed only when the previous
     *                       message ack is received. This may result in performance degradation as messages
     *                       are sent one at a time. This calls sync_produce() on each message in the buffer.
     *                       If set to False, all messages are flushed in one batch before waiting for acks,
     *                       however message reordering may occur if librdkafka setting 'messages.sent.max.retries > 0'.
     *
     * \remark Although it is possible to call flush from multiple threads concurrently, better
     *         performance is achieved when called from the same thread or when serialized
     *         with respect to other threads.
     */
    void flush(bool preserve_order = false);
    
    /**
     * \brief Flushes the buffered messages and waits up to 'timeout'
     *
     * \param timeout The maximum time to wait until all acks are received
     *
     * \param preserve_order True to preserve message ordering, False otherwise. See flush above for more details.
     *
     * \return True if the operation completes and all acks have been received.
     */
    bool flush(std::chrono::milliseconds timeout, bool preserve_order = false);

    /**
     * Waits for produced message's acknowledgements from the brokers
     */
    void wait_for_acks();
    
    /**
     * Waits for produced message's acknowledgements from the brokers up to 'timeout'.
     *
     * \return True if the operation completes and all acks have been received.
     */
    bool wait_for_acks(std::chrono::milliseconds timeout);

    /**
     * Clears any buffered messages
     */
    void clear();
    
    /**
     * \brief Get the number of messages in the buffer
     *
     * \return The number of messages
     */
    size_t get_buffer_size() const;
    
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
     * \brief Sets the method used to flush the internal buffer when 'max_buffer_size' is reached.
     *        Default is 'Sync'
     *
     * \param method The method
     */
    void set_flush_method(FlushMethod method);
    
    /**
     * \brief Gets the method used to flush the internal buffer.
     *
     * \return The method
     */
    FlushMethod get_flush_method() const;
    
    /**
     * \brief Get the number of messages not yet acked by the broker
     *
     * \return The number of messages
     */
    size_t get_pending_acks() const;
    
    /**
     * \brief Get the total number of messages successfully produced since the beginning
     *
     * \return The number of messages
     */
    size_t get_total_messages_produced() const;
    
    /**
     * \brief Get the total number of messages dropped since the beginning
     *
     * \return The number of messages
     */
    size_t get_total_messages_dropped() const;
    
    /**
     * \brief Get the total outstanding flush operations in progress
     *
     * Since flush can be called from multiple threads concurrently, this counter indicates
     * how many operations are curretnly in progress.
     *
     * \return The number of outstanding flush operations.
     */
    size_t get_flushes_in_progress() const;
    
    /**
     * \brief Sets the maximum number of retries per message until giving up
     *
     * Default is 5
     */
    void set_max_number_retries(size_t max_number_retries);
    
    /**
     * \brief Gets the max number of retries
     *
     * \return The number of retries
     */
    size_t get_max_number_retries() const;

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
    
    struct TestParameters {
        bool force_delivery_error_;
        bool force_produce_error_;
    };
protected:
    //For testing purposes only
#ifdef KAFKA_TEST_INSTANCE
    void set_test_parameters(TestParameters *test_params) {
        test_params_ = test_params;
    }
    TestParameters* get_test_parameters() {
        return test_params_;
    }
#else
    TestParameters* get_test_parameters() {
        return nullptr;
    }
#endif
    
private:
    using QueueType = std::deque<Builder>;
    enum class MessagePriority { Low, High };
    enum class SenderType { Sync, Async };
    
    template <typename T>
    struct CounterGuard{
        CounterGuard(std::atomic<T>& counter) : counter_(counter) { ++counter_; }
        ~CounterGuard() { --counter_; }
        std::atomic<T>& counter_;
    };
    
    struct Tracker : public Internal {
        Tracker(SenderType sender, size_t num_retries)
            : sender_(sender), num_retries_(num_retries)
        {}
        std::future<bool> get_new_future() {
            should_retry_ = std::promise<bool>(); //reset shared data
            return should_retry_.get_future(); //issue new future
        }
        SenderType sender_;
        std::promise<bool> should_retry_;
        size_t num_retries_;
    };
    using TrackerPtr = std::shared_ptr<Tracker>;
    
    // Returns existing tracker or creates new one
    template <typename BuilderType>
    TrackerPtr add_tracker(SenderType sender, BuilderType& builder) {
        if (has_internal_data_) {
            if (!builder.internal()) {
                // Add message tracker only if it hasn't been added before
                builder.internal(std::make_shared<Tracker>(sender, max_number_retries_));
            }
            return std::static_pointer_cast<Tracker>(builder.internal());
        }
        return nullptr;
    }
    template <typename BuilderType>
    void do_add_message(BuilderType&& builder, MessagePriority priority, bool do_flush);
    template <typename BuilderType>
    void produce_message(BuilderType&& builder);
    Configuration prepare_configuration(Configuration config);
    void on_delivery_report(const Message& message);
    template <typename BuilderType>
    void async_produce(BuilderType&& message, bool throw_on_error);
    
    // Members
    Producer producer_;
    QueueType messages_;
    mutable std::mutex mutex_;
    ProduceSuccessCallback produce_success_callback_;
    ProduceFailureCallback produce_failure_callback_;
    FlushFailureCallback flush_failure_callback_;
    ssize_t max_buffer_size_{-1};
    FlushMethod flush_method_{FlushMethod::Sync};
    std::atomic<size_t> pending_acks_{0};
    std::atomic<size_t> flushes_in_progress_{0};
    std::atomic<size_t> total_messages_produced_{0};
    std::atomic<size_t> total_messages_dropped_{0};
    int max_number_retries_{0};
    bool has_internal_data_{false};
#ifdef KAFKA_TEST_INSTANCE
    TestParameters* test_params_;
#endif
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
#ifdef KAFKA_TEST_INSTANCE
    test_params_ = nullptr;
#endif
}

template <typename BufferType>
void BufferedProducer<BufferType>::add_message(const MessageBuilder& builder) {
    add_message(Builder(builder)); //make ConcreteBuilder
}

template <typename BufferType>
void BufferedProducer<BufferType>::add_message(Builder builder) {
    add_tracker(SenderType::Async, builder);
    do_add_message(move(builder), MessagePriority::Low, true);
}

template <typename BufferType>
void BufferedProducer<BufferType>::produce(const MessageBuilder& builder) {
    if (has_internal_data_) {
        MessageBuilder builder_clone(builder.clone());
        add_tracker(SenderType::Async, builder_clone);
        async_produce(builder_clone, true);
    }
    else {
        async_produce(builder, true);
    }
}

template <typename BufferType>
void BufferedProducer<BufferType>::sync_produce(const MessageBuilder& builder) {
    if (has_internal_data_) {
        MessageBuilder builder_clone(builder.clone());
        TrackerPtr tracker = add_tracker(SenderType::Sync, builder_clone);
        // produce until we succeed or we reach max retry limit
        std::future<bool> should_retry;
        do {
            should_retry = tracker->get_new_future();
            produce_message(builder_clone);
            wait_for_acks();
        }
        while (should_retry.get());
    }
    else {
        // produce once
        produce_message(builder);
        wait_for_acks();
    }
}

template <typename BufferType>
void BufferedProducer<BufferType>::produce(const Message& message) {
    async_produce(MessageBuilder(message), true);
}

template <typename BufferType>
void BufferedProducer<BufferType>::async_flush() {
    CounterGuard<size_t> counter_guard(flushes_in_progress_);
    QueueType flush_queue; // flush from temporary queue
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::swap(messages_, flush_queue);
    }
    while (!flush_queue.empty()) {
        async_produce(std::move(flush_queue.front()), false);
        flush_queue.pop_front();
    }
}

template <typename BufferType>
void BufferedProducer<BufferType>::flush(bool preserve_order) {
    if (preserve_order) {
        CounterGuard<size_t> counter_guard(flushes_in_progress_);
        QueueType flush_queue; // flush from temporary queue
        {
            std::lock_guard<std::mutex> lock(mutex_);
            std::swap(messages_, flush_queue);
        }
        while (!flush_queue.empty()) {
            sync_produce(flush_queue.front());
            flush_queue.pop_front();
        }
    }
    else {
        async_flush();
        wait_for_acks();
    }
}

template <typename BufferType>
bool BufferedProducer<BufferType>::flush(std::chrono::milliseconds timeout,
                                         bool preserve_order) {
    if (preserve_order) {
        CounterGuard<size_t> counter_guard(flushes_in_progress_);
        QueueType flush_queue; // flush from temporary queue
        {
            std::lock_guard<std::mutex> lock(mutex_);
            std::swap(messages_, flush_queue);
        }
        auto start_time = std::chrono::high_resolution_clock::now();
        while (!flush_queue.empty() &&
               (std::chrono::duration_cast<std::chrono::milliseconds>
                   (std::chrono::high_resolution_clock::now() - start_time) < timeout)) {
            sync_produce(flush_queue.front());
            flush_queue.pop_front();
        }
    }
    else {
        async_flush();
        return wait_for_acks(timeout);
    }
}

template <typename BufferType>
void BufferedProducer<BufferType>::wait_for_acks() {
    while (pending_acks_ > 0) {
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
bool BufferedProducer<BufferType>::wait_for_acks(std::chrono::milliseconds timeout) {
    auto remaining = timeout;
    auto start_time = std::chrono::high_resolution_clock::now();
    while ((pending_acks_ > 0) && (remaining.count() > 0)) {
        try {
            producer_.flush(remaining);
        }
        catch (const HandleException& ex) {
            // If we just hit the timeout, keep going, otherwise re-throw
            if (ex.get_error() == RD_KAFKA_RESP_ERR__TIMED_OUT) {
                // There is no time remaining
                return (pending_acks_ == 0);
            }
            else {
                throw;
            }
        }
        // calculate remaining time
        remaining = timeout - std::chrono::duration_cast<std::chrono::milliseconds>
            (std::chrono::high_resolution_clock::now() - start_time);
    }
    return (pending_acks_ == 0);
}

template <typename BufferType>
void BufferedProducer<BufferType>::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    QueueType tmp;
    std::swap(tmp, messages_);
}

template <typename BufferType>
size_t BufferedProducer<BufferType>::get_buffer_size() const {
    return messages_.size();
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
void BufferedProducer<BufferType>::set_flush_method(FlushMethod method) {
    flush_method_ = method;
}

template <typename BufferType>
typename BufferedProducer<BufferType>::FlushMethod
BufferedProducer<BufferType>::get_flush_method() const {
    return flush_method_;
}

template <typename BufferType>
template <typename BuilderType>
void BufferedProducer<BufferType>::do_add_message(BuilderType&& builder,
                                                  MessagePriority priority,
                                                  bool do_flush) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (priority == MessagePriority::High) {
            messages_.emplace_front(std::forward<BuilderType>(builder));
        }
        else {
            messages_.emplace_back(std::forward<BuilderType>(builder));
        }
    }
    if (do_flush && (max_buffer_size_ >= 0) && (max_buffer_size_ <= (ssize_t)messages_.size())) {
        if (flush_method_ == FlushMethod::Sync) {
            flush();
        }
        else {
            async_flush();
        }
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
size_t BufferedProducer<BufferType>::get_pending_acks() const {
    return pending_acks_;
}

template <typename BufferType>
size_t BufferedProducer<BufferType>::get_total_messages_produced() const {
    return total_messages_produced_;
}

template <typename BufferType>
size_t BufferedProducer<BufferType>::get_total_messages_dropped() const {
    return total_messages_dropped_;
}

template <typename BufferType>
size_t BufferedProducer<BufferType>::get_flushes_in_progress() const {
    return flushes_in_progress_;
}

template <typename BufferType>
void BufferedProducer<BufferType>::set_max_number_retries(size_t max_number_retries) {
    if (!has_internal_data_ && (max_number_retries > 0)) {
        has_internal_data_ = true; //enable once
    }
    max_number_retries_ = max_number_retries;
}

template <typename BufferType>
size_t BufferedProducer<BufferType>::get_max_number_retries() const {
    return max_number_retries_;
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
template <typename BuilderType>
void BufferedProducer<BufferType>::produce_message(BuilderType&& builder) {
    using builder_type = typename std::decay<BuilderType>::type;
    while (true) {
        try {
            MessageInternalGuard<builder_type> internal_guard(const_cast<builder_type&>(builder));
            producer_.produce(builder);
            internal_guard.release();
            // Sent successfully
            ++pending_acks_;
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
template <typename BuilderType>
void BufferedProducer<BufferType>::async_produce(BuilderType&& builder, bool throw_on_error) {
    try {
        TestParameters* test_params = get_test_parameters();
        if (test_params && test_params->force_produce_error_) {
            throw HandleException(Error(RD_KAFKA_RESP_ERR_UNKNOWN));
        }
        produce_message(builder);
    }
    catch (const HandleException& ex) {
        // If we have a flush failure callback and it returns true, we retry producing this message later
        CallbackInvoker<FlushFailureCallback> callback("flush failure", flush_failure_callback_, &producer_);
        if (!callback || callback(builder, ex.get_error())) {
            TrackerPtr tracker = std::static_pointer_cast<Tracker>(builder.internal());
            if (tracker && tracker->num_retries_ > 0) {
                --tracker->num_retries_;
                do_add_message(std::forward<BuilderType>(builder), MessagePriority::High, false);
                return;
            }
        }
        ++total_messages_dropped_;
        if (throw_on_error) {
            throw;
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
    //Get tracker data
    TestParameters* test_params = get_test_parameters();
    TrackerPtr tracker = has_internal_data_ ?
        std::static_pointer_cast<Tracker>(MessageInternal::load(const_cast<Message&>(message))->get_internal()) : nullptr;
    bool should_retry = false;
    if (message.get_error() || (test_params && test_params->force_delivery_error_)) {
        // We should produce this message again if we don't have a produce failure callback
        // or we have one but it returns true
        CallbackInvoker<ProduceFailureCallback> callback("produce failure", produce_failure_callback_, &producer_);
        if (!callback || callback(message)) {
            // Check if we have reached the maximum retry limit
            if (tracker && tracker->num_retries_ > 0) {
                --tracker->num_retries_;
                if (tracker->sender_ == SenderType::Async) {
                    // Re-enqueue for later retransmission with higher priority (i.e. front of the queue)
                    do_add_message(Builder(message), MessagePriority::High, false);
                }
                should_retry = true;
            }
            else {
                ++total_messages_dropped_;
            }
        }
        else {
            ++total_messages_dropped_;
        }
    }
    else {
        // Successful delivery
        CallbackInvoker<ProduceSuccessCallback>("delivery success", produce_success_callback_, &producer_)(message);
        // Increment the total successful transmissions
        ++total_messages_produced_;
    }
    // Signal producers
    if (tracker) {
        tracker->should_retry_.set_value(should_retry);
    }
    // Decrement the expected acks and check to prevent underflow
    if (pending_acks_ > 0) {
        --pending_acks_;
    }
}

} // cppkafka

#endif // CPPKAFKA_BUFFERED_PRODUCER_H
