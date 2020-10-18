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
#include <map>
#include <mutex>
#include <atomic>
#include <future>
#include <thread>
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
 * When calling either flush or wait_for_acks/wait_for_current_thread_acks, the buffered producer
 * will block until all produced messages (either buffered or sent directly) are acknowledged
 * by the kafka brokers.
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
 * shall not make any internal copies of the message and it is the application's responsibility to free
 * the messages *after* the ProduceSuccessCallback has reported a successful delivery to avoid memory
 * corruptions.
 */
template <typename BufferType,
          typename Allocator = std::allocator<ConcreteMessageBuilder<BufferType>>>
class CPPKAFKA_API BufferedProducer {
public:
    enum class FlushMethod {
        Sync,    ///< Empty the buffer and wait for acks from the broker.
        Async    ///< Empty the buffer and don't wait for acks.
    };
    enum class QueueFullNotification {
        None,           ///< Don't notify (default).
        OncePerMessage, ///< Notify once per message.
        EachOccurence   ///< Notify on each occurence.
    };
    /**
     * Concrete builder
     */
    using Builder = ConcreteMessageBuilder<BufferType>;
    using QueueType = std::deque<Builder, Allocator>;
    
    /**
     * Callback to indicate a message was delivered to the broker
     */
    using ProduceSuccessCallback = std::function<void(const Message&)>;

    /**
     * Callback to indicate a message failed to be produced by the broker.
     *
     * The returned bool indicates whether the BufferedProducer should try to produce
     * the message again after each failure, subject to the maximum number of retries set. If this callback
     * is not set or returns false or if the number of retries reaches zero, the ProduceTerminationCallback
     * will be called.
     */
    using ProduceFailureCallback = std::function<bool(const Message&)>;
    
    /**
     * Callback to indicate a message failed to be produced by the broker and was dropped.
     *
     * The application can use this callback to track delivery failure of messages similar to the
     * FlushTerminationCallback. If the application is only interested in message dropped events,
     * then ProduceFailureCallback should not be set.
     */
    using ProduceTerminationCallback = std::function<void(const Message&)>;
    
    /**
     * Callback to indicate a message failed to be flushed
     *
     * If this callback returns true, the message will be re-enqueued and flushed again later subject
     * to the maximum number of retries set. If this callback is not set or returns false or if the number of retries
     * reaches zero, the FlushTerminationCallback will be called.
     */
    using FlushFailureCallback = std::function<bool(const MessageBuilder&, Error error)>;
    
    /**
     * Callback to indicate a message was dropped after multiple flush attempts or when the retry count
     * reaches zero.
     *
     * The application can use this callback to track delivery failure of messages similar to the
     * ProduceTerminationCallback. If the application is only interested in message dropped events,
     * then FlushFailureCallback should not be set.
     */
    using FlushTerminationCallback = std::function<void(const MessageBuilder&, Error error)>;
    
    /**
     * Callback to indicate a RD_KAFKA_RESP_ERR__QUEUE_FULL was received when producing.
     *
     * The MessageBuilder instance represents the message which triggered the error. This callback will be called
     * according to the set_queue_full_notification() setting.
     */
    using QueueFullCallback = std::function<void(const MessageBuilder&)>;

    /**
     * \brief Constructs a buffered producer using the provided configuration
     *
     * \param config The configuration to be used on the actual Producer object
     * \param alloc The optionally supplied allocator for the internal message buffer
     */
    BufferedProducer(Configuration config, const Allocator& alloc = Allocator());

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
     * The message will still be tracked so that a call to flush or
     * wait_for_acks/wait_for_current_thread_acks will actually wait for it
     * to be acknowledged.
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
     * \brief Same as sync_produce but waits up to 'timeout' for acks to be received.
     *
     * If retries are enabled, the timeout will limit the amount of time to wait
     * before all retries are completed.
     *
     * \returns True if succeeded, false otherwise. If retries are enabled, false
     *          indicates there are still retries left.
     */
    bool sync_produce(const MessageBuilder& builder, std::chrono::milliseconds timeout);

    /**
     * \brief Produces a message asynchronously without buffering it
     *
     * The message will still be tracked so that a call to flush or
     * wait_for_acks/wait_for_current_thread_acks will actually wait for it
     * to be acknowledged.
     *
     * \param message The message to be produced
     *
     * \remark This method throws cppkafka::HandleException on failure
     */
    void produce(const Message& message);
    
    /**
     * \brief Flushes all buffered messages and returns immediately.
     *
     * Similar to flush, it will send all messages but will not wait for acks to complete. However the underlying
     * producer will still be flushed.
     */
    void async_flush();

    /**
     * \brief Flushes the buffered messages.
     *
     * This will send all messages and keep waiting until all of them are acknowledged (this is
     * done by calling wait_for_acks/wait_for_current_thread_acks).
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
     * \brief Waits for produced message's acknowledgements from the brokers
     */
    void wait_for_acks();
    
    /**
     * \brief Waits for acknowledgements from brokers for messages produced
     *        on the current thread only
     */
    void wait_for_current_thread_acks();
    
    /**
     * \brief Waits for produced message's acknowledgements from the brokers up to 'timeout'.
     *
     * \return True if the operation completes and all acks have been received.
     */
    bool wait_for_acks(std::chrono::milliseconds timeout);
    
    /**
     * \brief Waits for acknowledgements from brokers for messages produced
     *        on the current thread only. Times out after 'timeout' milliseconds.
     *
     * \return True if the operation completes and all acks have been received.
     */
    bool wait_for_current_thread_acks(std::chrono::milliseconds timeout);

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
     * \brief Get the number of messages not yet acked by the broker.
     *
     * \return The number of messages
     */
    size_t get_pending_acks() const;
    
    /**
     * \brief Get the number of pending acks for messages produces on the
     *        current thread only.
     *
     * \return The number of messages
     */
    size_t get_current_thread_pending_acks() const;
    
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
     * \brief Sets the maximum number of retries per message until giving up. Default is 5.
     *
     * \remark Is it recommended to set the RdKafka option message.send.max.retries=0
     *         to prevent re-ordering of messages inside RdKafka.
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
     * Set the type of notification when RD_KAFKA_RESP_ERR__QUEUE_FULL is received.
     *
     * This will call the error callback for this producer. By default this is set to QueueFullNotification::None.
     */
    void set_queue_full_notification(QueueFullNotification notification);
    
    /**
     * Get the queue full notification type.
     */
    QueueFullNotification get_queue_full_notification() const;

    /**
     * \brief Sets the message produce failure callback
     *
     * This will be called when the delivery report callback is executed for a message having
     * an error. The callback should return true if the message should be re-sent, otherwise
     * false. Note that if the callback return false, then the message will be discarded.
     *
     * \param callback The callback to be set
     *
     * \warning Do not call any method on the BufferedProducer while inside this callback.
     */
    void set_produce_failure_callback(ProduceFailureCallback callback);
    
    /**
     * \brief Sets the message produce termination callback
     *
     * This will be called when the delivery report callback is executed for a message having
     * an error and after all retries have expired and the message is dropped.
     *
     * \param callback The callback to be set
     *
     * \remark If the application only tracks dropped messages, the set_produce_failure_callback() should not be set.
     *
     * \warning Do not call any method on the BufferedProducer while inside this callback.
     */
    void set_produce_termination_callback(ProduceTerminationCallback callback);
    
    /**
     * \brief Sets the successful delivery callback
     *
     * The user can use this function to cleanup any application-owned message buffers.
     *
     * \param callback The callback to be set
     */
    void set_produce_success_callback(ProduceSuccessCallback callback);
    
    /**
     * \brief Sets the local flush failure callback
     *
     * This callback will be called when local message production fails during a flush() operation.
     * Failure errors are typically payload too large, unknown topic or unknown partition.
     * Note that if the callback returns false, the message will be dropped from the buffer,
     * otherwise it will be re-enqueued for later retry subject to the message retry count.
     *
     * \param callback
     *
     * \warning Do not call any method on the BufferedProducer while inside this callback
     */
    void set_flush_failure_callback(FlushFailureCallback callback);
    
    /**
     * \brief Sets the local flush termination callback
     *
     * This callback will be called when local message production fails during a flush() operation after
     * all previous flush attempts have failed. The message will be dropped after this callback.
     *
     * \param callback
     *
     * \remark If the application only tracks dropped messages, the set_flush_failure_callback() should not be set.
     *
     * \warning Do not call any method on the BufferedProducer while inside this callback
     */
    void set_flush_termination_callback(FlushTerminationCallback callback);
    
    /**
     * \brief Sets the local queue full error callback
     *
     * This callback will be called when local message production fails during a produce() operation according to the
     * set_queue_full_notification() setting.
     *
     * \param callback
     *
     * \warning Do not call any method on the BufferedProducer while inside this callback
     */
    void set_queue_full_callback(QueueFullCallback callback);
    
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
    enum class SenderType { Sync, Async };
    enum class QueueKind { Retry, Produce };
    enum class FlushAction { DontFlush, DoFlush };
    enum class Threads { All, Current };

    // Simple RAII type which increments a counter on construction and
    // decrements it on destruction, meant to be used as reference counting.
    template <typename T>
    struct CounterGuard{
        CounterGuard(std::atomic<T>& counter)
        : counter_(counter) {
            ++counter_;
        }
        ~CounterGuard() { --counter_; }
        std::atomic<T>& counter_;
    };
    
    // If the application enables retry logic, this object is passed
    // as internal (opaque) data with each message, so that it can keep
    // track of each failed attempt. Only a single tracker will be
    // instantiated and it's lifetime will be the same as the message it
    // belongs to.
    struct Tracker : public Internal {
        Tracker(SenderType sender, size_t num_retries)
        : sender_(sender),
          num_retries_(num_retries) {
        }
        // Creates a new promise for synchronizing with the
        // on_delivery_report() callback. For synchronous producers only.
        void prepare_to_retry() {
            if (sender_ == SenderType::Sync) {
                retry_promise_ = std::promise<bool>();
            }
        }
        // Waits for the on_delivery_report() callback and determines if this message
        // should be retried. This call will block until on_delivery_report() executes.
        // For synchronous producers only.
        bool retry_again() {
            if (sender_ == SenderType::Sync) {
                return retry_promise_.get_future().get();
            }
            return false;
        }
        // Signal the synchronous producer if the message should be retried or not.
        // Called from inside on_delivery_report(). For synchronous producers only.
        void should_retry(bool value) const {
            if (sender_ == SenderType::Sync) {
                try {
                    retry_promise_.set_value(value);
                }
                catch (const std::future_error&) {
                    //Promise has already been set once.
                }
            }
        }
        void set_sender_type(SenderType type) {
            sender_ = type;
        }
        SenderType get_sender_type() const {
            return sender_;
        }
        bool has_retries_left() const {
            return num_retries_ > 0;
        }
        void decrement_retries() {
            if (num_retries_ > 0) {
                --num_retries_;
            }
        }
    private:
        SenderType sender_;
        mutable std::promise<bool> retry_promise_;
        size_t num_retries_;
    };
    using TrackerPtr = std::shared_ptr<Tracker>;
    
    // The AckMonitor is responsible for properly counting the
    // outstanding unacknowledged messages for each thread as well
    // as the total acks. Counting acks on a per-thread basis is
    // critical in a multi-threaded producer since we don't want one
    // producer having to wait for all concurrent pending acks. Each
    // producer should only wait for his own acks.
    struct AckMonitor
    {
        // Increments the number of sent acks
        void increment_pending_acks() {
            while (!flag_.test_and_set()) {
                //save the last ack number for this thread so we only
                //wait up to this number.
                last_ack_[std::this_thread::get_id()] = ++sent_acks_;
                flag_.clear();
                break;
            }
        }
        // Increments the number of received acks,
        // reducing the total pending acks.
        void decrement_pending_acks() {
            while (!flag_.test_and_set()) {
                ++received_acks_;
                flag_.clear();
                break;
            }
        }
        // Returns true if there are any pending acks overall.
        bool has_pending_acks() const {
            return get_pending_acks() > 0;
        }
        // Returns true if there are any pending acks on this thread.
        bool has_current_thread_pending_acks() const {
            return get_current_thread_pending_acks() > 0;
        }
        // Returns total pending acks. This is the difference between
        // total produced and total received.
        ssize_t get_pending_acks() const {
            ssize_t rc = 0;
            while (!flag_.test_and_set()) {
                rc = get_pending_acks_impl();
                flag_.clear();
                break;
            }
            return rc;
        }
        // Returns the total pending acks for this thread
        ssize_t get_current_thread_pending_acks() const {
            ssize_t rc = 0;
            while (!flag_.test_and_set()) {
                rc = get_current_thread_pending_acks_impl();
                flag_.clear();
                break;
            }
            return rc;
        }
    private:
        ssize_t get_pending_acks_impl() const {
            return (sent_acks_ - received_acks_);
        }
        ssize_t get_current_thread_pending_acks_impl() const {
            auto it  = last_ack_.find(std::this_thread::get_id());
            if (it != last_ack_.end()) {
                return (it->second > received_acks_) ? it->second - received_acks_ : 0;
            }
            return 0;
        }
        mutable std::atomic_flag flag_{0};
        ssize_t sent_acks_{0};
        ssize_t received_acks_{0};
        std::map<std::thread::id, ssize_t> last_ack_; //last ack number expected for this thread
    };
    
    // Returns existing tracker or creates new one
    template <typename BuilderType>
    TrackerPtr add_tracker(SenderType sender, BuilderType& builder) {
        if (enable_message_retries_) {
            if (!builder.internal()) {
                // Add message tracker only if it hasn't been added before
                builder.internal(std::make_shared<Tracker>(sender, max_number_retries_));
                return std::static_pointer_cast<Tracker>(builder.internal());
            }
            // Return existing tracker
            TrackerPtr tracker = std::static_pointer_cast<Tracker>(builder.internal());
            // Update the sender type. Since a message could have been initially produced
            // asynchronously but then flushed synchronously (or vice-versa), the sender
            // type should always reflect the latest retry mechanism.
            tracker->set_sender_type(sender);
            return tracker;
        }
        return nullptr;
    }
    template <typename BuilderType>
    void do_add_message(BuilderType&& builder, QueueKind queue_kind, FlushAction flush_action);
    template <typename BuilderType>
    void produce_message(BuilderType&& builder);
    bool sync_produce(const MessageBuilder& builder, std::chrono::milliseconds timeout, bool throw_on_error);
    Configuration prepare_configuration(Configuration config);
    void on_delivery_report(const Message& message);
    template <typename BuilderType>
    void async_produce(BuilderType&& message, bool throw_on_error);
    static void swap_queues(QueueType & queue1, QueueType & queue2, std::mutex & mutex);
    bool wait_for_acks_impl(Threads threads, std::chrono::milliseconds timeout);

    // Static members
    static const std::chrono::milliseconds infinite_timeout;
    static const std::chrono::milliseconds no_timeout;
    
    // Members
    Producer producer_;
    QueueType messages_;
    QueueType retry_messages_;
    mutable std::mutex mutex_;
    mutable std::mutex retry_mutex_;
    ProduceSuccessCallback produce_success_callback_;
    ProduceFailureCallback produce_failure_callback_;
    ProduceTerminationCallback produce_termination_callback_;
    FlushFailureCallback flush_failure_callback_;
    FlushTerminationCallback flush_termination_callback_;
    QueueFullCallback queue_full_callback_;
    ssize_t max_buffer_size_{-1};
    FlushMethod flush_method_{FlushMethod::Sync};
    AckMonitor ack_monitor_;
    std::atomic<size_t> flushes_in_progress_{0};
    std::atomic<size_t> total_messages_produced_{0};
    std::atomic<size_t> total_messages_dropped_{0};
    int max_number_retries_{0};
    bool enable_message_retries_{false};
    QueueFullNotification queue_full_notification_{QueueFullNotification::None};
#ifdef KAFKA_TEST_INSTANCE
    TestParameters* test_params_;
#endif
};

// Full blocking wait as per RdKafka
template <typename BufferType, typename Allocator>
const std::chrono::milliseconds
BufferedProducer<BufferType, Allocator>::infinite_timeout = std::chrono::milliseconds(-1);
template <typename BufferType, typename Allocator>
const std::chrono::milliseconds
BufferedProducer<BufferType, Allocator>::no_timeout = std::chrono::milliseconds::zero();

template <typename BufferType>
Producer::PayloadPolicy get_default_payload_policy() {
    return Producer::PayloadPolicy::COPY_PAYLOAD;
}

template <> inline
Producer::PayloadPolicy get_default_payload_policy<Buffer>() {
    return Producer::PayloadPolicy::PASSTHROUGH_PAYLOAD;
}

template <typename BufferType, typename Allocator>
BufferedProducer<BufferType, Allocator>::BufferedProducer(Configuration config,
                                                          const Allocator& alloc)
: producer_(prepare_configuration(std::move(config))),
  messages_(alloc),
  retry_messages_(alloc) {
    producer_.set_payload_policy(get_default_payload_policy<BufferType>());
#ifdef KAFKA_TEST_INSTANCE
    test_params_ = nullptr;
#endif
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::add_message(const MessageBuilder& builder) {
    add_message(Builder(builder)); //make ConcreteBuilder
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::add_message(Builder builder) {
    add_tracker(SenderType::Async, builder);
    //post message unto the producer queue
    do_add_message(move(builder), QueueKind::Produce, FlushAction::DoFlush);
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::produce(const MessageBuilder& builder) {
    if (enable_message_retries_) {
        //Adding a retry tracker requires copying the builder since
        //we cannot modify the original instance. Cloning is a fast operation
        //since the MessageBuilder class holds pointers to data only.
        MessageBuilder builder_clone(builder.clone());
        add_tracker(SenderType::Async, builder_clone);
        async_produce(builder_clone, true);
    }
    else {
        async_produce(builder, true);
    }
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::sync_produce(const MessageBuilder& builder) {
    sync_produce(builder, infinite_timeout, true);
}

template <typename BufferType, typename Allocator>
bool BufferedProducer<BufferType, Allocator>::sync_produce(const MessageBuilder& builder,
                                                           std::chrono::milliseconds timeout) {
    return sync_produce(builder, infinite_timeout, true);
}

template <typename BufferType, typename Allocator>
bool BufferedProducer<BufferType, Allocator>::sync_produce(const MessageBuilder& builder,
                                                           std::chrono::milliseconds timeout,
                                                           bool throw_on_error) {
    if (enable_message_retries_) {
        //Adding a retry tracker requires copying the builder since
        //we cannot modify the original instance. Cloning is a fast operation
        //since the MessageBuilder class holds pointers to data only.
        MessageBuilder builder_clone(builder.clone());
        TrackerPtr tracker = add_tracker(SenderType::Sync, builder_clone);
        // produce until we succeed or we reach max retry limit
        auto endTime = std::chrono::steady_clock::now() + timeout;
        do {
            try {
                tracker->prepare_to_retry();
                produce_message(builder_clone);
                //Wait w/o timeout since we must get the ack to avoid a race condition.
                //Otherwise retry_again() will block as the producer won't get flushed
                //and the delivery callback will never be invoked.
                wait_for_current_thread_acks();
            }
            catch (const HandleException& ex) {
                // If we have a flush failure callback and it returns true, we retry producing this message later
                CallbackInvoker<FlushFailureCallback> callback("flush failure", flush_failure_callback_, &producer_);
                if (!callback || callback(builder, ex.get_error())) {
                    if (tracker && tracker->has_retries_left()) {
                        tracker->decrement_retries();
                        continue;
                    }
                }
                ++total_messages_dropped_;
                // Call the flush termination callback
                CallbackInvoker<FlushTerminationCallback>("flush termination", flush_termination_callback_, &producer_)
                        (builder, ex.get_error());
                if (throw_on_error) {
                    throw;
                }
                break;
            }
        }
        while (tracker->retry_again() &&
              ((timeout == infinite_timeout) ||
              (std::chrono::steady_clock::now() >= endTime)));
        return !tracker->has_retries_left();
    }
    else {
        // produce once
        try {
            produce_message(builder);
            wait_for_current_thread_acks(timeout);
            return !ack_monitor_.has_current_thread_pending_acks();
        }
        catch (const HandleException& ex) {
            ++total_messages_dropped_;
            // Call the flush termination callback
            CallbackInvoker<FlushTerminationCallback>("flush termination", flush_termination_callback_, &producer_)
                    (builder, ex.get_error());
            if (throw_on_error) {
                throw;
            }
        }
    }
    return false;
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::produce(const Message& message) {
    async_produce(MessageBuilder(message), true);
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::async_flush() {
    flush(no_timeout, false);
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::flush(bool preserve_order) {
    flush(infinite_timeout, preserve_order);
}

template <typename BufferType, typename Allocator>
bool BufferedProducer<BufferType, Allocator>::flush(std::chrono::milliseconds timeout,
                                                    bool preserve_order) {
    CounterGuard<size_t> counter_guard(flushes_in_progress_);
    auto queue_flusher = [timeout, preserve_order, this]
                         (QueueType& queue, std::mutex & mutex)->void
    {
        QueueType flush_queue; // flush from temporary queue
        swap_queues(queue, flush_queue, mutex);
        //Produce one message at a time and wait for acks until queue is empty
        while (!flush_queue.empty()) {
            if (preserve_order) {
                //When preserving order, we must ensure that each message
                //gets delivered before producing the next one.
                sync_produce(flush_queue.front(), timeout, false);
            }
            else {
                //Produce as fast as possible w/o waiting. If one or more
                //messages fail, they will be re-enqueued for retry
                //on the next flush cycle, which causes re-ordering.
                async_produce(flush_queue.front(), false);
            }
            flush_queue.pop_front();
        }
    };
    //Produce retry queue first since these messages were produced first.
    queue_flusher(retry_messages_, retry_mutex_);
    //Produce recently enqueued messages
    queue_flusher(messages_, mutex_);
    if (!preserve_order) {
        //Wait for acks from the messages produced above via async_produce
        wait_for_current_thread_acks(timeout);
    }
    return !ack_monitor_.has_current_thread_pending_acks();
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::wait_for_acks() {
    //block until all acks have been received
    wait_for_acks_impl(Threads::All, infinite_timeout);
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::wait_for_current_thread_acks() {
    //block until all acks from the current thread have been received
    wait_for_acks_impl(Threads::Current, infinite_timeout);
}

template <typename BufferType, typename Allocator>
bool BufferedProducer<BufferType, Allocator>::wait_for_acks(std::chrono::milliseconds timeout) {
    //block until all acks have been received
    return wait_for_acks_impl(Threads::All, timeout);
}

template <typename BufferType, typename Allocator>
bool BufferedProducer<BufferType, Allocator>::wait_for_current_thread_acks(std::chrono::milliseconds timeout) {
    //block until all acks from the current thread have been received
    return wait_for_acks_impl(Threads::Current, timeout);
}

template <typename BufferType, typename Allocator>
bool BufferedProducer<BufferType, Allocator>::wait_for_acks_impl(Threads threads,
                                                                 std::chrono::milliseconds timeout) {
    auto remaining = timeout;
    auto start_time = std::chrono::high_resolution_clock::now();
    bool pending_acks = true;
    do {
        try {
            producer_.flush(remaining);
        }
        catch (const HandleException& ex) {
            // If we just hit the timeout, keep going, otherwise re-throw
            if (ex.get_error() == RD_KAFKA_RESP_ERR__TIMED_OUT) {
                // There is no time remaining
                pending_acks = (threads == Threads::All) ?
                                ack_monitor_.has_pending_acks() :
                                ack_monitor_.has_current_thread_pending_acks();
                return !pending_acks;
            }
            else {
                throw;
            }
        }
        // calculate remaining time
        remaining = timeout - std::chrono::duration_cast<std::chrono::milliseconds>
            (std::chrono::high_resolution_clock::now() - start_time);
        pending_acks = (threads == Threads::All) ?
                        ack_monitor_.has_pending_acks() :
                        ack_monitor_.has_current_thread_pending_acks();
    } while (pending_acks && ((remaining.count() > 0) || (timeout == infinite_timeout)));
    return !pending_acks;
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::clear() {
    QueueType tmp;
    swap_queues(messages_, tmp, mutex_);
    QueueType retry_tmp;
    swap_queues(retry_messages_, retry_tmp, retry_mutex_);
}

template <typename BufferType, typename Allocator>
size_t BufferedProducer<BufferType, Allocator>::get_buffer_size() const {
    size_t size = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        size += messages_.size();
    }
    {
        std::lock_guard<std::mutex> lock(retry_mutex_);
        size += retry_messages_.size();
    }
    return size;
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::set_max_buffer_size(ssize_t max_buffer_size) {
    if (max_buffer_size < -1) {
        throw Exception("Invalid buffer size.");
    }
    max_buffer_size_ = max_buffer_size;
}

template <typename BufferType, typename Allocator>
ssize_t BufferedProducer<BufferType, Allocator>::get_max_buffer_size() const {
    return max_buffer_size_;
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::set_flush_method(FlushMethod method) {
    flush_method_ = method;
}

template <typename BufferType, typename Allocator>
typename BufferedProducer<BufferType, Allocator>::FlushMethod
BufferedProducer<BufferType, Allocator>::get_flush_method() const {
    return flush_method_;
}

template <typename BufferType, typename Allocator>
template <typename BuilderType>
void BufferedProducer<BufferType, Allocator>::do_add_message(BuilderType&& builder,
                                                             QueueKind queue_kind,
                                                             FlushAction flush_action) {
    if (queue_kind == QueueKind::Retry) {
        std::lock_guard<std::mutex> lock(retry_mutex_);
        retry_messages_.emplace_back(std::forward<BuilderType>(builder));
    }
    else {
        std::lock_guard<std::mutex> lock(mutex_);
        messages_.emplace_back(std::forward<BuilderType>(builder));
    }
    // Flush the queues only if a produced message is added. Retry messages may be added
    // from on_delivery_report() during which flush()/async_flush() cannot be called.
    if (queue_kind == QueueKind::Produce &&
        flush_action == FlushAction::DoFlush &&
        (max_buffer_size_ >= 0) &&
        (max_buffer_size_ <= (ssize_t)get_buffer_size())) {
        if (flush_method_ == FlushMethod::Sync) {
            flush();
        }
        else {
            async_flush();
        }
    }
}

template <typename BufferType, typename Allocator>
Producer& BufferedProducer<BufferType, Allocator>::get_producer() {
    return producer_;
}

template <typename BufferType, typename Allocator>
const Producer& BufferedProducer<BufferType, Allocator>::get_producer() const {
    return producer_;
}

template <typename BufferType, typename Allocator>
size_t BufferedProducer<BufferType, Allocator>::get_pending_acks() const {
    return ack_monitor_.get_pending_acks();
}

template <typename BufferType, typename Allocator>
size_t BufferedProducer<BufferType, Allocator>::get_current_thread_pending_acks() const {
    return ack_monitor_.get_current_thread_pending_acks();
}

template <typename BufferType, typename Allocator>
size_t BufferedProducer<BufferType, Allocator>::get_total_messages_produced() const {
    return total_messages_produced_;
}

template <typename BufferType, typename Allocator>
size_t BufferedProducer<BufferType, Allocator>::get_total_messages_dropped() const {
    return total_messages_dropped_;
}

template <typename BufferType, typename Allocator>
size_t BufferedProducer<BufferType, Allocator>::get_flushes_in_progress() const {
    return flushes_in_progress_;
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::set_max_number_retries(size_t max_number_retries) {
    if (!enable_message_retries_ && (max_number_retries > 0)) {
        enable_message_retries_ = true; //enable once
    }
    max_number_retries_ = max_number_retries;
}

template <typename BufferType, typename Allocator>
size_t BufferedProducer<BufferType, Allocator>::get_max_number_retries() const {
    return max_number_retries_;
}

template <typename BufferType, typename Allocator>
typename BufferedProducer<BufferType, Allocator>::Builder
BufferedProducer<BufferType, Allocator>::make_builder(std::string topic) {
    return Builder(std::move(topic));
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::set_queue_full_notification(QueueFullNotification notification) {
    queue_full_notification_ = notification;
}

template <typename BufferType, typename Allocator>
typename BufferedProducer<BufferType, Allocator>::QueueFullNotification
BufferedProducer<BufferType, Allocator>::get_queue_full_notification() const {
    return queue_full_notification_;
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::set_produce_failure_callback(ProduceFailureCallback callback) {
    produce_failure_callback_ = std::move(callback);
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::set_produce_termination_callback(ProduceTerminationCallback callback) {
    produce_termination_callback_ = std::move(callback);
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::set_produce_success_callback(ProduceSuccessCallback callback) {
    produce_success_callback_ = std::move(callback);
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::set_flush_failure_callback(FlushFailureCallback callback) {
    flush_failure_callback_ = std::move(callback);
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::set_flush_termination_callback(FlushTerminationCallback callback) {
    flush_termination_callback_ = std::move(callback);
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::set_queue_full_callback(QueueFullCallback callback) {
    queue_full_callback_ = std::move(callback);
}

template <typename BufferType, typename Allocator>
template <typename BuilderType>
void BufferedProducer<BufferType, Allocator>::produce_message(BuilderType&& builder) {
    using builder_type = typename std::decay<BuilderType>::type;
    bool queue_full_notify = queue_full_notification_ != QueueFullNotification::None;
    while (true) {
        try {
            MessageInternalGuard<builder_type> internal_guard(const_cast<builder_type&>(builder));
            producer_.produce(builder);
            internal_guard.release();
            // Sent successfully
            ack_monitor_.increment_pending_acks();
            break;
        }
        catch (const HandleException& ex) {
            if (ex.get_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                // If the output queue is full, then just poll
                producer_.poll();
                // Notify application so it can slow-down production
                if (queue_full_notify) {
                    queue_full_notify = queue_full_notification_ == QueueFullNotification::EachOccurence;
                    CallbackInvoker<QueueFullCallback>("queue full", queue_full_callback_, &producer_)(builder);
                }
            }
            else {
                throw;
            }
        }
    }
}

template <typename BufferType, typename Allocator>
template <typename BuilderType>
void BufferedProducer<BufferType, Allocator>::async_produce(BuilderType&& builder, bool throw_on_error) {
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
            if (tracker && tracker->has_retries_left()) {
                tracker->decrement_retries();
                //Post message unto the retry queue. This queue has higher priority and will be
                //flushed before the producer queue to preserve original message order.
                //We don't flush now since we just had an error while producing.
                do_add_message(std::forward<BuilderType>(builder), QueueKind::Retry, FlushAction::DontFlush);
                return;
            }
        }
        ++total_messages_dropped_;
        // Call the flush termination callback
        CallbackInvoker<FlushTerminationCallback>("flush termination", flush_termination_callback_, &producer_)
            (builder, ex.get_error());
        if (throw_on_error) {
            throw;
        }
    }
}

template <typename BufferType, typename Allocator>
Configuration BufferedProducer<BufferType, Allocator>::prepare_configuration(Configuration config) {
    using std::placeholders::_2;
    auto callback = std::bind(&BufferedProducer<BufferType, Allocator>::on_delivery_report, this, _2);
    config.set_delivery_report_callback(std::move(callback));
    return config;
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::on_delivery_report(const Message& message) {
    TestParameters* test_params = get_test_parameters();
    //Get tracker if present
    TrackerPtr tracker =
        enable_message_retries_ ?
        std::static_pointer_cast<Tracker>(MessageInternal::load(const_cast<Message&>(message))->get_internal()) :
        nullptr;
    bool retry = false;
    if (message.get_error() || (test_params && test_params->force_delivery_error_)) {
        // We should produce this message again if we don't have a produce failure callback
        // or we have one but it returns true (indicating error is re-tryable)
        CallbackInvoker<ProduceFailureCallback> callback("produce failure", produce_failure_callback_, &producer_);
        if (!callback || callback(message)) {
            // Check if we have reached the maximum retry limit
            if (tracker && tracker->has_retries_left()) {
                tracker->decrement_retries();
                //If the sender is asynchronous, the message is re-enqueued. If the sender is
                //synchronous, we simply notify via Tracker::should_retry() below.
                if (tracker->get_sender_type() == SenderType::Async) {
                    //Post message unto the retry queue. This queue has higher priority and will be
                    //flushed later by the application (before the producer queue) to preserve original message order.
                    //We prevent flushing now since we are within a callback context.
                    do_add_message(Builder(message), QueueKind::Retry, FlushAction::DontFlush);
                }
                retry = true;
            }
            else {
                ++total_messages_dropped_;
                CallbackInvoker<ProduceTerminationCallback>
                    ("produce termination", produce_termination_callback_, &producer_)(message);
            }
        }
        else {
            ++total_messages_dropped_;
            CallbackInvoker<ProduceTerminationCallback>
                ("produce termination", produce_termination_callback_, &producer_)(message);
        }
    }
    else {
        // Successful delivery
        CallbackInvoker<ProduceSuccessCallback>("delivery success", produce_success_callback_, &producer_)(message);
        // Increment the total successful transmissions
        ++total_messages_produced_;
    }
    // Signal synchronous sender and unblock it since it's waiting for this ack to arrive.
    if (tracker) {
        tracker->should_retry(retry);
    }
    // Decrement the expected acks and check to prevent underflow
    ack_monitor_.decrement_pending_acks();
}

template <typename BufferType, typename Allocator>
void BufferedProducer<BufferType, Allocator>::swap_queues(BufferedProducer<BufferType, Allocator>::QueueType & queue1,
                                                          BufferedProducer<BufferType, Allocator>::QueueType & queue2,
                                                          std::mutex & mutex)
{
    std::lock_guard<std::mutex> lock(mutex);
    std::swap(queue1, queue2);
}

} // cppkafka

#endif // CPPKAFKA_BUFFERED_PRODUCER_H
