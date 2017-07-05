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
#include <queue>
#include <cstdint>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>
#include <map>
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
 * produced messages (either in a buffer or non buffered way) are acknowledged by the kafka
 * brokers.
 *
 * When producing messages, this class will handle cases where the producer's queue is full so it\
 * will poll until the production is successful.
 *
 * This class is not thread safe.
 */
template <typename BufferType>
class CPPKAFKA_API BufferedProducer {
public:
    /**
     * Concrete builder
     */
    using Builder = ConcreteMessageBuilder<BufferType>;

    /**
     * Callback to indicate a message failed to be produced.
     */
    using ProduceFailureCallback = std::function<bool(const Message&)>;

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
     */
    void produce(const MessageBuilder& builder);

    /**
     * \brief Flushes the buffered messages.
     *
     * This will send all messages and keep waiting until all of them are acknowledged (this is
     * done by calling wait_for_acks).
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
     */
    void set_produce_failure_callback(ProduceFailureCallback callback);
private:
    using QueueType = std::queue<Builder>;

    template <typename BuilderType>
    void do_add_message(BuilderType&& builder);
    void produce_message(const MessageBuilder& message);
    Configuration prepare_configuration(Configuration config);
    void on_delivery_report(const Message& message);

    Producer producer_;
    QueueType messages_;
    ProduceFailureCallback produce_failure_callback_;
    size_t expected_acks_{0};
    size_t messages_acked_{0};
};

template <typename BufferType>
BufferedProducer<BufferType>::BufferedProducer(Configuration config)
: producer_(prepare_configuration(std::move(config))) {

}

template <typename BufferType>
void BufferedProducer<BufferType>::add_message(const MessageBuilder& builder) {
    do_add_message(builder);
}

template <typename BufferType>
void BufferedProducer<BufferType>::add_message(Builder builder) {
    do_add_message(move(builder));
}

template <typename BufferType>
void BufferedProducer<BufferType>::produce(const MessageBuilder& builder) {
    expected_acks_++;
    produce_message(builder);
}

template <typename BufferType>
void BufferedProducer<BufferType>::flush() {
    while (!messages_.empty()) {
        produce_message(messages_.front());
        messages_.pop();
    }

    wait_for_acks();
}

template <typename BufferType>
void BufferedProducer<BufferType>::wait_for_acks() {
    while (messages_acked_ < expected_acks_) {
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
    expected_acks_ = 0;
    messages_acked_ = 0;
}

template <typename BufferType>
void BufferedProducer<BufferType>::clear() {
    QueueType tmp;
    std::swap(tmp, messages_);
}

template <typename BufferType>
template <typename BuilderType>
void BufferedProducer<BufferType>::do_add_message(BuilderType&& builder) {
    expected_acks_++;
    messages_.push(std::move(builder));
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
typename BufferedProducer<BufferType>::Builder
BufferedProducer<BufferType>::make_builder(std::string topic) {
    return Builder(std::move(topic));
}

template <typename BufferType>
void BufferedProducer<BufferType>::set_produce_failure_callback(ProduceFailureCallback callback) {
    produce_failure_callback_ = std::move(callback);
}

template <typename BufferType>
void BufferedProducer<BufferType>::produce_message(const MessageBuilder& builder) {
    bool sent = false;
    while (!sent) {
        try {
            producer_.produce(builder);
            sent = true;
        }
        catch (const HandleException& ex) {
            const Error error = ex.get_error();
            if (error == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
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
    // We should produce this message again if it has an error and we either don't have a 
    // produce failure callback or we have one but it returns true
    bool should_produce = message.get_error() &&
                          (!produce_failure_callback_ || produce_failure_callback_(message));
    if (should_produce) {
        MessageBuilder builder(message.get_topic());
        const auto& key = message.get_key();
        const auto& payload = message.get_payload();
        builder.partition(message.get_partition())
               .key(Buffer(key.get_data(), key.get_size()))
               .payload(Buffer(payload.get_data(), payload.get_size()));
        if (message.get_timestamp()) {
            builder.timestamp(message.get_timestamp()->get_timestamp());
        }
        produce_message(builder);
        return;
    }
    // If production was successful or the produce failure callback returned false, then
    // let's consider it to be acked 
    messages_acked_++;
}

} // cppkafka

#endif // CPPKAFKA_BUFFERED_PRODUCER_H
