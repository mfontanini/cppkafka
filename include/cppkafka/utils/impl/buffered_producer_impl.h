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
#ifndef CPPKAFKA_BUFFERED_PRODUCER_IMPL_H
#define CPPKAFKA_BUFFERED_PRODUCER_IMPL_H

namespace cppkafka {

template <typename BufferType, typename ProducerType>
BufferedProducer<BufferType, ProducerType>::BufferedProducer(typename ProducerType::traits_type::config_type config)
: producer_(prepare_configuration(std::move(config))) {

}

template <typename BufferType, typename ProducerType>
void BufferedProducer<BufferType, ProducerType>::add_message(const MessageBuilder& builder) {
    do_add_message(builder);
}

template <typename BufferType, typename ProducerType>
void BufferedProducer<BufferType, ProducerType>::add_message(Builder builder) {
    do_add_message(move(builder));
}

template <typename BufferType, typename ProducerType>
void BufferedProducer<BufferType, ProducerType>::produce(const MessageBuilder& builder) {
    produce_message(builder);
    expected_acks_++;
}

template <typename BufferType, typename ProducerType>
void BufferedProducer<BufferType, ProducerType>::flush() {
    while (!messages_.empty()) {
        produce_message(messages_.front());
        messages_.pop();
    }

    wait_for_acks();
}

template <typename BufferType, typename ProducerType>
void BufferedProducer<BufferType, ProducerType>::wait_for_acks() {
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

template <typename BufferType, typename ProducerType>
void BufferedProducer<BufferType, ProducerType>::clear() {
    QueueType tmp;
    std::swap(tmp, messages_);
    expected_acks_ = 0;
    messages_acked_ = 0;
}

template <typename BufferType, typename ProducerType>
template <typename BuilderType>
void BufferedProducer<BufferType, ProducerType>::do_add_message(BuilderType&& builder) {
    expected_acks_++;
    messages_.push(std::move(builder));
}

template <typename BufferType, typename ProducerType>
ProducerType& BufferedProducer<BufferType, ProducerType>::get_producer() {
    return producer_;
}

template <typename BufferType, typename ProducerType>
const ProducerType& BufferedProducer<BufferType, ProducerType>::get_producer() const {
    return producer_;
}

template <typename BufferType, typename ProducerType>
typename BufferedProducer<BufferType, ProducerType>::Builder
BufferedProducer<BufferType, ProducerType>::make_builder(std::string topic) {
    return Builder(std::move(topic));
}

template <typename BufferType, typename ProducerType>
void BufferedProducer<BufferType, ProducerType>::set_produce_failure_callback(ProduceFailureCallback callback) {
    produce_failure_callback_ = std::move(callback);
}

template <typename BufferType, typename ProducerType>
void BufferedProducer<BufferType, ProducerType>::produce_message(const MessageBuilder& builder) {
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

template <typename BufferType, typename ProducerType>
typename ProducerType::traits_type::config_type
BufferedProducer<BufferType, ProducerType>::prepare_configuration(typename ProducerType::traits_type::config_type config) {
    using std::placeholders::_2;
    auto callback = std::bind(&BufferedProducer<BufferType>::on_delivery_report, this, _2);
    config.set_delivery_report_callback(std::move(callback));
    return config;
}

template <typename BufferType, typename ProducerType>
void BufferedProducer<BufferType, ProducerType>::on_delivery_report(const Message& message) {
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
               .payload(Buffer(payload.get_data(), payload.get_size()))
               .user_data(message.get_user_data());
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

}

#endif //CPPKAFKA_BUFFERED_PRODUCER_IMPL_H
