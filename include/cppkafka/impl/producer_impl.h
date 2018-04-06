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

namespace cppkafka {

template <typename Traits>
ProducerHandle<Traits>::ProducerHandle(typename Traits::config_type config)
: base_type(move(config)),
  message_payload_policy_(PayloadPolicy::COPY_PAYLOAD) {
    char error_buffer[512];
    auto config_handle = this->get_configuration().get_handle();
    rd_kafka_conf_set_opaque(config_handle, this);
    rd_kafka_t* ptr = rd_kafka_new(RD_KAFKA_PRODUCER,
                                   rd_kafka_conf_dup(config_handle),
                                   error_buffer, sizeof(error_buffer));
    if (!ptr) {
        throw Exception("Failed to create producer handle: " + string(error_buffer));
    }
    rd_kafka_set_log_level(ptr, 7);
    this->set_handle(ptr);
}

template <typename Traits>
void ProducerHandle<Traits>::set_payload_policy(typename ProducerHandle<Traits>::PayloadPolicy policy) {
    message_payload_policy_ = policy;
}

template <typename Traits>
typename ProducerHandle<Traits>::PayloadPolicy
ProducerHandle<Traits>::get_payload_policy() const {
    return message_payload_policy_;
}

template <typename Traits>
void ProducerHandle<Traits>::produce(const MessageBuilder& builder) {
    const Buffer& payload = builder.payload();
    const Buffer& key = builder.key();
    const int policy = static_cast<int>(message_payload_policy_);
    auto result = rd_kafka_producev(this->get_handle(),
                                    RD_KAFKA_V_TOPIC(builder.topic().data()),
                                    RD_KAFKA_V_PARTITION(builder.partition()),
                                    RD_KAFKA_V_MSGFLAGS(policy),
                                    RD_KAFKA_V_TIMESTAMP(builder.timestamp().count()),
                                    RD_KAFKA_V_KEY((void*)key.get_data(), key.get_size()),
                                    RD_KAFKA_V_VALUE((void*)payload.get_data(), payload.get_size()),
                                    RD_KAFKA_V_OPAQUE(builder.user_data()),
                                    RD_KAFKA_V_END);
    this->check_error(result);
}

template <typename Traits>
int ProducerHandle<Traits>::poll() {
    return poll(this->get_timeout());
}

template <typename Traits>
int ProducerHandle<Traits>::poll(milliseconds timeout) {
    return rd_kafka_poll(this->get_handle(), static_cast<int>(timeout.count()));
}

template <typename Traits>
void ProducerHandle<Traits>::flush() {
    flush(this->get_timeout());
}

template <typename Traits>
void ProducerHandle<Traits>::flush(milliseconds timeout) {
    auto result = rd_kafka_flush(this->get_handle(), static_cast<int>(timeout.count()));
    this->check_error(result);
}

} // cppkafka
