/*
 * Copyright (c) 2016, Matias Fontanini
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

#include <errno.h>
#include "producer.h"
#include "exceptions.h"

using std::move;
using std::string;

using std::chrono::milliseconds;

namespace cppkafka {

Producer::Producer(Configuration config)
: KafkaHandleBase(move(config)) {
    char error_buffer[512];
    auto config_handle = get_configuration().get_handle();
    rd_kafka_conf_set_opaque(config_handle, this);
    rd_kafka_t* ptr = rd_kafka_new(RD_KAFKA_PRODUCER,
                                   rd_kafka_conf_dup(config_handle),
                                   error_buffer, sizeof(error_buffer));
    if (!ptr) {
        throw Exception("Failed to create producer handle: " + string(error_buffer));
    }
    rd_kafka_set_log_level(ptr, 7);
    set_handle(ptr);
    set_payload_policy(Producer::COPY_PAYLOAD);
}

void Producer::set_payload_policy(PayloadPolicy policy) {
    message_payload_policy_ = policy;
}

Producer::PayloadPolicy Producer::get_payload_policy() const {
    return message_payload_policy_;
}

void Producer::produce(const MessageBuilder& builder) {
    void* payload_ptr = (void*)builder.payload().get_data();
    void* key_ptr = (void*)builder.key().get_data();
    const int policy = static_cast<int>(message_payload_policy_);
    int result = rd_kafka_produce(builder.topic().get_handle(),
                                  builder.partition().get_partition(),
                                  policy, payload_ptr, builder.payload().get_size(),
                                  key_ptr, builder.key().get_size(), builder.user_data());
    if (result == -1) {
        throw HandleException(rd_kafka_errno2err(errno));
    }
}

int Producer::poll() {
    return poll(get_timeout());
}

int Producer::poll(milliseconds timeout) {
    return rd_kafka_poll(get_handle(), timeout.count());
}

} // cppkafka
