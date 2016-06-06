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

#ifndef CPPKAFKA_MESSAGE_H
#define CPPKAFKA_MESSAGE_H

#include <memory>
#include <cstdint>
#include <librdkafka/rdkafka.h>
#include "buffer.h"
#include "topic.h"

namespace cppkafka {

class Message {
public:
    static Message make_non_owning(rd_kafka_message_t* handle);

    Message();
    Message(rd_kafka_message_t* handle);
    Message(const Message&) = delete;
    Message(Message&& rhs) = default;
    Message& operator=(const Message&) = delete;
    Message& operator=(Message&& rhs) = default;

    bool has_error() const;
    rd_kafka_resp_err_t get_error() const;
    std::string get_topic() const;
    int get_partition() const;
    const Buffer& get_payload() const;
    const Buffer& get_key() const;
    int64_t get_offset() const;
    void* private_data();
    explicit operator bool() const;

    rd_kafka_message_t* get_handle() const;
private:
    using HandlePtr = std::unique_ptr<rd_kafka_message_t, decltype(&rd_kafka_message_destroy)>;

    struct NonOwningTag { };

    Message(rd_kafka_message_t* handle, NonOwningTag);
    Message(HandlePtr handle);

    HandlePtr handle_;
    Buffer payload_;
    Buffer key_;
};

} // cppkafka

#endif // CPPKAFKA_MESSAGE_H
