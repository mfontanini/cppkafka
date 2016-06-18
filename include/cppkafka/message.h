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

/**
 * \brief Thin wrapper over a rdkafka message handle
 *
 * This is a non copyable, movable class that wraps a rd_kafka_message_t*.
 *
 * Messages can be empty (contain a null rd_kafka_message_t*). Therefore, users should check
 * that the message isn't empty by using the operator bool() before using them. This is especially
 * necessary when calling Consumer::poll() as any poll operation that returns a null pointer will
 * return an empty message.
 */
class Message {
public:
    /**
     * Constructs a message that won't take ownership of the given pointer
     */
    static Message make_non_owning(rd_kafka_message_t* handle);

    /**
     * Constructs an empty message
     */
    Message();

    /**
     * \brief Constructs a message from a handle
     *
     * The constructed message instance *will own* the given pointer, calling 
     * rd_kafka_message_destroy upon destruction.
     *
     * \param handle The message handle to be wrapped
     */
    Message(rd_kafka_message_t* handle);
    Message(const Message&) = delete;
    Message(Message&& rhs) = default;
    Message& operator=(const Message&) = delete;
    Message& operator=(Message&& rhs) = default;

    /**
     * Indicates whether this is a message carrying an error notification
     */
    bool has_error() const;

    /**
     * Gets the error attribute
     */
    rd_kafka_resp_err_t get_error() const;

    /**
     * Gets the error as a string
     */
    std::string get_error_string() const;    

    /**
     * Utility function to check for get_error() == RD_KAFKA_RESP_ERR__PARTITION_EOF
     */
    bool is_eof() const;

    /**
     * Gets the topic that this message belongs to
     */
    std::string get_topic() const;

    /**
     * Gets the partition that this message belongs to
     */
    int get_partition() const;

    /**
     * Gets the message's payload
     */
    const Buffer& get_payload() const;

    /**
     * Gets the message's key
     */
    const Buffer& get_key() const;

    /**
     * Gets the message offset
     */
    int64_t get_offset() const;

    /**
     * \brief Gets the private data.
     *
     * This should only be used on messages produced by a Producer that were set a private data
     * attribute 
     */
    void* private_data() const;

    /**
     * Indicates whether this message is valid (not null)
     */
    explicit operator bool() const;

    /**
     * Gets the rdkafka message handle
     */
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
