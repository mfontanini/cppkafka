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

#ifndef CPPKAFKA_MESSAGE_BUILDER_H
#define CPPKAFKA_MESSAGE_BUILDER_H

#include <boost/optional.hpp>
#include "buffer.h"
#include "topic.h"
#include "partition.h"

namespace cppkafka {

/**
 * \brief Message builder class
 * 
 * Allows building a message including topic, partition, key, payload, etc.
 *
 * Example:
 *
 * \code
 * Producer producer(...);
 * Topic topic = producer.get_topic("test");
 * 
 * string payload = "hello world";
 * producer.produce(MessageBuilder(topic).partition(5).payload(payload));
 * \endcode
 */
class MessageBuilder {
public:
    /**
     * Construct a MessageBuilder
     *
     * \param topic The topic into which this message would be produced
     */
    MessageBuilder(const Topic& topic);

    /**
     * Sets the partition into which this message will be produced
     *
     * \param value The partition to be used  
     */
    MessageBuilder& partition(Partition value);

    /**
     * Sets the message's key
     *
     * \param value The key to be used
     */
    MessageBuilder& key(const Buffer& value);

    /**
     * Sets the message's payload
     *
     * \param value The payload to be used
     */
    MessageBuilder& payload(const Buffer& value);

    /**
     * Sets the message's user data pointer
     *
     * \param value Pointer to the user data to be used on the produce call
     */
    MessageBuilder& user_data(void* value);

    /**
     * Gets the topic this message will be produced into
     */
    const Topic& topic() const;

    /**
     * Gets the partition this message will be produced into
     */
    const Partition& partition() const;

    /**
     * Gets the message's key
     */
    const Buffer& key() const;

    /**
     * Gets the message's payload
     */
    const Buffer& payload() const;

    /**
     * Gets the message's user data pointer
     */
    void* user_data() const;
private:
    const Topic& topic_;
    Partition partition_;
    Buffer key_;
    Buffer payload_;
    void* user_data_;
};

} // cppkafka

#endif // CPPKAFKA_MESSAGE_BUILDER_H
