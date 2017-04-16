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
#include "macros.h"

namespace cppkafka {

/**
 * \brief Base template class for message construction
 */
template <typename BufferType, typename Concrete>
class CPPKAFKA_API BasicMessageBuilder {
public:
    /**
     * Construct a BasicMessageBuilder
     *
     * \param topic The topic into which this message would be produced
     */
    BasicMessageBuilder(const Topic& topic);

    /**
     * Sets the partition into which this message will be produced
     *
     * \param value The partition to be used  
     */
    Concrete& partition(Partition value);

    /**
     * Sets the message's key
     *
     * \param value The key to be used
     */
    Concrete& key(const BufferType& value);

    /**
     * Sets the message's key
     *
     * \param value The key to be used
     */
    Concrete& key(BufferType&& value);

    /**
     * Sets the message's payload
     *
     * \param value The payload to be used
     */
    Concrete& payload(const BufferType& value);

    /**
     * Sets the message's payload
     *
     * \param value The payload to be used
     */
    Concrete& payload(BufferType&& value);

    /**
     * Sets the message's user data pointer
     *
     * \param value Pointer to the user data to be used on the produce call
     */
    Concrete& user_data(void* value);

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
    const BufferType& key() const;

    /**
     * Gets the message's payload
     */
    const BufferType& payload() const;

    /**
     * Gets the message's user data pointer
     */
    void* user_data() const;
private:
    void construct_buffer(BufferType& lhs, const BufferType& rhs);

    const Topic& topic_;
    Partition partition_;
    BufferType key_;
    BufferType payload_;
    void* user_data_;
};

template <typename T, typename C>
BasicMessageBuilder<T, C>::BasicMessageBuilder(const Topic& topic)
: topic_(topic) {
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::partition(Partition value) {
    partition_ = value;
    return static_cast<C&>(*this);
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::key(const T& value) {
    static_cast<C&>(*this).construct_buffer(key_, value);
    return static_cast<C&>(*this);
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::key(T&& value) {
    key_ = std::move(value);
    return static_cast<C&>(*this);
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::payload(const T& value) {
    static_cast<C&>(*this).construct_buffer(payload_, value);
    return static_cast<C&>(*this);
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::payload(T&& value) {
    payload_ = std::move(value);
    return static_cast<C&>(*this);
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::user_data(void* value) {
    user_data_ = value;
    return static_cast<C&>(*this);
}

template <typename T, typename C>
const Topic& BasicMessageBuilder<T, C>::topic() const {
    return topic_;
}

template <typename T, typename C>
const Partition& BasicMessageBuilder<T, C>::partition() const {
    return partition_;
}

template <typename T, typename C>
const T& BasicMessageBuilder<T, C>::key() const {
    return key_;
}

template <typename T, typename C>
const T& BasicMessageBuilder<T, C>::payload() const {
    return payload_;
}

template <typename T, typename C>
void* BasicMessageBuilder<T, C>::user_data() const {
    return user_data_;
}

template <typename T, typename C>
void BasicMessageBuilder<T, C>::construct_buffer(T& lhs, const T& rhs) {
    lhs = rhs;
}

/**
 * \brief Message builder class
 * 
 * Allows building a message including topic, partition, key, payload, etc.
 *
 * The topic and buffer objects used <b>must</b> be kept alive while the message builder object
 * is still being used.
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
class MessageBuilder : public BasicMessageBuilder<Buffer, MessageBuilder> {
public:
    using BasicMessageBuilder::BasicMessageBuilder;

    void construct_buffer(Buffer& lhs, const Buffer& rhs) {
        lhs = Buffer(rhs.get_data(), rhs.get_size());
    }
};

/**
 * \brief Message builder class for a specific data type
 */
template <typename T>
class ConcreteMessageBuilder : public BasicMessageBuilder<T, ConcreteMessageBuilder<T>> {
public:
    using BasicMessageBuilder<T, ConcreteMessageBuilder<T>>::BasicMessageBuilder;
};

} // cppkafka

#endif // CPPKAFKA_MESSAGE_BUILDER_H
