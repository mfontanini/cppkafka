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

#ifndef CPPKAFKA_MESSAGE_BUILDER_H
#define CPPKAFKA_MESSAGE_BUILDER_H

#include <chrono>
#include "buffer.h"
#include "topic.h"
#include "macros.h"
#include "message.h"

namespace cppkafka {

/**
 * \brief Base template class for message construction
 */
template <typename BufferType, typename Concrete>
class BasicMessageBuilder {
public:
    /**
     * Construct a BasicMessageBuilder
     *
     * \param topic The topic into which this message would be produced
     */
    BasicMessageBuilder(std::string topic);
    
    /**
     * Construct a BasicMessageBuilder from a Message object
     */
    BasicMessageBuilder(const Message& message);

    /**
     * \brief Construct a message builder from another one that uses a different buffer type
     *
     * Note that this can only be used if BufferType can be constructed from an OtherBufferType
     *
     * \param rhs The message builder to be constructed from
     */
    template <typename OtherBufferType, typename OtherConcrete>
    BasicMessageBuilder(const BasicMessageBuilder<OtherBufferType, OtherConcrete>& rhs);

    BasicMessageBuilder(BasicMessageBuilder&&) = default;
    BasicMessageBuilder(const BasicMessageBuilder&) = default;
    BasicMessageBuilder& operator=(BasicMessageBuilder&&) = default;
    BasicMessageBuilder& operator=(const BasicMessageBuilder&) = default;

    /**
     * Sets the topic in which this message will be produced
     *
     * \param value The topic to be used
     */
    Concrete& topic(std::string value);

    /**
     * Sets the partition into which this message will be produced
     *
     * \param value The partition to be used  
     */
    Concrete& partition(int value);

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
     * Sets the message's timestamp
     *
     * \param value The timestamp to be used
     */
    Concrete& timestamp(std::chrono::milliseconds value);

    /**
     * Sets the message's user data pointer
     *
     * \param value Pointer to the user data to be used on the produce call
     */
    Concrete& user_data(void* value);

    /**
     * Gets the topic this message will be produced into
     */
    const std::string& topic() const;

    /**
     * Gets the partition this message will be produced into
     */
    int partition() const;

    /**
     * Gets the message's key
     */
    const BufferType& key() const;

    /**
     * Gets the message's key
     */
    BufferType& key();

    /**
     * Gets the message's payload
     */
    const BufferType& payload() const;

    /**
     * Gets the message's payload
     */
    BufferType& payload();

    /**
     * Gets the message's timestamp
     */
    std::chrono::milliseconds timestamp() const;

    /**
     * Gets the message's user data pointer
     */
    void* user_data() const;
    
    /**
     * Private data accessor (internal use only)
     */
    Message::InternalPtr internal() const;
    Concrete& internal(Message::InternalPtr internal);
    
private:
    void construct_buffer(BufferType& lhs, const BufferType& rhs);
    Concrete& get_concrete();

    std::string topic_;
    int partition_{-1};
    BufferType key_;
    BufferType payload_;
    std::chrono::milliseconds timestamp_{0};
    void* user_data_;
    Message::InternalPtr internal_;
};

template <typename T, typename C>
BasicMessageBuilder<T, C>::BasicMessageBuilder(std::string topic)
: topic_(std::move(topic)),
  user_data_(nullptr) {
}

template <typename T, typename C>
BasicMessageBuilder<T, C>::BasicMessageBuilder(const Message& message)
: topic_(message.get_topic()),
  key_(Buffer(message.get_key().get_data(), message.get_key().get_size())),
  payload_(Buffer(message.get_payload().get_data(), message.get_payload().get_size())),
  timestamp_(message.get_timestamp() ? message.get_timestamp().get().get_timestamp() :
                                       std::chrono::milliseconds(0)),
  user_data_(message.get_user_data()),
  internal_(message.internal()) {
}

template <typename T, typename C>
template <typename U, typename V>
BasicMessageBuilder<T, C>::BasicMessageBuilder(const BasicMessageBuilder<U, V>& rhs)
: topic_(rhs.topic()), partition_(rhs.partition()), timestamp_(rhs.timestamp()),
  user_data_(rhs.user_data()),
  internal_(rhs.internal()) {
    get_concrete().construct_buffer(key_, rhs.key());
    get_concrete().construct_buffer(payload_, rhs.payload());
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::topic(std::string value) {
    topic_ = std::move(value);
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::partition(int value) {
    partition_ = value;
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::key(const T& value) {
    get_concrete().construct_buffer(key_, value);
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::key(T&& value) {
    key_ = std::move(value);
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::payload(const T& value) {
    get_concrete().construct_buffer(payload_, value);
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::payload(T&& value) {
    payload_ = std::move(value);
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::timestamp(std::chrono::milliseconds value) {
    timestamp_ = value;
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::user_data(void* value) {
    user_data_ = value;
    return get_concrete();
}

template <typename T, typename C>
const std::string& BasicMessageBuilder<T, C>::topic() const {
    return topic_;
}

template <typename T, typename C>
int BasicMessageBuilder<T, C>::partition() const {
    return partition_;
}

template <typename T, typename C>
const T& BasicMessageBuilder<T, C>::key() const {
    return key_;
}

template <typename T, typename C>
T& BasicMessageBuilder<T, C>::key() {
    return key_;
}

template <typename T, typename C>
const T& BasicMessageBuilder<T, C>::payload() const {
    return payload_;
}

template <typename T, typename C>
T& BasicMessageBuilder<T, C>::payload() {
    return payload_;
}

template <typename T, typename C>
std::chrono::milliseconds BasicMessageBuilder<T, C>::timestamp() const {
    return timestamp_;
}

template <typename T, typename C>
void* BasicMessageBuilder<T, C>::user_data() const {
    return user_data_;
}

template <typename T, typename C>
Message::InternalPtr BasicMessageBuilder<T, C>::internal() const {
    return internal_;
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::internal(Message::InternalPtr internal) {
    internal_ = internal;
    return get_concrete();
}

template <typename T, typename C>
void BasicMessageBuilder<T, C>::construct_buffer(T& lhs, const T& rhs) {
    lhs = rhs;
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::get_concrete() {
    return static_cast<C&>(*this);
}

/**
 * \brief Message builder class
 * 
 * Allows building a message including topic, partition, key, payload, etc.
 *
 * Example:
 *
 * \code
 * Producer producer(...);
 * 
 * string payload = "hello world";
 * producer.produce(MessageBuilder("test").partition(5).payload(payload));
 * \endcode
 */
class MessageBuilder : public BasicMessageBuilder<Buffer, MessageBuilder> {
public:
    using BasicMessageBuilder::BasicMessageBuilder;

    void construct_buffer(Buffer& lhs, const Buffer& rhs) {
        lhs = Buffer(rhs.get_data(), rhs.get_size());
    }

    template <typename T>
    void construct_buffer(Buffer& lhs, const T& rhs) {
        lhs = Buffer(rhs);
    }
    
    MessageBuilder clone() const {
        return std::move(MessageBuilder(topic()).
                             key(Buffer(key().get_data(), key().get_size())).
                             payload(Buffer(payload().get_data(), payload().get_size())).
                             timestamp(timestamp()).
                             user_data(user_data()).
                             internal(internal()));
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
