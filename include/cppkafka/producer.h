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

#ifndef CPPKAFKA_PRODUCER_H
#define CPPKAFKA_PRODUCER_H

#include <memory>
#include <errno.h>
#include "kafka_handle_base.h"
#include "configuration.h"
#include "buffer.h"
#include "topic.h"
#include "macros.h"
#include "message_builder.h"
#include "exceptions.h"

using std::move;
using std::string;
using std::chrono::milliseconds;

namespace cppkafka {

class Buffer;
template <typename T> class TopicConfig;

/**
 * \brief ProducerHandle class
 *
 * This class allows producing messages on kafka.
 *
 * By default the payloads will be copied (using the RD_KAFKA_MSG_F_COPY flag) but the 
 * behavior can be changed, in which case rdkafka will be responsible for freeing it.
 *
 * In order to produce messages you could do something like:
 *
 * \code
 * // Set the broker list
 * Configuration config = {
 *     { "metadata.broker.list", "127.0.0.1:9092" }
 * };
 *
 * // Create a producer 
 * Producer producer(config);
 *
 * // Create some key and payload
 * string key = "some key";
 * string payload = "some payload";
 *
 * // Write a message into an unassigned partition
 * producer.produce(MessageBuilder("some_topic").payload(payload));
 *
 * // Write using a key on a fixed partition (42)
 * producer.produce(MessageBuilder("some_topic").partition(42).key(key).payload(payload));
 * 
 * \endcode
 */
template <typename Traits>
class CPPKAFKA_API ProducerHandle : public HandleBase<Traits> {
public:
    using traits_type = Traits;
    using base_type = HandleBase<traits_type>;
    using config_type = typename traits_type::config_type;
    using topic_config_type = typename traits_type::topic_config_type;
    /**
     * The policy to use for the payload. The default policy is COPY_PAYLOAD
     */
    enum class PayloadPolicy {
        COPY_PAYLOAD = RD_KAFKA_MSG_F_COPY, ///< Means RD_KAFKA_MSG_F_COPY
        FREE_PAYLOAD = RD_KAFKA_MSG_F_FREE  ///< Means RD_KAFKA_MSG_F_FREE
    };

    /**
     * Constructs a producer using the given configuration
     *
     * \param config The configuration to use
     */
    ProducerHandle(config_type config);

    /**
     * Sets the payload policy
     *
     * \param policy The payload policy to be used
     */
    void set_payload_policy(PayloadPolicy policy);

    /**
     * Returns the current payload policy
     */
    PayloadPolicy get_payload_policy() const;

    /**
     * Produces a message
     *
     * \param topic The topic to write the message to
     * \param partition The partition to write the message to
     * \param payload The message payload
     */
    void produce(const MessageBuilder& builder);

    /**
     * \brief Polls on this handle
     *
     * This translates into a call to rd_kafka_poll.
     *
     * The timeout used on this call is the one configured via ProducerHandle::set_timeout.
     */
    int poll();

    /**
     * \brief Polls on this handle
     *
     * This translates into a call to rd_kafka_poll.
     *
     * \param timeout The timeout used on this call
     */
    int poll(std::chrono::milliseconds timeout);

    /**
     * \brief Flush all outstanding produce requests
     *
     * This translates into a call to rd_kafka_flush.
     *
     * The timeout used on this call is the one configured via ProducerHandle::set_timeout.
     */
    void flush();

    /**
     * \brief Flush all outstanding produce requests
     *
     * This translates into a call to rd_kafka_flush
     *
     * \param timeout The timeout used on this call
     */
    void flush(std::chrono::milliseconds timeout);
    
private:
    PayloadPolicy message_payload_policy_;
};

} // cppkafka

#include "impl/producer_impl.h"

#endif // CPPKAFKA_PRODUCER_H
