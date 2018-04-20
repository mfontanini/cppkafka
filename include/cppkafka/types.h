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

#ifndef CPPKAFKA_TYPES_H
#define CPPKAFKA_TYPES_H

#include "traits.h"

namespace cppkafka {

template <typename T> class BasicConsumerDispatcher;
template <typename T> class BasicBackoffCommitter;
template <typename Key, typename Value, typename ConsumerType> class CompactedTopicProcessor;
template <typename BufferType, typename ProducerType> class BufferedProducer;

//=============================================================================
//                             TYPEDEFS
//=============================================================================
//------------------------- Traits TYPES --------------------------------------
using GenericTraits                 = HandleTraits<GenericType>;
using ConsumerTraits                = HandleTraits<ConsumerType>;
using ProducerTraits                = HandleTraits<ProducerType>;

//------------------------- CONSUMER TYPES ------------------------------------
using KafkaConsumer                 = ConsumerHandle<ConsumerTraits>;
using KafkaConsumerConfig           = HandleConfig<ConsumerTraits>;
using KafkaConsumerTopicConfig      = TopicConfig<ConsumerTraits>;
using KafkaConsumerDispatcher       = BasicConsumerDispatcher<KafkaConsumer>;
using KafkaBackoffCommitter         = BasicBackoffCommitter<KafkaConsumer>;
template <typename Key, typename Value>
using KafkaCompactedTopicProcessor  = CompactedTopicProcessor<Key, Value, KafkaConsumer>;

//------------------------- PRODUCER TYPES ------------------------------------
using KafkaProducer                 = ProducerHandle<ProducerTraits>;
using KafkaProducerConfig           = HandleConfig<ProducerTraits>;
using KafkaProducerTopicConfig      = TopicConfig<ProducerTraits>;
template <typename BufferType>
using KafkaBufferedProducer         = BufferedProducer<BufferType, KafkaProducer>;

//------------------------- LEGACY TYPES --------------------------------------
using Consumer                      = ConsumerHandle<GenericTraits>;
using Producer                      = ProducerHandle<GenericTraits>;
using Configuration                 = HandleConfig<GenericTraits>;
using TopicConfiguration            = TopicConfig<GenericTraits>;
using ConsumerDispatcher            = BasicConsumerDispatcher<Consumer>;
using BackoffCommitter              = BasicBackoffCommitter<Consumer>;

} //namespace

#endif //CPPKAFKA_TYPES_H
