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
#ifndef CPPKAFKA_COMPACTED_TOPIC_EVENT_IMPL_H
#define CPPKAFKA_COMPACTED_TOPIC_EVENT_IMPL_H

namespace cppkafka {

// CompactedTopicEvent

template <typename K, typename V>
CompactedTopicEvent<K, V>::CompactedTopicEvent(EventType type, std::string topic, int partition)
: type_(type), topic_(std::move(topic)), partition_(partition) {

}

template <typename K, typename V>
CompactedTopicEvent<K, V>::CompactedTopicEvent(EventType type, std::string topic, int partition,
                                               K key)
: type_(type), topic_(std::move(topic)), partition_(partition), key_(std::move(key)) {

}

template <typename K, typename V>
CompactedTopicEvent<K, V>::CompactedTopicEvent(EventType type, std::string topic, int partition,
                                               K key, V value)
: type_(type), topic_(std::move(topic)), partition_(partition), key_(std::move(key)),
  value_(std::move(value)) {

}

template <typename K, typename V>
typename CompactedTopicEvent<K, V>::EventType CompactedTopicEvent<K, V>::get_type() const {
    return type_;
}

template <typename K, typename V>
const std::string& CompactedTopicEvent<K, V>::get_topic() const {
    return topic_;
}

template <typename K, typename V>
int CompactedTopicEvent<K, V>::get_partition() const {
    return partition_;
}

template <typename K, typename V>
const K& CompactedTopicEvent<K, V>::get_key() const {
    return *key_;
}

template <typename K, typename V>
const V& CompactedTopicEvent<K, V>::get_value() const {
    return *value_;
}

} //namespace

#endif //CPPKAFKA_COMPACTED_TOPIC_EVENT_IMPL_H
