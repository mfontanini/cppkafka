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
#ifndef CPPKAFKA_COMPACTED_TOPIC_PROCESSOR_IMPL_H
#define CPPKAFKA_COMPACTED_TOPIC_PROCESSOR_IMPL_H

namespace cppkafka {

// CompactedTopicProcessor

template <typename K, typename V, typename ConsumerType>
CompactedTopicProcessor<K, V, ConsumerType>::CompactedTopicProcessor(ConsumerType& consumer)
: consumer_(consumer) {
    // Save the current assignment callback and assign ours
    original_assignment_callback_ = consumer_.get_assignment_callback();
    consumer_.set_assignment_callback([&](TopicPartitionList& topic_partitions) {
        on_assignment(topic_partitions);
    });
}

template <typename K, typename V, typename ConsumerType>
CompactedTopicProcessor<K, V, ConsumerType>::~CompactedTopicProcessor() {
    // Restore previous assignment callback
    consumer_.set_assignment_callback(original_assignment_callback_);
}

template <typename K, typename V, typename ConsumerType>
void CompactedTopicProcessor<K, V, ConsumerType>::set_key_decoder(KeyDecoder callback) {
    key_decoder_ = std::move(callback);
}

template <typename K, typename V, typename ConsumerType>
void CompactedTopicProcessor<K, V, ConsumerType>::set_value_decoder(ValueDecoder callback) {
    value_decoder_ = std::move(callback);
}

template <typename K, typename V, typename ConsumerType>
void CompactedTopicProcessor<K, V, ConsumerType>::set_event_handler(EventHandler callback) {
    event_handler_ = std::move(callback);
}

template <typename K, typename V, typename ConsumerType>
void CompactedTopicProcessor<K, V, ConsumerType>::set_error_handler(ErrorHandler callback) {
    error_handler_ = std::move(callback);
}

template <typename Key, typename Value, typename ConsumerType>
void CompactedTopicProcessor<Key, Value, ConsumerType>::process_event() {
    Message message = consumer_.poll();
    if (message) {
        if (!message.get_error()) {
            boost::optional<Key> key = key_decoder_(message.get_key());
            if (key) {
                if (message.get_payload()) {
                    boost::optional<Value> value = value_decoder_(*key, message.get_payload());
                    if (value) {
                        // If there's a payload and we managed to parse the value, generate a
                        // SET_ELEMENT event
                        event_handler_({ Event::SET_ELEMENT, message.get_topic(),
                                         message.get_partition(), *key, std::move(*value) });
                    }
                }
                else {
                    // No payload, generate a DELETE_ELEMENT event
                    event_handler_({ Event::DELETE_ELEMENT, message.get_topic(),
                                     message.get_partition(), *key });
                }
            }
            // Store the offset for this topic/partition
            TopicPartition topic_partition(message.get_topic(), message.get_partition());
            partition_offsets_[topic_partition] = message.get_offset();
        }
        else {
            if (message.is_eof()) {
                event_handler_({ Event::REACHED_EOF, message.get_topic(),
                                 message.get_partition() });
            }
            else if (error_handler_) {
                error_handler_(std::move(message));
            }
        }
    }
}

template <typename K, typename V, typename ConsumerType>
void CompactedTopicProcessor<K, V, ConsumerType>::on_assignment(TopicPartitionList& topic_partitions) {
    if (original_assignment_callback_) {
        original_assignment_callback_(topic_partitions);
    }
    std::set<TopicPartition> partitions_found;
    // See if we already had an assignment for any of these topic/partitions. If we do,
    // then restore the offset following the last one we saw
    for (TopicPartition& topic_partition : topic_partitions) {
        auto iter = partition_offsets_.find(topic_partition);
        if (iter != partition_offsets_.end()) {
            topic_partition.set_offset(iter->second);
        }
        // Populate this set
        partitions_found.insert(topic_partition);
    }
    // Clear our cache: remove any entries for topic/partitions that aren't assigned to us now.
    // Emit a CLEAR_ELEMENTS event for each topic/partition that is gone
    auto iter = partition_offsets_.begin();
    while (iter != partition_offsets_.end()) {
        const TopicPartition& topic_partition = iter->first;
        if (partitions_found.count(topic_partition) == 0) {
            event_handler_({ Event::CLEAR_ELEMENTS, topic_partition.get_topic(),
                             topic_partition.get_partition() });
            iter = partition_offsets_.erase(iter);
        }
        else {
            ++iter;
        }
    }
}

}

#endif //CPPKAFKA_COMPACTED_TOPIC_PROCESSOR_IMPL_H
