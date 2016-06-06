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

#ifndef CPPKAFKA_TOPIC_PARTITION_LIST_H
#define CPPKAFKA_TOPIC_PARTITION_LIST_H

#include <memory>
#include <algorithm>
#include <librdkafka/rdkafka.h>

namespace cppkafka {

class TopicPartition;

using TopicPartitionsListPtr = std::unique_ptr<rd_kafka_topic_partition_list_t, 
                                               decltype(&rd_kafka_topic_partition_list_destroy)>;
using TopicPartitionList = std::vector<TopicPartition>;

TopicPartitionsListPtr convert(const std::vector<TopicPartition>& topic_partitions);
std::vector<TopicPartition> convert(const TopicPartitionsListPtr& topic_partitions);
std::vector<TopicPartition> convert(rd_kafka_topic_partition_list_t* topic_partitions);
TopicPartitionsListPtr make_handle(rd_kafka_topic_partition_list_t* handle);

} // cppkafka

#endif // CPPKAFKA_TOPIC_PARTITION_LIST_H
