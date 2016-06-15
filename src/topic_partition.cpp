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

#include <iostream>
#include <librdkafka/rdkafka.h>
#include "topic_partition.h"

using std::string;
using std::ostream;

namespace cppkafka {

TopicPartition::TopicPartition() 
: TopicPartition("") {

}

TopicPartition::TopicPartition(const char* topic) 
: TopicPartition(string(topic)) {

}

TopicPartition::TopicPartition(string topic) 
: TopicPartition(move(topic), RD_KAFKA_PARTITION_UA) {

}

TopicPartition::TopicPartition(string topic, int partition) 
: TopicPartition(move(topic), partition, RD_KAFKA_OFFSET_INVALID) {

}

TopicPartition::TopicPartition(string topic, int partition, int64_t offset) 
: topic_(move(topic)), partition_(partition), offset_(offset) {

}

const string& TopicPartition::get_topic() const {
    return topic_;
}

int TopicPartition::get_partition() const {
    return partition_;
}

int64_t TopicPartition::get_offset() const {
    return offset_;
}

ostream& operator<<(ostream& output, const TopicPartition& rhs) {
    return output << rhs.get_topic() << "[" << rhs.get_partition() << "]";
}

} // cppkafka
