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

#include <iostream>
#include <string>
#include "topic_partition_list.h"
#include "topic_partition.h"
#include "exceptions.h"

using std::vector;
using std::ostream;
using std::string;

namespace cppkafka {

TopicPartitionsListPtr convert(const TopicPartitionList& topic_partitions) {
    TopicPartitionsListPtr handle(rd_kafka_topic_partition_list_new(topic_partitions.size()),
                                  &rd_kafka_topic_partition_list_destroy);
    for (const auto& item : topic_partitions) {
        rd_kafka_topic_partition_t* new_item = nullptr;
        new_item = rd_kafka_topic_partition_list_add(handle.get(),
                                                     item.get_topic().data(),
                                                     item.get_partition());
        new_item->offset = item.get_offset();
    }
    return handle;
}

TopicPartitionList convert(const TopicPartitionsListPtr& topic_partitions) {
    return convert(topic_partitions.get());
}

TopicPartitionList convert(rd_kafka_topic_partition_list_t* topic_partitions) {
    TopicPartitionList output;
    for (int i = 0; i < topic_partitions->cnt; ++i) {
        const auto& elem = topic_partitions->elems[i];
        output.emplace_back(elem.topic, elem.partition, elem.offset);
    }
    return output;
}

TopicPartitionsListPtr make_handle(rd_kafka_topic_partition_list_t* handle) {
    return TopicPartitionsListPtr(handle, &rd_kafka_topic_partition_list_destroy);
}

TopicPartitionList make_subset(const TopicPartitionList& partitions,
                               const vector<string>& topics) {
    vector<bool> skip(partitions.size(), false);
    TopicPartitionList subset;
    for (const auto& topic : topics) {
        for (size_t i = 0; i < partitions.size(); ++i) {
            if (!skip[i] && (topic.size() == partitions[i].get_topic().size())) {
                // compare both strings
                bool match = equal(topic.begin(), topic.end(), partitions[i].get_topic().begin(),
                                   [](char c1, char c2)->bool {
                    return toupper(c1) == toupper(c2);
                });
                if (match) {
                    skip[i] = true;
                    subset.emplace_back(partitions[i]);
                }
            }
        }
    }
    return subset;
}

TopicPartitionList make_subset(const TopicPartitionList& partitions,
                               const vector<int>& ids) {
    TopicPartitionList subset;
    for (const auto& id : ids) {
        for (const auto& partition : partitions) {
            // compare both partition ids
            if (id == partition.get_partition()) {
                subset.emplace_back(partition);
            }
        }
    }
    return subset;
}

ostream& operator<<(ostream& output, const TopicPartitionList& rhs) {
    output << "[ ";
    for (auto iter = rhs.begin(); iter != rhs.end(); ++iter) {
        if (iter != rhs.begin()) {
            output << ", ";
        }
        output << *iter;
    }
    output << " ]";
    return output;
}

} // cppkafka
