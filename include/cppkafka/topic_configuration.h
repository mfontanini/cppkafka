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

#ifndef CPPKAFKA_TOPIC_CONFIGURATION_H
#define CPPKAFKA_TOPIC_CONFIGURATION_H

#include <string>
#include <functional>
#include <librdkafka/rdkafka.h>
#include "clonable_ptr.h"
#include "configuration_base.h"

namespace cppkafka {

class Topic;
class Buffer;

class TopicConfiguration : public ConfigurationBase<TopicConfiguration> {
public:
    using PartitionerCallback = std::function<int32_t(const Topic&, const Buffer& key,
                                                      int32_t partition_count)>;

    using ConfigurationBase<TopicConfiguration>::set;

    TopicConfiguration();

    void set(const std::string& name, const std::string& value);

    void set_partitioner_callback(PartitionerCallback callback);

    void set_as_opaque();

    const PartitionerCallback& get_partitioner_callback() const;
    rd_kafka_topic_conf_t* get_handle() const;
    std::string get(const std::string& name) const;
private:
    using HandlePtr = ClonablePtr<rd_kafka_topic_conf_t,
                                  decltype(&rd_kafka_topic_conf_destroy),
                                  decltype(&rd_kafka_topic_conf_dup)>;

    TopicConfiguration(rd_kafka_topic_conf_t* ptr);
    static HandlePtr make_handle(rd_kafka_topic_conf_t* ptr);

    HandlePtr handle_;
    PartitionerCallback partitioner_callback_;
};

} // cppkafka

#endif // CPPKAFKA_TOPIC_CONFIGURATION_H
