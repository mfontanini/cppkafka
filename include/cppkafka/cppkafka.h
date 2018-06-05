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

#ifndef CPPKAFKA_H
#define CPPKAFKA_H

#include <cppkafka/buffer.h>
#include <cppkafka/clonable_ptr.h>
#include <cppkafka/configuration.h>
#include <cppkafka/configuration_base.h>
#include <cppkafka/configuration_option.h>
#include <cppkafka/consumer.h>
#include <cppkafka/error.h>
#include <cppkafka/exceptions.h>
#include <cppkafka/group_information.h>
#include <cppkafka/kafka_handle_base.h>
#include <cppkafka/logging.h>
#include <cppkafka/macros.h>
#include <cppkafka/message.h>
#include <cppkafka/message_builder.h>
#include <cppkafka/message_internal.h>
#include <cppkafka/metadata.h>
#include <cppkafka/producer.h>
#include <cppkafka/queue.h>
#include <cppkafka/topic.h>
#include <cppkafka/topic_configuration.h>
#include <cppkafka/topic_partition.h>
#include <cppkafka/topic_partition_list.h>
#include <cppkafka/utils/backoff_committer.h>
#include <cppkafka/utils/backoff_performer.h>
#include <cppkafka/utils/buffered_producer.h>
#include <cppkafka/utils/compacted_topic_processor.h>
#include <cppkafka/utils/consumer_dispatcher.h>
#include <cppkafka/utils/poll_interface.h>
#include <cppkafka/utils/poll_strategy_base.h>
#include <cppkafka/utils/roundrobin_poll_strategy.h>

#endif
