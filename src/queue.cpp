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
#include "queue.h"
#include "exceptions.h"

using std::vector;
using std::exception;
using std::chrono::milliseconds;

namespace cppkafka {

void dummy_deleter(rd_kafka_queue_t*) {

}

const milliseconds Queue::DEFAULT_TIMEOUT{1000};

Queue Queue::make_non_owning(rd_kafka_queue_t* handle) {
    return Queue(handle, NonOwningTag{});
}

Queue::Queue()
: handle_(nullptr, nullptr),
  timeout_ms_(DEFAULT_TIMEOUT) {

}

Queue::Queue(rd_kafka_queue_t* handle)
: handle_(handle, &rd_kafka_queue_destroy),
  timeout_ms_(DEFAULT_TIMEOUT) {

}

Queue::Queue(rd_kafka_queue_t* handle, NonOwningTag)
: handle_(handle, &dummy_deleter) {

}

rd_kafka_queue_t* Queue::get_handle() const {
    return handle_.get();
}

size_t Queue::get_length() const {
    return rd_kafka_queue_length(handle_.get());
}

void Queue::forward_to_queue(const Queue& forward_queue) const {
    return rd_kafka_queue_forward(handle_.get(), forward_queue.handle_.get());
}

void Queue::disable_queue_forwarding() const {
    return rd_kafka_queue_forward(handle_.get(), nullptr);
}

void Queue::set_timeout(milliseconds timeout) {
    timeout_ms_ = timeout;
}

milliseconds Queue::get_timeout() const {
    return timeout_ms_;
}

Message Queue::consume() const {
    return consume(timeout_ms_);
}

Message Queue::consume(milliseconds timeout) const {
    return Message(rd_kafka_consume_queue(handle_.get(), static_cast<int>(timeout.count())));
}

MessageList Queue::consume_batch(size_t max_batch_size) const {
    return consume_batch(max_batch_size, timeout_ms_);
}

MessageList Queue::consume_batch(size_t max_batch_size, milliseconds timeout) const {
    vector<rd_kafka_message_t*> raw_messages(max_batch_size);
    ssize_t result = rd_kafka_consume_batch_queue(handle_.get(),
                                                  static_cast<int>(timeout.count()),
                                                  raw_messages.data(),
                                                  raw_messages.size());
    if (result == -1) {
        rd_kafka_resp_err_t error = rd_kafka_last_error();
        if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw QueueException(error);
        }
        return MessageList();
    }
    // Build message list
    return MessageList(raw_messages.begin(), raw_messages.begin() + result);
}

} //cppkafka
