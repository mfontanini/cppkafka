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

#include "message.h"
#include "message_internal.h"

using std::chrono::milliseconds;

namespace cppkafka {

void dummy_deleter(rd_kafka_message_t*) {

}

Message Message::make_non_owning(rd_kafka_message_t* handle) {
    return Message(handle, NonOwningTag());
}

Message::Message()
: handle_(nullptr, nullptr),
  user_data_(nullptr) {

}

Message::Message(rd_kafka_message_t* handle) 
: Message(HandlePtr(handle, &rd_kafka_message_destroy)) {

}

Message::Message(rd_kafka_message_t* handle, NonOwningTag)
: Message(HandlePtr(handle, &dummy_deleter)) {

}

Message::Message(HandlePtr handle)
: handle_(move(handle)),
  payload_(handle_ ? Buffer((const Buffer::DataType*)handle_->payload, handle_->len) : Buffer()),
  key_(handle_ ? Buffer((const Buffer::DataType*)handle_->key, handle_->key_len) : Buffer()),
  user_data_(handle_ ? handle_->_private : nullptr) {
}

Message& Message::load_internal() {
    if (user_data_) {
        MessageInternal* mi = static_cast<MessageInternal*>(user_data_);
        user_data_ = mi->get_user_data();
        internal_ = mi->get_internal();
    }
    return *this;
}

// MessageTimestamp

MessageTimestamp::MessageTimestamp(milliseconds timestamp, TimestampType type)
: timestamp_(timestamp), type_(type) {

}

milliseconds MessageTimestamp::get_timestamp() const {
    return timestamp_;
}

MessageTimestamp::TimestampType MessageTimestamp::get_type() const {
    return type_;
}

} // cppkafka
