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

#include "message.h"

using std::string;

using boost::optional;
using boost::none_t;

namespace cppkafka {

void dummy_deleter(rd_kafka_message_t*) {

}

Message Message::make_non_owning(rd_kafka_message_t* handle) {
    return Message(handle, NonOwningTag());
}

Message::Message() 
: handle_(nullptr, nullptr) {

}

Message::Message(rd_kafka_message_t* handle) 
: Message(HandlePtr(handle, &rd_kafka_message_destroy)) {

}

Message::Message(rd_kafka_message_t* handle, NonOwningTag) 
: Message(HandlePtr(handle, &dummy_deleter)) {

}

Message::Message(HandlePtr handle) 
: handle_(move(handle)), 
  payload_((const Buffer::DataType*)handle_->payload, handle_->len),
  key_((const Buffer::DataType*)handle_->key, handle_->key_len) {

}

bool Message::has_error() const {
    return get_error() != RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t Message::get_error() const {
    return handle_->err;
}

string Message::get_error_string() const {
    return rd_kafka_err2str(handle_->err);
}

bool Message::is_eof() const {
    return get_error() == RD_KAFKA_RESP_ERR__PARTITION_EOF;
}

int Message::get_partition() const {
    return handle_->partition;
}

string Message::get_topic() const {
    return rd_kafka_topic_name(handle_->rkt);
}

const Buffer& Message::get_payload() const {
    return payload_;
}

const Buffer& Message::get_key() const {
    return key_;
}

int64_t Message::get_offset() const {
    return handle_->offset;
}

void* Message::get_private_data() const {
    return handle_->_private;
}

optional<MessageTimestamp> Message::get_timestamp() const {
    rd_kafka_timestamp_type_t type = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
    int64_t timestamp = rd_kafka_message_timestamp(handle_.get(), &type);
    if (timestamp == -1 || type == RD_KAFKA_TIMESTAMP_NOT_AVAILABLE) {
        return {};
    }
    return MessageTimestamp(timestamp, static_cast<MessageTimestamp::TimestampType>(type));
}

Message::operator bool() const {
    return handle_ != nullptr;
}

rd_kafka_message_t* Message::get_handle() const {
    return handle_.get();
}

// MessageTimestamp

MessageTimestamp::MessageTimestamp(int64_t timestamp, TimestampType type)
: timestamp_(timestamp), type_(type) {

}

int64_t MessageTimestamp::get_timestamp() const {
    return timestamp_;
}

MessageTimestamp::TimestampType MessageTimestamp::get_type() const {
    return type_;
}

} // cppkafka
