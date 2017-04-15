#include "message_builder.h"
#include <boost/utility/typed_in_place_factory.hpp>

namespace cppkafka {

MessageBuilder::MessageBuilder(const Topic& topic)
: topic_(topic) {
}

MessageBuilder& MessageBuilder::partition(Partition value) {
    partition_ = value;
    return *this;
}

MessageBuilder& MessageBuilder::key(const Buffer& value) {
    key_ = Buffer(value.get_data(), value.get_size());
    return *this;
}

MessageBuilder& MessageBuilder::payload(const Buffer& value) {
    payload_ = Buffer(value.get_data(), value.get_size());
    return *this;
}

MessageBuilder& MessageBuilder::user_data(void* value) {
    user_data_ = value;
    return *this;
}

const Topic& MessageBuilder::topic() const {
    return topic_;
}

const Partition& MessageBuilder::partition() const {
    return partition_;
}

const Buffer& MessageBuilder::key() const {
    return key_;
}

const Buffer& MessageBuilder::payload() const {
    return payload_;
}

void* MessageBuilder::user_data() const {
    return user_data_;
}

} // cppkafka
