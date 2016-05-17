#include "kafka_handle_base.h"
#include "exceptions.h"
#include "topic_configuration.h"
#include "topic.h"

using std::string;

namespace cppkafka {

KafkaHandleBase::KafkaHandleBase() 
: handle_(nullptr, nullptr) {

}

KafkaHandleBase::KafkaHandleBase(rd_kafka_t* handle) 
: handle_(handle, &rd_kafka_destroy) {

}

void KafkaHandleBase::set_handle(rd_kafka_t* handle) {
    handle_ = HandlePtr(handle, &rd_kafka_destroy);
}

rd_kafka_t* KafkaHandleBase::get_handle() {
    return handle_.get();
}

Topic KafkaHandleBase::get_topic(const string& name) {
    rd_kafka_topic_t* topic = rd_kafka_topic_new(handle_.get(), name.data(), nullptr);
    if (!topic) {
        throw Exception("Failed to create topic handle");
    }
    return Topic(topic);
}

Topic KafkaHandleBase::get_topic(const string& name, const TopicConfiguration& config) {
    rd_kafka_topic_t* topic = rd_kafka_topic_new(handle_.get(), name.data(),
                                                 config.get_handle());
    if (!topic) {
        throw Exception("Failed to create topic handle");
    }
    return Topic(topic);
}

} // cppkafka
