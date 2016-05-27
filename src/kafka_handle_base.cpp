#include "kafka_handle_base.h"
#include "exceptions.h"
#include "topic_configuration.h"
#include "topic.h"

using std::string;
using std::chrono::milliseconds;

namespace cppkafka {

const milliseconds KafkaHandleBase::DEFAULT_TIMEOUT{1000};

KafkaHandleBase::KafkaHandleBase() 
: handle_(nullptr, nullptr), timeout_ms_(DEFAULT_TIMEOUT) {

}

KafkaHandleBase::KafkaHandleBase(rd_kafka_t* handle) 
: handle_(handle, &rd_kafka_destroy), timeout_ms_(DEFAULT_TIMEOUT) {

}

void KafkaHandleBase::pause_partitions(const TopicPartitionList& topic_partitions) {
    rd_kafka_resp_err_t error = rd_kafka_pause_partitions(get_handle(), 
                                                          topic_partitions.get_handle());
    check_error(error);
}

void KafkaHandleBase::resume_partitions(const TopicPartitionList& topic_partitions) {
    rd_kafka_resp_err_t error = rd_kafka_resume_partitions(get_handle(), 
                                                           topic_partitions.get_handle());
    check_error(error);
}

void KafkaHandleBase::set_timeout(const milliseconds& timeout) {
    timeout_ms_ = timeout;
}

rd_kafka_t* KafkaHandleBase::get_handle() {
    return handle_.get();
}

Topic KafkaHandleBase::get_topic(const string& name) {
    return get_topic(name, nullptr);
}

Topic KafkaHandleBase::get_topic(const string& name, TopicConfiguration topicConfig) {
    return get_topic(name, topicConfig.get_handle());
}

Metadata KafkaHandleBase::get_metadata() {
    return get_metadata(nullptr);
}

Metadata KafkaHandleBase::get_metadata(const Topic& topic) {
    return get_metadata(topic.get_handle());
}

milliseconds KafkaHandleBase::get_timeout() const {
    return timeout_ms_;
}

void KafkaHandleBase::set_handle(rd_kafka_t* handle) {
    handle_ = HandlePtr(handle, &rd_kafka_destroy);
}

Topic KafkaHandleBase::get_topic(const string& name, rd_kafka_topic_conf_t* conf) {
    rd_kafka_topic_t* topic = rd_kafka_topic_new(get_handle(), name.data(), conf);
    if (!topic) {
        throw HandleException(rd_kafka_errno2err(errno));
    }
    return Topic(topic);
}

Metadata KafkaHandleBase::get_metadata(rd_kafka_topic_t* topic_ptr) {
    const rd_kafka_metadata_t* metadata;
    rd_kafka_resp_err_t error = rd_kafka_metadata(get_handle(), topic_ptr != nullptr, 
                                                  topic_ptr, &metadata, timeout_ms_.count());
    check_error(error);
    return Metadata(metadata);
}

void KafkaHandleBase::check_error(rd_kafka_resp_err_t error) {
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw HandleException(error);
    }
}

} // cppkafka
