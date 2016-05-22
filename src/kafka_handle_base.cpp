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

rd_kafka_t* KafkaHandleBase::get_handle() {
    return handle_.get();
}

Topic KafkaHandleBase::get_topic(const string& name) {
    return get_topic(name, nullptr);
}

Topic KafkaHandleBase::get_topic(const string& name, TopicConfiguration topicConfig) {
    return get_topic(name, topicConfig.get_handle());
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

void KafkaHandleBase::check_error(rd_kafka_resp_err_t error) {
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw HandleException(error);
    }
}

} // cppkafka
