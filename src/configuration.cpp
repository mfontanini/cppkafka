#include "configuration.h"
#include <librdkafka/rdkafka.h>
#include "exceptions.h"
#include "message.h"
#include "producer.h"
#include "consumer.h"

using std::string;

namespace cppkafka {

// Callback proxies

void delivery_report_proxy(rd_kafka_t *rk, const rd_kafka_message_t* msg, void *opaque) {
    const Producer* producer = static_cast<const Producer*>(opaque);
    Message message = Message::make_non_owning((rd_kafka_message_t*)msg);
    const auto& callback = producer->get_configuration().get_delivery_report_callback();
    if (callback) {
        callback(message);
    }
}

void offset_commit_proxy(rd_kafka_t *rk,
                         rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *offsets,
                         void *opaque) {
    const Consumer* consumer = static_cast<const Consumer*>(opaque);
    TopicPartitionList list = offsets ? convert(offsets) : TopicPartitionList{};
    const auto& callback = consumer->get_configuration().get_offset_commit_callback();
    if (callback) {
        callback(err, list);
    }
}

// Configuration

Configuration::Configuration() 
: handle_(make_handle(rd_kafka_conf_new())) {

}

Configuration::Configuration(rd_kafka_conf_t* ptr) 
: handle_(make_handle(ptr)) {

}

void Configuration::set(const string& name, const string& value) {
    char error_buffer[512];
    rd_kafka_conf_res_t result;
    result = rd_kafka_conf_set(handle_.get(), name.data(), value.data(), error_buffer,
                               sizeof(error_buffer));
    if (result != RD_KAFKA_CONF_OK) {
        throw ConfigException(name, error_buffer);
    }
}

void Configuration::set_delivery_report_callback(DeliveryReportCallback callback) {
    delivery_report_callback_ = move(callback);
    rd_kafka_conf_set_dr_msg_cb(handle_.get(), &delivery_report_proxy);
}

void Configuration::set_offset_commit_callback(OffsetCommitCallback callback) {
    offset_commit_callback_ = move(callback);
    rd_kafka_conf_set_offset_commit_cb(handle_.get(), &offset_commit_proxy);
}

rd_kafka_conf_t* Configuration::get_handle() const {
    return handle_.get();
}

const Configuration::DeliveryReportCallback& Configuration::get_delivery_report_callback() const {
    return delivery_report_callback_;
}

const Configuration::OffsetCommitCallback& Configuration::get_offset_commit_callback() const {
    return offset_commit_callback_;
}

Configuration::HandlePtr Configuration::make_handle(rd_kafka_conf_t* ptr) {
    return HandlePtr(ptr, &rd_kafka_conf_destroy, &rd_kafka_conf_dup);
}     

} // cppkafka
