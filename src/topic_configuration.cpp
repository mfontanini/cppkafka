#include "topic_configuration.h"
#include <vector>
#include <librdkafka/rdkafka.h>
#include "exceptions.h"
#include "topic.h"
#include "buffer.h"

using std::string;
using std::vector;

namespace cppkafka {

int32_t partitioner_callback_proxy(const rd_kafka_topic_t* handle, const void *key_ptr,
                                   size_t key_size, int32_t partition_count,
                                   void* topic_opaque, void* message_opaque) {
    const TopicConfiguration* config = static_cast<TopicConfiguration*>(topic_opaque);
    const auto& callback = config->get_partitioner_callback();
    if (callback) {
        Topic topic = Topic::make_non_owning(const_cast<rd_kafka_topic_t*>(handle));
        Buffer key(static_cast<const char*>(key_ptr), key_size);
        return callback(topic, key, partition_count);
    }
    else {
        return rd_kafka_msg_partitioner_consistent_random(handle, key_ptr, key_size, 
                                                          partition_count, topic_opaque,
                                                          message_opaque);
    }
}

TopicConfiguration::TopicConfiguration() 
: handle_(make_handle(rd_kafka_topic_conf_new())) {

}

TopicConfiguration::TopicConfiguration(rd_kafka_topic_conf_t* ptr) 
: handle_(make_handle(ptr)) {

}

void TopicConfiguration::set(const string& name, const string& value) {
    char error_buffer[512];
    rd_kafka_conf_res_t result;
    result = rd_kafka_topic_conf_set(handle_.get(), name.data(), value.data(), error_buffer,
                                     sizeof(error_buffer));
    if (result != RD_KAFKA_CONF_OK) {
        throw ConfigException(name, error_buffer);
    }
}

void TopicConfiguration::set_partitioner_callback(PartitionerCallback callback) {
    partitioner_callback_ = move(callback);
    rd_kafka_topic_conf_set_partitioner_cb(handle_.get(), &partitioner_callback_proxy);
}

void TopicConfiguration::set_as_opaque() {
    rd_kafka_topic_conf_set_opaque(handle_.get(), this);
}

const TopicConfiguration::PartitionerCallback&
TopicConfiguration::get_partitioner_callback() const {
    return partitioner_callback_;
}

rd_kafka_topic_conf_t* TopicConfiguration::get_handle() const {
    return handle_.get();
}

string TopicConfiguration::get(const string& name) const {
    size_t size = 0;
    auto result = rd_kafka_topic_conf_get(handle_.get(), name.data(), nullptr, &size);
    if (result != RD_KAFKA_CONF_OK) {
        throw ConfigOptionNotFound(name);
    }
    vector<char> buffer(size);
    rd_kafka_topic_conf_get(handle_.get(), name.data(), buffer.data(), &size);
    return string(buffer.data());
}

TopicConfiguration::HandlePtr TopicConfiguration::make_handle(rd_kafka_topic_conf_t* ptr) {
    return HandlePtr(ptr, &rd_kafka_topic_conf_destroy, &rd_kafka_topic_conf_dup);
}     

} // cppkafka
