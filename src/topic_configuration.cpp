#include "topic_configuration.h"
#include <librdkafka/rdkafka.h>
#include "exceptions.h"

using std::string;

namespace cppkafka {

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

rd_kafka_topic_conf_t* TopicConfiguration::get_handle() const {
    return handle_.get();
}

TopicConfiguration::HandlePtr TopicConfiguration::make_handle(rd_kafka_topic_conf_t* ptr) {
    return HandlePtr(ptr, &rd_kafka_topic_conf_destroy, &rd_kafka_topic_conf_dup);
}     

} // cppkafka
