#include "configuration.h"
#include <librdkafka/rdkafka.h>
#include "exceptions.h"

using std::string;

namespace cppkafka {

Configuration::Configuration() 
: handle_(make_handle(rd_kafka_conf_new())) {

}

Configuration::Configuration(rd_kafka_conf_t* ptr) 
: handle_(make_handle(ptr)) {

}

Configuration::Configuration(const Configuration& rhs) 
: handle_(make_handle(rd_kafka_conf_dup(rhs.handle_.get()))) {

}

Configuration& Configuration::operator=(const Configuration& rhs) {
    handle_.reset(rd_kafka_conf_dup(rhs.handle_.get()));
    return *this;
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

rd_kafka_conf_t* Configuration::get_handle() const {
    return handle_.get();
}

Configuration::HandlePtr Configuration::make_handle(rd_kafka_conf_t* ptr) {
    return HandlePtr(ptr, &rd_kafka_conf_destroy);
}     

} // cppkafka
