#include "exceptions.h"

using std::string;

namespace cppkafka {

// Exception

Exception::Exception(string message) 
: message_(move(message)) {

}

const char* Exception::what() const noexcept {
    return message_.data();
}

// ConfigException

ConfigException::ConfigException(const string& config_name, const string& error)
: Exception("Failed to set " + config_name + ": " + error) {

}

// ConfigOptionNotFound

ConfigOptionNotFound::ConfigOptionNotFound(const string& config_name) 
: Exception(config_name + " not found") {

}

// HandleException

HandleException::HandleException(rd_kafka_resp_err_t error_code) 
: Exception(rd_kafka_err2str(error_code)), error_code_(error_code) {

}

rd_kafka_resp_err_t HandleException::get_error_code() const {
    return error_code_;
}

} // cppkafka
