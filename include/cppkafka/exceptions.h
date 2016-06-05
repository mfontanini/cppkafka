#ifndef CPPKAFKA_EXCEPTIONS_H
#define CPPKAFKA_EXCEPTIONS_H

#include <stdexcept>
#include <string>
#include <librdkafka/rdkafka.h>

namespace cppkafka {

class Exception : public std::exception {
public:
    Exception(std::string message);

    const char* what() const noexcept;
private:
    std::string message_;
};

class ConfigException : public Exception {
public:
    ConfigException(const std::string& config_name, const std::string& error);
};

class ConfigOptionNotFound : public Exception {
public:
    ConfigOptionNotFound(const std::string& config_name);
};

class HandleException : public Exception {
public:
    HandleException(rd_kafka_resp_err_t error_code);

    rd_kafka_resp_err_t get_error_code() const;
private:
    rd_kafka_resp_err_t error_code_;
};

} // cppkafka

#endif // CPPKAFKA_EXCEPTIONS_H
