#ifndef CPPKAFKA_EXCEPTIONS_H
#define CPPKAFKA_EXCEPTIONS_H

#include <stdexcept>
#include <string>

namespace cppkafka {

class KafkaException : public std::exception {

};

class KafkaConfigException : public KafkaException {
public:
    KafkaConfigException(const std::string& config_name, const std::string& error);

    const char* what() const noexcept;
private:
    std::string message_;
};

} // cppkafka

#endif // CPPKAFKA_EXCEPTIONS_H
