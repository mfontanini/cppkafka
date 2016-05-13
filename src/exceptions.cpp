#include "exceptions.h"

using std::string;

namespace cppkafka {

KafkaConfigException::KafkaConfigException(const string& config_name,
                                           const string& error)
: message_("Failed to set " + config_name + ": " + error) {
}

const char* KafkaConfigException::what() const noexcept {
    return message_.data();
}

} // cppkafka
