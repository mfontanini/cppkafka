#ifndef CPPKAFKA_CONFIGURATION_H
#define CPPKAFKA_CONFIGURATION_H

#include <memory>
#include <string>
#include <librdkafka/rdkafka.h>

namespace cppkafka {

class Configuration {
public:
    Configuration();
    Configuration(const Configuration& rhs);
    Configuration(Configuration&& rhs) noexcept = default;
    Configuration& operator=(const Configuration& rhs);
    Configuration& operator=(Configuration&& rhs) noexcept = default;

    void set(const std::string& name, const std::string& value);
private:
    using HandlePtr = std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)>;

    Configuration(rd_kafka_conf_t* ptr);
    static HandlePtr make_handle(rd_kafka_conf_t* ptr);

    HandlePtr handle_;
};

} // cppkafka

#endif // CPPKAFKA_CONFIGURATION_H
