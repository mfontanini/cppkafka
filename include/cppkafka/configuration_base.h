#ifndef CPPKAFKA_CONFIGURATION_BASE_H
#define CPPKAFKA_CONFIGURATION_BASE_H

#include <string>

namespace cppkafka {

template <typename Concrete>
class ConfigurationBase {
public:
    void set(const std::string& name, bool value) {
        proxy_set(name, value ? "true" : "false");
    }

    // Overload for any integral value
    template <typename T,
              typename = typename std::enable_if<std::is_integral<T>::value>::type>
    void set(const std::string& name, T value) {
        proxy_set(name, std::to_string(value));
    }

    void set(const std::string& name, const char* value) {
        proxy_set(name, value);
    }
private:
    void proxy_set(const std::string& name, const std::string& value) {
        static_cast<Concrete&>(*this).set(name, value);
    }
};

} // cppkafka

#endif // CPPKAFKA_CONFIGURATION_BASE_H
