/*
 * Copyright (c) 2017, Matias Fontanini
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following disclaimer
 *   in the documentation and/or other materials provided with the
 *   distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#ifndef CPPKAFKA_CONFIGURATION_BASE_H
#define CPPKAFKA_CONFIGURATION_BASE_H

#include <string>
#include <map>
#include <vector>
#include <sstream>
#include "exceptions.h"
#include "configuration_option.h"

namespace cppkafka {

template <typename Concrete>
class ConfigBase {
private:
    template <typename T>
    struct Type2Type {};
    
public:
    virtual ~ConfigBase() = default;

    /**
     * Sets a std::string value
     */
    virtual Concrete& set(const std::string& name,
                          const std::string& value) = 0;
    
    /**
     * Sets a bool value
     */
    Concrete& set(const std::string& name, bool value) {
        return set(name, value ? std::string("true") : std::string("false"));
    }
    
    /**
     * Sets a value of any integral value.
     *
     * Valid value type: any integral type which is supported by rdkafka
     */
    template <typename T,
              typename = std::enable_if_t<is_rdkafka_type<T>::value>>
    Concrete& set(const std::string& name, T value) {
        return set(name, std::to_string(value));
    }

    /**
     * Sets a list of options
     */
    Concrete& set(const std::vector<ConfigurationOption>& options) {
        for (const auto& option : options) {
            set(option.get_key(), option.get_value());
        }
        return static_cast<Concrete&>(*this);
    }
    
    virtual std::string get(const std::string& name) const = 0;

    /**
     * \brief Gets a value, converting it to the given type.
     *
     * If the configuration option is not found, then ConfigOptionNotFound is thrown.
     *
     * If the configuration value can't be converted to the given type, then 
     * InvalidConfigOptionType is thrown.
     *
     * Valid conversion types: any integral type which is supported by rdkafka
     */
    template <typename T,
              typename = std::enable_if_t<is_rdkafka_type<T>::value>>
    T get(const std::string& name) const {
        std::string value = static_cast<const Concrete&>(*this).get(name);
        return convert(value, Type2Type<T>());
    }
    
protected:
    static std::map<std::string, std::string> parse_dump(const char** values, size_t count) {
        std::map<std::string, std::string> output;
        for (size_t i = 0; i < count; i += 2) {
            output[values[i]] = values[i + 1];
        }
        return output;
    }
private:
    static std::string convert(const std::string& value, Type2Type<std::string>) {
        return value;
    }

    static bool convert(const std::string& value, Type2Type<bool>) {
        if (value == "true") {
            return true;
        }
        else if (value == "false") {
            return false;
        }
        else {
            throw InvalidConfigOptionType(value, "bool");
        }
    }

    template <typename T>
    static int convert(const std::string& value, Type2Type<T>) {
        std::istringstream ss(value);
        T ret;
        ss >> ret;
        if (ss.fail()) {
            throw InvalidConfigOptionType(value, "invalid type");
        }
        return ret;
    }
};

} // cppkafka

#endif // CPPKAFKA_CONFIGURATION_BASE_H
