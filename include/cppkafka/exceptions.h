/*
 * Copyright (c) 2016, Matias Fontanini
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
