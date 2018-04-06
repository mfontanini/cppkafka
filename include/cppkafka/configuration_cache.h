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
#ifndef CPPKAFKA_CONFIGURATION_CACHE_H
#define CPPKAFKA_CONFIGURATION_CACHE_H

#include <memory>
#include <unordered_map>
#include <mutex>
#include <librdkafka/rdkafka.h>

namespace cppkafka {

/**
 * \class ConfigurationCache
 * \brief Singleton implementation. Holds a map of all librdkafka configuration parameters.
 */
class ConfigurationCache {
public:
    using CachePtr = std::unique_ptr<ConfigurationCache>;
    using Cache = std::unordered_map<std::string,                       //option name
                                     const rd_kafka_config_option_t*>;  //option metadata
    
    /**
     * \brief Creates and returns a pointer to this cache. This method is thread-safe.
     * \returns A unique pointer to the cache instance.
     */
    static std::unique_ptr<ConfigurationCache>& instance();
    
    /**
     * \brief Verifies if a configuration option matches the scope specified.
     * \param name The name of the configuration option. Must be in lower case.
     * \param scope The scope of the option. This value can be composed by
     *              OR-ing various rd_kafka_conf_scope_t enums together.
     * \returns TRUE if the config is valid, FALSE otherwise.
     * \remark If only RD_PRODUCER or RD_CONSUMER is passed, then this function will
     *         also imply the RD_GLOBAL flag. In other words a configuration parameter
     *         which is defined in the global scope will return TRUE for both RD_PRODUCER
     *         and RD_CONSUMER.
     *         When validating a topic configuration option, RD_TOPIC must always be present
     *         along with (optionally) RD_CONSUMER or RD_PRODUCER.
     *         Also note that at most 2 flags can be combined together
     *         (RD_GLOBAL or RD_TOPIC) and (RD_PRODUCER or RD_CONSUMER or RD_CGRP).
     *         Furthermore RD_CGRP can only be combined with RD_GLOBAL.
     */
    bool validate_configuration_option(const std::string& name, int scope);
    
private:
    
    /**
     * \brief Constructor
     */
    ConfigurationCache();
    ConfigurationCache(const ConfigurationCache& other) = delete;
    ConfigurationCache(ConfigurationCache&& other) = delete;
    ConfigurationCache& operator=(const ConfigurationCache& other) = delete;
    ConfigurationCache& operator=(ConfigurationCache&& other) = delete;
    
    static CachePtr     cachePtr_;      //unique instance
    static std::mutex   cacheMutex_;    //protects access during construction
    Cache               cache_;         //map containing all options
};

} //namespace cppkafka

#endif //CPPKAFKA_CONFIGURATION_CACHE_H
