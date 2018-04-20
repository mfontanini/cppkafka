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

#include "configuration_cache.h"

namespace cppkafka {

//static variables
ConfigurationCache::CachePtr ConfigurationCache::cachePtr_;
std::mutex                   ConfigurationCache::cacheMutex_;

std::unique_ptr<ConfigurationCache>& ConfigurationCache::instance()
{
    if (!cachePtr_) {
        std::lock_guard<std::mutex> lock(cacheMutex_);
        if (!cachePtr_) {
            cachePtr_ = CachePtr(new ConfigurationCache());
        }
    }
    return cachePtr_;
}

ConfigurationCache::ConfigurationCache()
{
    int size = 0;
    const auto *prop = rd_kafka_get_config_options(&size);
	for (int i = 0; i < size; ++i, ++prop) {
	    if (prop->type == RD_C_PTR) {
	        continue; //this can only be set via special API
	    }
	    cache_[prop->name] = prop;
	}
	
	//Replace the alias types with the real ones
    for (auto&& elem : cache_) {
	    if (elem.second->type == RD_C_ALIAS) {
	        //do a lookup and replace its type
            auto elem_it = cache_.find(elem.first);
            if (elem_it == cache_.end()) {
                continue; //dangling alias ??
            }
            const_cast<rd_kafka_config_option_t*>(elem.second)->type = elem_it->second->type;
	    }
	}
}

bool ConfigurationCache::validate_configuration_option(const std::string& name, int scope)
{
    //find the config option
    auto it = cache_.find(name);
    if (it == cache_.end()) {
        return false;
    }
    
    int configScope = it->second->scope;
    
    switch (scope)
    {
        case RD_GLOBAL:
        case RD_TOPIC:
        case RD_CGRP:
            //single global flags
            return (configScope & scope) != 0; //check if the bit is set
        case RD_CONSUMER:
            return (configScope == RD_GLOBAL) || //allow global config
                   (configScope == (RD_GLOBAL|RD_CONSUMER)) ||
                   (configScope == (RD_GLOBAL|RD_CGRP)); //consider consumer group setting as well
        case RD_PRODUCER:
            return (configScope == RD_GLOBAL) || //allow global config
                   (configScope == (RD_GLOBAL|RD_PRODUCER));
        default:
            //all other flag combinations
            if (((scope & RD_GLOBAL) != 0) && (configScope == RD_GLOBAL)) {
                return true; //allow generic config
            }
            if (((scope & RD_TOPIC) != 0) && (configScope == RD_TOPIC)) {
                return true; //allow generic topic config
            }
            if ((scope & RD_CGRP) == (configScope & RD_CGRP)) {
                return true; //only allow consumer group settings
            }
            return configScope == scope;
    }
}

}

