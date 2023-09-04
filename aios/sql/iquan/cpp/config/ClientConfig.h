/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <map>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"

namespace iquan {

class CacheConfig : public autil::legacy::Jsonizable {
public:
    CacheConfig()
        : initialCapcity(1024)
        , concurrencyLevel(8)
        , maxSize(4096) {}

    CacheConfig(int64_t maxSize)
        : initialCapcity(1024)
        , concurrencyLevel(8)
        , maxSize(maxSize) {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("initial_capacity", initialCapcity, initialCapcity);
        json.Jsonize("concurrency_level", concurrencyLevel, concurrencyLevel);
        json.Jsonize("maximum_size", maxSize, maxSize);
    }

    bool isValid() const {
        if (initialCapcity <= 0 || concurrencyLevel <= 0 || maxSize <= 0) {
            return false;
        }
        return true;
    }

public:
    int64_t initialCapcity;
    int64_t concurrencyLevel;
    int64_t maxSize;
};

class ClientConfig : public autil::legacy::Jsonizable {
public:
    ClientConfig()
        : debugFlag(false) {
        cacheConfigs[IQUAN_SQL_PARSE] = CacheConfig();
        cacheConfigs[IQUAN_REL_TRANSFORM] = CacheConfig();
        cacheConfigs[IQUAN_REL_POST_OPTIMIZE] = CacheConfig();
        cacheConfigs[IQUAN_JNI_POST_OPTIMIZE] = CacheConfig(100 * 1024 * 1024);
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("debug_flag", debugFlag, debugFlag);
        json.Jsonize("cache_config", cacheConfigs, cacheConfigs);
    }

    bool isValid() const {
        std::map<std::string, CacheConfig>::const_iterator iter = cacheConfigs.begin();
        ;
        for (; iter != cacheConfigs.end(); iter++) {
            if (!iter->second.isValid()) {
                return false;
            }
        }
        return true;
    }

public:
    bool debugFlag;
    std::map<std::string, CacheConfig> cacheConfigs;
};

} // namespace iquan
