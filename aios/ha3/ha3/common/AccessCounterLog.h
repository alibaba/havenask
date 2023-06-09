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

#include <stdint.h>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {

class AccessCounterLog
{
private:
    static const uint32_t LOG_INTERVAL = 30 * 60; // 30 min
public:
    AccessCounterLog();
    ~AccessCounterLog();
private:
    AccessCounterLog(const AccessCounterLog &);
    AccessCounterLog& operator=(const AccessCounterLog &);
public:
    bool canReport();
    void reportIndexCounter(const std::string &indexName, uint32_t count);
    void reportAttributeCounter(const std::string &attributeName, 
                                uint32_t count);
private:
    void log(const std::string &prefix, const std::string &name, uint32_t count);
private:
    int64_t _lastLogTime;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AccessCounterLog> AccessCounterLogPtr;

} // namespace common
} // namespace isearch

