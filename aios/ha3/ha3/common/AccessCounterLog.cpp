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
#include "ha3/common/AccessCounterLog.h" // IWYU pragma: keep

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AccessCounterLog);

AccessCounterLog::AccessCounterLog() 
    : _lastLogTime(0)
{ 
}

AccessCounterLog::~AccessCounterLog() { 
}

bool AccessCounterLog::canReport() {
    int64_t now = TimeUtility::currentTimeInSeconds();
    if (now >= _lastLogTime + LOG_INTERVAL) {
        _lastLogTime = now;
        return true;
    } else {
        return false;
    }
    return true;
}

void AccessCounterLog::reportIndexCounter(const string &name, uint32_t count) {
    log("Index", name, count);
}

void AccessCounterLog::reportAttributeCounter(
        const string &name, uint32_t count) 
{
    log("Attribute", name, count);
}

void AccessCounterLog::log(const string &prefix, 
                           const string &name, uint32_t count) 
{
    AUTIL_LOG(INFO, "[%s: %s]: %u", prefix.c_str(), name.c_str(), count);
}

} // namespace common
} // namespace isearch

