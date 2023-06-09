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
#include "ha3/common/HealthCheckClause.h"

#include "autil/DataBuffer.h"

#include "autil/Log.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, HealthCheckClause);

HealthCheckClause::HealthCheckClause() { 
    _check = false;
    _checkTimes = 0;
    _recoverTime = 3ll * 60 * 1000 * 1000; // 3min
}

HealthCheckClause::~HealthCheckClause() { 
}

void HealthCheckClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_check);
    dataBuffer.write(_checkTimes);
    dataBuffer.write(_recoverTime);
}

void HealthCheckClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_check);
    dataBuffer.read(_checkTimes);
    dataBuffer.read(_recoverTime);
}

std::string HealthCheckClause::toString() const {
    return _originalString;
}

} // namespace common
} // namespace isearch

