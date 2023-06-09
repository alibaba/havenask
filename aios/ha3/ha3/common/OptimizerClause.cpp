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
#include "ha3/common/OptimizerClause.h"

#include <stddef.h>
#include <algorithm>

#include "autil/DataBuffer.h"

#include "autil/Log.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, OptimizerClause);

OptimizerClause::OptimizerClause() { 
}

OptimizerClause::~OptimizerClause() { 
}

void OptimizerClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_optimizeNames);
    dataBuffer.write(_optimizeOptions);
}

void OptimizerClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_optimizeNames);
    dataBuffer.read(_optimizeOptions);
}

std::string OptimizerClause::toString() const {
    std::string optimizeStr;
    size_t optimizerNum = std::min(_optimizeNames.size(), _optimizeOptions.size());
    for (size_t i = 0; i < optimizerNum; i++) {
        optimizeStr.append(_optimizeNames[i]);
        optimizeStr.append(":");
        optimizeStr.append(_optimizeOptions[i]);
        optimizeStr.append(";");
    }
    return optimizeStr;
}

} // namespace common
} // namespace isearch

