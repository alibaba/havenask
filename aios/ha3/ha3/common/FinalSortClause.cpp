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
#include "ha3/common/FinalSortClause.h"

#include <utility>

#include "autil/DataBuffer.h"

#include "ha3/isearch.h"
#include "autil/Log.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, FinalSortClause);

const std::string FinalSortClause::DEFAULT_SORTER = "DEFAULT";

FinalSortClause::FinalSortClause() { 
}

FinalSortClause::~FinalSortClause() { 
}

void FinalSortClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_sortName);
    dataBuffer.write(_params);
}

void FinalSortClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_sortName);
    dataBuffer.read(_params);
}

std::string FinalSortClause::toString() const {
    std::string finalSortStr;
    finalSortStr.append(_sortName);
    finalSortStr.append("[");
    for (KeyValueMap::const_iterator it = _params.begin(); 
         it != _params.end(); ++it)
    {
        finalSortStr.append(it->first);
        finalSortStr.append(":");
        finalSortStr.append(it->second);
        finalSortStr.append(",");
    }
    finalSortStr.append("]");
    return finalSortStr;
}

} // namespace common
} // namespace isearch

