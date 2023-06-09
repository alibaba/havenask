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
#include "ha3/common/AttributeClause.h"

#include "autil/DataBuffer.h"

#include "autil/Log.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AttributeClause);

AttributeClause::AttributeClause() { 
}

AttributeClause::~AttributeClause() { 
}

void AttributeClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_attributeNames);
}

void AttributeClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_attributeNames);
}

std::string AttributeClause::toString() const {
    std::string attrClauseStr;
    std::set<std::string>::const_iterator iter 
        =  _attributeNames.begin();
    for (; iter != _attributeNames.end(); iter++) {
        attrClauseStr.append(*iter);
        attrClauseStr.append("|");
    }
    return attrClauseStr;
}

} // namespace common
} // namespace isearch

