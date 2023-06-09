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
#include "ha3/common/DistinctClause.h"

#include <cstddef>

#include "autil/DataBuffer.h"

#include "ha3/common/DistinctDescription.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, DistinctClause);

DistinctClause::DistinctClause() {
}

DistinctClause::~DistinctClause() {
    clearDistinctDescriptions();
}

const vector<DistinctDescription*>&
DistinctClause::getDistinctDescriptions() const {
    return _distDescriptions;
}

const DistinctDescription*
DistinctClause::getDistinctDescription(uint32_t pos) const {
    if (pos >= (uint32_t)_distDescriptions.size()) {
        return NULL;
    }
    return _distDescriptions[pos];
}

DistinctDescription*
DistinctClause::getDistinctDescription(uint32_t pos) {
    if (pos >= (uint32_t)_distDescriptions.size()) {
        return NULL;
    }
    return _distDescriptions[pos];
}

void DistinctClause::addDistinctDescription(
        DistinctDescription *distDescription)
{
    _distDescriptions.push_back(distDescription);
}

void DistinctClause::clearDistinctDescriptions() {
    for (vector<DistinctDescription*>::iterator it = _distDescriptions.begin();
         it != _distDescriptions.end(); ++it)
    {
        delete (*it);
    }
    _distDescriptions.clear();
}

uint32_t DistinctClause::getDistinctDescriptionsCount() const {
    return _distDescriptions.size();
}

void DistinctClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_distDescriptions);
}

void DistinctClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_distDescriptions);
}

string DistinctClause::toString() const {
    string distinctClauseStr;
    for(size_t i = 0; i < _distDescriptions.size(); i++) {
        distinctClauseStr.append("[");
        if (_distDescriptions[i]) {
            distinctClauseStr.append(_distDescriptions[i]->toString());
        } else {
            distinctClauseStr.append("none_dist");
        }
        distinctClauseStr.append("]");
    }
    return distinctClauseStr;
}

} // namespace common
} // namespace isearch
