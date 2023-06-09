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
#include "ha3/common/AggregateClause.h"

#include <assert.h>
#include <cstddef>

#include "autil/DataBuffer.h"

#include "ha3/common/AggregateDescription.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AggregateClause);

AggregateClause::AggregateClause() {
}

AggregateClause::~AggregateClause() {
    for (vector<AggregateDescription*>::iterator it = _aggDescriptions.begin();
         it != _aggDescriptions.end(); it++)
    {
        delete *it;
    }
    _aggDescriptions.clear();
}

const std::vector<AggregateDescription*>& AggregateClause::getAggDescriptions() const {
    return _aggDescriptions;
}

void AggregateClause::addAggDescription(AggregateDescription *aggDescription) {
    if (aggDescription) {
        _aggDescriptions.push_back(aggDescription);
    }
}

bool AggregateClause::removeAggDescription(AggregateDescription *aggDescription) {
    assert(false);
    return false;
}

void AggregateClause::clearAggDescriptions() {
    _aggDescriptions.clear();
}

void AggregateClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_originalString);
    dataBuffer.write(_aggDescriptions);
}

void AggregateClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_originalString);
    dataBuffer.read(_aggDescriptions);
}

string AggregateClause::toString() const {
    string aggClauseStr;
    size_t descCount = _aggDescriptions.size();
    for (size_t i = 0; i < descCount; i++) {
        assert(_aggDescriptions[i]);
        aggClauseStr.append("[");
        aggClauseStr.append(_aggDescriptions[i]->toString());
        aggClauseStr.append("]");
    }
    return aggClauseStr;
}

} // namespace common
} // namespace isearch
