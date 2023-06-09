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
#include "ha3/common/AggregateResult.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/legacy/exception.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/common.h"

#include "ha3/common/CommonDef.h"
#include "autil/Log.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace autil::legacy;
using namespace std;
using namespace suez::turing;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AggregateResult);

AggregateResult::AggregateResult() {
}

AggregateResult::~AggregateResult() {
    if (_allocatorPtr) {
        for (auto doc : _matchdocVec) {
            _allocatorPtr->deallocate(doc);
        }
    }
    _exprValue2AggResult.clear();
    _matchdocVec.clear();
}

const string& AggregateResult::getAggFunName(uint32_t funOffset) const {
    if (funOffset < _funNames.size()) {
        return _funNames[funOffset];
    }

    const static string sNullString;
    return sNullString;
}

std::string AggregateResult::getAggFunDesStr(uint32_t i) const {
    if (i >= _funNames.size() || i >= _funParameters.size()) {
        return "";
    }

    return _funNames[i] + "(" + _funParameters[i] + ")";
}

const vector<string>& AggregateResult::getAggFunNames() const {
    return _funNames;
}

uint32_t AggregateResult::getAggFunCount() const {
    return _funNames.size();
}

AggregateResult::AggExprValueMap& AggregateResult::getAggExprValueMap() {
    return _exprValue2AggResult;
}

uint32_t AggregateResult::getAggExprValueCount() const {
    return _matchdocVec.size();
}

matchdoc::MatchDoc AggregateResult::getAggFunResults(
        const AggExprValue &value) const
{
    AggExprValueMap::const_iterator it = _exprValue2AggResult.find(value);
    if (it != _exprValue2AggResult.end()) {
        return it->second;
    }
    return matchdoc::INVALID_MATCHDOC;
}

void AggregateResult::addAggFunName(const std::string &functionName) {
    _funNames.push_back(functionName);
}

void AggregateResult::addAggFunParameter(const std::string &funParameter) {
    _funParameters.push_back(funParameter);
}

void AggregateResult::addAggFunResults(matchdoc::MatchDoc aggFunResults) {
    _matchdocVec.push_back(aggFunResults);
}

void AggregateResult::addAggFunResults(const std::string &key,
                                       matchdoc::MatchDoc aggFunResults)
{
    _matchdocVec.push_back(aggFunResults);
    _exprValue2AggResult.insert(make_pair(key, aggFunResults));
}

void AggregateResult::serialize(autil::DataBuffer &dataBuffer) const {
    bool bExisted = (_allocatorPtr != NULL);
    dataBuffer.writeBytes(&bExisted, sizeof(bExisted));
    if (!bExisted) {
        return;
    }
    _allocatorPtr->extend(); // empty result, Aggregator may not extend()
    _allocatorPtr->setSortRefFlag(false);
    _allocatorPtr->serialize(dataBuffer, _matchdocVec, SL_CACHE);
    dataBuffer.write(_funNames);
    dataBuffer.write(_funParameters);
    dataBuffer.write(_groupExprStr);
}

void AggregateResult::deserialize(autil::DataBuffer &dataBuffer,
                                  autil::mem_pool::Pool *pool)
{
    bool bExisted = false;
    dataBuffer.readBytes(&bExisted, sizeof(bExisted));
    if (!bExisted) {
        return;
    }
    _allocatorPtr.reset(new matchdoc::MatchDocAllocator(pool));
    assert(_allocatorPtr != NULL);
    _allocatorPtr->deserialize(dataBuffer, _matchdocVec);
    dataBuffer.read(_funNames);
    dataBuffer.read(_funParameters);
    dataBuffer.read(_groupExprStr);
}

// TODO: remove constructGroupValueMap interface, try to use fillGroupValueMap
void AggregateResult::constructGroupValueMap() {
    fillGroupValueMap(_exprValue2AggResult);
}

void AggregateResult::fillGroupValueMap(AggregateResult::AggExprValueMap& exprValueMap) {
    exprValueMap.clear();
    if (_matchdocVec.size() > 0) {
        auto ref = _allocatorPtr->findReferenceWithoutType(common::GROUP_KEY_REF);
        assert(ref);
        for (auto doc : _matchdocVec) {
            exprValueMap[ref->toString(doc)] = doc;
        }
    }
}

} // namespace common
} // namespace isearch
