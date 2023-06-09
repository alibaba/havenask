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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"

namespace autil {
class DataBuffer;

namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class ReferenceBase;
template <typename T> class Reference;
}  // namespace matchdoc

namespace isearch {
namespace common {

/** to store the aggregation result of one Aggregator. */
class AggregateResult
{
public:
    typedef std::string AggExprValue;
    typedef std::map<AggExprValue, matchdoc::MatchDoc> AggExprValueMap;
public:
    AggregateResult();
    AggregateResult(const std::string &groupExprStr) {
        _groupExprStr=groupExprStr;
    }

    ~AggregateResult();
public:
    /** get the aggregation function name, like 'sum', 'count' */
    void addAggFunName(const std::string &functionName);
    void addAggFunParameter(const std::string &funParameter);

    const std::string& getAggFunName(uint32_t funOffset) const;
    const std::vector<std::string>& getAggFunNames() const;
    uint32_t getAggFunCount() const;

    std::string getAggFunDesStr(uint32_t funOffset) const;

    uint32_t getAggExprValueCount() const;
    AggExprValueMap& getAggExprValueMap();
    template <typename T>
    T getAggFunResult(const AggExprValue &value, uint32_t funOffset, const T &defautValue) const;
    template <typename T>
    T getFunResult(matchdoc::MatchDoc aggFunResults, uint32_t funOffset,
                   const T &defautValue) const;
    matchdoc::MatchDoc getAggFunResults(const AggExprValue &value) const;

    template <typename T>
    const matchdoc::Reference<T> *getAggFunResultRefer(uint32_t funOffset) const;

    const matchdoc::ReferenceBase *getAggFunResultReferBase(uint32_t funOffset) const;
    void setMatchDocAllocator(const matchdoc::MatchDocAllocatorPtr &allocatorPtr) {
        _allocatorPtr = allocatorPtr;
    }
    void addAggFunResults(matchdoc::MatchDoc aggFunResults);

    // interface for compatible, only work in qrs_worker.
    void addAggFunResults(const std::string &key, matchdoc::MatchDoc aggFunResults);

    void setGroupExprStr(const std::string &groupExprStr) {
        _groupExprStr = groupExprStr;
    }
    const std::string &getGroupExprStr() const {
        return _groupExprStr;
    }
    const std::vector<std::string>& getAggParams() const {
        return _funParameters;
    }
public:
    AggregateResult *clone() const { return new AggregateResult(*this); }
public:
    // inner function
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool);

    void constructGroupValueMap();
    void fillGroupValueMap(AggregateResult::AggExprValueMap& exprValueMap);
    const matchdoc::MatchDocAllocatorPtr &getMatchDocAllocator() const {
        return _allocatorPtr;
    }
    void setAggResultVector(const std::vector<matchdoc::MatchDoc> &matchDocVec) {
        _matchdocVec = matchDocVec;
    }
    const std::vector<matchdoc::MatchDoc> &getAggResultVector() const {
        return _matchdocVec;
    }
    std::vector<matchdoc::MatchDoc> &getAggResultVector() {
        return _matchdocVec;
    }
    void clearMatchdocs() {
        _exprValue2AggResult.clear();
        _matchdocVec.clear();
    }
private:
    // the map from 'group key expression value' to 'VariableSlab'
    AggExprValueMap _exprValue2AggResult;
    std::vector<std::string> _funNames;
    std::vector<std::string> _funParameters;
    std::string _groupExprStr;
    matchdoc::MatchDocAllocatorPtr _allocatorPtr;
    std::vector<matchdoc::MatchDoc> _matchdocVec;

    friend class AggResultReader;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggregateResult> AggregateResultPtr;
typedef std::vector<AggregateResultPtr> AggregateResults;
typedef std::shared_ptr<AggregateResults> AggregateResultsPtr;

template <typename T>
T AggregateResult::getAggFunResult(const AggExprValue &value, uint32_t funOffset,
                                   const T &defautValue) const
{
    AggExprValueMap::const_iterator it = _exprValue2AggResult.find(value);
    if (it == _exprValue2AggResult.end()) {
        AUTIL_LOG(TRACE3, "not find expr Value: %s", value.c_str());
        return defautValue;
    }
    auto aggFunResults = it->second;
    auto refer = getAggFunResultRefer<T>(funOffset);
    if (NULL == refer) {
        AUTIL_LOG(TRACE3, "refer is NULL");
        return defautValue;
    }
    return refer->get(aggFunResults);
}

inline const matchdoc::ReferenceBase *AggregateResult::getAggFunResultReferBase(
        uint32_t funOffset) const
{
    assert(_allocatorPtr);
    auto referenceVec = _allocatorPtr->getAllNeedSerializeReferences(
            SL_CACHE);
    if (referenceVec.size() <= funOffset + 1 ) {
        return NULL;
    }
    return referenceVec[funOffset + 1];
}

template <typename T>
const matchdoc::Reference<T> *AggregateResult::getAggFunResultRefer(
        uint32_t funOffset) const
{
    auto base = getAggFunResultReferBase(funOffset);
    if (!base) {
        return NULL;
    }
    return dynamic_cast<const matchdoc::Reference<T> *>(base);
}

template <typename T>
T AggregateResult::getFunResult(matchdoc::MatchDoc aggFunResults, uint32_t funOffset,
                                   const T &defautValue) const
{
    if (matchdoc::INVALID_MATCHDOC == aggFunResults) {
        return defautValue;
    }
    auto refer = getAggFunResultRefer<T>(funOffset);
    if (NULL == refer) {
        AUTIL_LOG(TRACE3, "refer is NULL");
        return defautValue;
    }
    return refer->get(aggFunResults);
}
} // namespace common
} // namespace isearch
