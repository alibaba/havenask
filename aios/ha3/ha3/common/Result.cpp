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
#include "ha3/common/Result.h"

#include <assert.h>
#include <stdint.h>
#include <unistd.h>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/Hits.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/SortExprMeta.h"
#include "ha3/common/searchinfo/ExtraSearchInfo.h"
#include "ha3/common/searchinfo/PhaseOneSearchInfo.h"
#include "ha3/common/searchinfo/PhaseTwoSearchInfo.h"
#include "ha3/isearch.h"
#include "suez/turing/expression/common.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace autil::legacy;
using namespace autil::mem_pool;
using namespace std;
using namespace suez::turing;
using namespace isearch::util;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, Result);

Result::Result(const ErrorCode errorCode, const string &errorMsg) {
    init();
    _mErrorResult->addError(errorCode, errorMsg);
}

Result::Result(Hits *hits) {
    init();
    _hits = hits;
}

Result::Result(MatchDocs *matchDocs) {
    init();
    _matchDocs = matchDocs;
}

Result::~Result() {
    clear();
}

void Result::init() {
    _hits = NULL;
    _matchDocs = NULL;
    _mErrorResult.reset(new MultiErrorResult);
    _phaseOneSearchInfo = NULL;
    _phaseTwoSearchInfo = NULL;
    _useTruncateOptimizer = false;
    _totalTime = 0;
    _serializeLevel = SL_QRS;
    _lackResult = false;
    _srcCount = 1u;
}

void Result::setHits(Hits *hits) {
    if (_hits) {
        delete _hits;
    }
    _hits = hits;
}

Hits *Result::getHits() const {
    return _hits;
}

Hits *Result::stealHits() {
    Hits *hits = _hits;
    _hits = NULL;
    return hits;
}

uint32_t Result::getTotalHits() const {
    return _hits == NULL ? 0 : _hits->totalHits();
}

void Result::mergeByAppend(ResultPtr &resultPtr, bool doDedup) {
    _useTruncateOptimizer =
        _useTruncateOptimizer || resultPtr->useTruncateOptimizer();
    if (_hits == NULL) {
        _hits = resultPtr->stealHits();
    } else if (resultPtr->getHits() != NULL) {
        _hits->stealAndMergeHits(*(resultPtr->getHits()), doDedup);
    }
    addErrorResult(resultPtr->getMultiErrorResult());
    mergeAggregateResults(resultPtr->getAggregateResults());
}

void Result::mergeByDefaultSort(ResultPtr &resultPtr, bool doDedup) {
    mergeByAppend(resultPtr, doDedup);
    _hits->sortHits();
}

void Result::mergeAggregateResults(AggregateResults &aggregateResults) {
    _aggResults.insert(_aggResults.end(),
                       aggregateResults.begin(), aggregateResults.end());
}

void Result::setMatchDocs(MatchDocs *matchDocs) {
    if (_matchDocs) {
        delete _matchDocs;
        _matchDocs = NULL;
    }
    _matchDocs = matchDocs;
}

MatchDocs *Result::getMatchDocs() const {
    return _matchDocs;
}

void Result::clearMatchDocs() {
    if (_matchDocs) {
        delete _matchDocs;
        _matchDocs = NULL;
    }
}

uint32_t Result::getTotalMatchDocs() const {
    if (_matchDocs) {
        return _matchDocs->totalMatchDocs();
    }
    return 0;
}

void Result::setTotalMatchDocs(uint32_t totalMatchDocs) {
    if (_matchDocs) {
        _matchDocs->setTotalMatchDocs(totalMatchDocs);
    }
}

void Result::setTracer(common::TracerPtr tracer) {
    _tracer = tracer;
}

Tracer *Result::getTracer() const {
    return _tracer.get();
}

TracerPtr Result::getTracerPtr() const {
    return _tracer;
}

void Result::setPhaseOneSearchInfo(PhaseOneSearchInfo *searchInfo) {
    if (_phaseOneSearchInfo) {
        delete _phaseOneSearchInfo;
    }
    _phaseOneSearchInfo = searchInfo;
}

void Result::setPhaseTwoSearchInfo(PhaseTwoSearchInfo *searchInfo) {
    if (_phaseTwoSearchInfo) {
        delete _phaseTwoSearchInfo;
    }
    _phaseTwoSearchInfo = searchInfo;
}

PhaseOneSearchInfo* Result::getPhaseOneSearchInfo() const {
    return _phaseOneSearchInfo;
}

PhaseTwoSearchInfo* Result::getPhaseTwoSearchInfo() const {
    return _phaseTwoSearchInfo;
}

void Result::setUseTruncateOptimizer(bool flag) {
    _useTruncateOptimizer = flag;
}

bool Result::useTruncateOptimizer() const {
    return _useTruncateOptimizer;
}

uint32_t Result::getActualMatchDocs() const {
    if (_matchDocs) {
        return _matchDocs->actualMatchDocs();
    }
    return 0;
}

void Result::setActualMatchDocs(uint32_t actualMatchDocs) {
    if (_matchDocs) {
        _matchDocs->setActualMatchDocs(actualMatchDocs);
    }
}

AggregateResults& Result::getAggregateResults() {
    return _aggResults;
}

uint32_t Result::getAggregateResultCount() const {
    return _aggResults.size();
}

AggregateResultPtr Result::getAggregateResult(uint32_t offset) const {
    assert(offset < _aggResults.size());
    return _aggResults[offset];
}
void Result::addAggregateResult(AggregateResultPtr aggregateResultPtr) {
    _aggResults.push_back(aggregateResultPtr);
}

void Result::fillAggregateResults(const AggregateResultsPtr &aggResultsPtr) {
    if (!aggResultsPtr) {
        return;
    }
    for (AggregateResults::const_iterator it = aggResultsPtr->begin();
         it != aggResultsPtr->end(); ++it)
    {
        addAggregateResult(*it);
    }
}

void Result::serialize(autil::DataBuffer &dataBuffer) const {
    AUTIL_LOG(DEBUG, "coming serialize");
    if (_hits) {
        dataBuffer.write(true);
        dataBuffer.write(*_hits);
    } else {
        dataBuffer.write(false);
    }
    if (_matchDocs) {
        dataBuffer.write(true);
        dataBuffer.write(*_matchDocs);
    } else {
        dataBuffer.write(false);
    }
    ExtraSearchInfo extraSearchInfo;
    if (likely(_phaseOneSearchInfo != NULL)) {
        extraSearchInfo.otherInfoStr = _phaseOneSearchInfo->otherInfoStr;
        extraSearchInfo.seekDocCount = _phaseOneSearchInfo->seekDocCount;
        extraSearchInfo.phaseTwoSearchInfo = _phaseTwoSearchInfo;
        extraSearchInfo.toString(_phaseOneSearchInfo->otherInfoStr, NULL);
    }
    dataBuffer.write(_aggResults);
    dataBuffer.write(_mErrorResult);
    dataBuffer.write(_tracer.get());
    dataBuffer.write(_phaseOneSearchInfo);
    dataBuffer.write(_coveredRanges);
    dataBuffer.write(_globalVariables);
    dataBuffer.write(_useTruncateOptimizer);
    dataBuffer.write(_srcCount);
    if (likely(_phaseOneSearchInfo != NULL)) {
        _phaseOneSearchInfo->otherInfoStr = extraSearchInfo.otherInfoStr;
    }
}

void Result::deserialize(autil::DataBuffer &dataBuffer, Pool *pool) {
    clear();

    bool hasHits = false;
    dataBuffer.read(hasHits);
    if (hasHits) {
        _hits = new Hits();
        _hits->deserialize(dataBuffer, pool);
    }
    bool hasMatchDocs = false;
    dataBuffer.read(hasMatchDocs);
    if (hasMatchDocs) {
        _matchDocs = new MatchDocs();
        _matchDocs->setSerializeLevel(_serializeLevel);
        _matchDocs->deserialize(dataBuffer, pool);
    }
    dataBuffer.read(_aggResults, pool);
    dataBuffer.read(_mErrorResult);
    dataBuffer.read(_tracer);
    dataBuffer.read(_phaseOneSearchInfo);
    dataBuffer.read(_coveredRanges);
    dataBuffer.read(_globalVariables);
    dataBuffer.read(_useTruncateOptimizer);
    dataBuffer.read(_srcCount);

    if (likely(_phaseOneSearchInfo != NULL)) {
        if (!_phaseOneSearchInfo->otherInfoStr.empty()) {
            ExtraSearchInfo extraSearchInfo;
            extraSearchInfo.fromString(_phaseOneSearchInfo->otherInfoStr, pool);
            _phaseOneSearchInfo->seekDocCount = extraSearchInfo.seekDocCount;
            _phaseOneSearchInfo->otherInfoStr = extraSearchInfo.otherInfoStr;
            _phaseTwoSearchInfo = extraSearchInfo.phaseTwoSearchInfo;
        }
    }
}

void Result::clear() {
    DELETE_AND_SET_NULL(_hits);
    DELETE_AND_SET_NULL(_matchDocs);
    DELETE_AND_SET_NULL(_phaseOneSearchInfo);
    DELETE_AND_SET_NULL(_phaseTwoSearchInfo);
    _mErrorResult.reset();
    _aggResults.clear();
    _coveredRanges.clear();
    _formatedDocs.clear();
    _clusterNames.clear();
    _globalVariables.clear();
    _sortExprMetaVec.clear();
}

void Result::serializeToString(string &str, autil::mem_pool::Pool *pool) const {
    autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE * 4, pool);
    serialize(dataBuffer);
    str.assign(dataBuffer.getData(), dataBuffer.getDataLen());
}

bool Result::deserializeFromString(const string &str, Pool *pool) {
    return deserializeFromString(str.c_str(), str.size(), pool);
}

bool Result::deserializeFromString(const char *data, size_t size,  Pool *pool) {
    autil::DataBuffer dataBuffer((void*)data, size, pool);
    try {
        deserialize(dataBuffer, pool);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "deserialize result from databuffer failed. error:[%s].",
                  e.what());
        return false;
    }
    return true;
}

void Result::setClusterInfo(const string &clusterName, clusterid_t clusterId) {
    if (_matchDocs) {
        _matchDocs->setClusterId(clusterId);
    }
    if (_hits) {
        _hits->setClusterInfo(clusterName, clusterId);
    }
}

void Result::setErrorHostInfo(const std::string &partitionID,
                              const std::string &hostName)
{
    if (hostName.empty()) {
        char tmpHostname[256];
        //don't care whether error happen
        gethostname(tmpHostname, sizeof(tmpHostname));
        _mErrorResult->setHostInfo(partitionID, tmpHostname);
    } else {
        _mErrorResult->setHostInfo(partitionID, hostName);
    }
}

} // namespace common
} // namespace isearch
