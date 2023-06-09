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
#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/indexlib.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/SubDocAccessor.h"

namespace isearch {
namespace search {

class BatchAggregateSampler
{
public:
    static const uint32_t DEFAULT_AGG_THRESHOLD = 1000;
    static const uint32_t DEFAULT_DOCIDS_INIT_SIZE = 20000;
    BatchAggregateSampler(uint32_t aggThreshold = DEFAULT_AGG_THRESHOLD,
                          uint32_t sampleMaxCount = DEFAULT_AGG_THRESHOLD);
    ~BatchAggregateSampler();
private:
    BatchAggregateSampler(const BatchAggregateSampler &);
    BatchAggregateSampler& operator=(const BatchAggregateSampler &);
public:
    void collect(matchdoc::MatchDoc matchDoc,
                 matchdoc::MatchDocAllocator *allocator) {
        _docids.push_back(matchDoc.getDocId());
        auto accessor = allocator->getSubDocAccessor();
        if (accessor) {
            auto count = accessor->getSubDocIds(matchDoc, _subdocids);
            uint32_t newEnds = _subdocidEnds.empty() ? 0 : _subdocidEnds.back();
            newEnds += count;
            _subdocidEnds.push_back(newEnds);
            if (count > _maxSubDocNum) {
                _maxSubDocNum = count;
            }
        }
    }
    void endLayer(double factor) {
        assert(factor >= 1.0);
        _multiLayerPos.push_back(_docids.size());
        _multiLayerFactor.push_back(factor);
    }
    void calculateSampleStep();
    bool hasNextLayer() {
        return _curLayerIdx < _multiLayerPos.size();
    }
    double getLayerFactor() {
        assert(_curLayerIdx < _multiLayerFactor.size());
        return _multiLayerFactor[_curLayerIdx++];
    }
    bool hasNext() {
        return _docidPos < _multiLayerPos[_curLayerIdx];
    }
    docid_t getCurDocId() {
        assert(_docidPos < _docids.size());
        return _docids[_docidPos];
    }
    bool beginSubDoc() {
        if (_subdocidEnds.empty()) {
            return false;
        }
        _curSubDocId = !_docidPos ? 0 : _subdocidEnds[_docidPos - 1];
        _curSubDocEnd = _subdocidEnds[_docidPos];
        return true;
    }
    docid_t nextSubDocId() {
        if (_curSubDocId < _curSubDocEnd) {
            return _subdocids[_curSubDocId++];
        } else {
            return INVALID_DOCID;
        }
    }
    void next() {
        assert(_step >= 1);
        _docidPos += _step;
    }
    float getStep() {
        return _step;
    }
    void reStart() {
        _aggDocCount = 0;
        _curLayerIdx = 0;
        _docidPos = 0;
        _curSubDocId = 0;
        _curSubDocEnd = 0;
        _step = 0;
    }
    uint32_t getAggregateCount() {
        return _aggDocCount;
    }
    void setAggThreshold(uint32_t aggThreshold) {
        _aggThreshold = aggThreshold;
    }
    uint32_t getAggThreshold() {
        return _aggThreshold;
    }
    void setSampleMaxCount(uint32_t sampleMaxCount) {
        _sampleMaxCount = sampleMaxCount;
    }
    uint32_t getSampleMaxCount() {
        return _sampleMaxCount;
    }
    uint32_t getMaxSubDocCount() {
        return _maxSubDocNum;
    }
    void fillCurDocIds() {
    }
private:
    // for test
    void reset();
private:
    uint32_t _aggThreshold;
    uint32_t _sampleMaxCount;
    uint32_t _aggDocCount;
    std::vector<docid_t> _docids;
    std::vector<uint32_t> _subdocidEnds;
    std::vector<docid_t> _subdocids;
    std::vector<uint32_t> _multiLayerPos;
    std::vector<double> _multiLayerFactor;
    size_t _curLayerIdx;
    size_t _docidPos;
    size_t _curSubDocId;
    size_t _curSubDocEnd;
    size_t _step;
    uint32_t _maxSubDocNum;
private:
    AUTIL_LOG_DECLARE();
    friend class BatchAggregateSamplerTest;
};

typedef std::shared_ptr<BatchAggregateSampler> BatchAggregateSamplerPtr;

} // namespace search
} // namespace isearch

