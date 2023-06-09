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
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/PoolBase.h"
#include "ha3/isearch.h"
#include "ha3/search/MatchDataCollectorCenter.h"
#include "ha3/search/MatchDataFetcher.h"
#include "ha3/search/MatchValuesFetcher.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/MatchData.h"
#include "ha3/search/MatchValues.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/search/SimpleMatchData.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class MatchDoc;
class MatchDocAllocator;
template <typename T> class Reference;
}  // namespace matchdoc

namespace isearch {
namespace search {

class MatchDataManager
{
public:
    MatchDataManager();
    ~MatchDataManager();
private:
    MatchDataManager(const MatchDataManager &);
    MatchDataManager& operator=(const MatchDataManager &);
public:
    matchdoc::Reference<rank::SimpleMatchData> *requireSimpleMatchData(
            matchdoc::MatchDocAllocator *allocator,
            const std::string &refName,
            SubDocDisplayType subDocDisplayType,
            autil::mem_pool::Pool *pool);
    matchdoc::Reference<rank::MatchData> *requireMatchData(
            matchdoc::MatchDocAllocator *allocator,
            const std::string &refName,
            SubDocDisplayType subDocDisplayType,
            autil::mem_pool::Pool *pool);
    matchdoc::Reference<rank::SimpleMatchData> *requireSubSimpleMatchData(
            matchdoc::MatchDocAllocator *allocator,
            const std::string &refName,
            SubDocDisplayType subDocDisplayType,
            autil::mem_pool::Pool *pool);
    matchdoc::Reference<rank::MatchValues> *requireMatchValues(
            matchdoc::MatchDocAllocator *allocator,
            const std::string &refName,
            SubDocDisplayType subDocDisplayType,
            autil::mem_pool::Pool *pool);
    bool hasMatchData() const { return _fetcher != NULL || _simpleFetcher != NULL;}
    bool hasSubMatchData() const { return _subFetcher != NULL; }
    bool hasMatchValues() const { return _matchValuesFetcher != NULL; }
    bool needMatchData() { return _needMatchData || !_matchDataCollectorCenter.isEmpty(); }
    void requireMatchData() { _needMatchData = true; }
    void setAttributeExprScope(AttributeExprScope scope) { _scope |= scope; }
    bool filterNeedMatchData() { return _scope & AE_FILTER; }
public:
    indexlib::index::ErrorCode fillMatchData(matchdoc::MatchDoc matchDoc);
    indexlib::index::ErrorCode fillMatchValues(matchdoc::MatchDoc matchDoc);
    indexlib::index::ErrorCode fillSubMatchData(matchdoc::MatchDoc matchDoc,
                          matchdoc::MatchDoc subDoc,
                          docid_t startSubDocid,
                          docid_t endSubDocId);
    void getQueryTermMetaInfo(rank::MetaInfo *metaInfo) const;
    void moveToLayer(uint32_t curLayer) {
        _curSearchLayer = curLayer;
        if (_fetcher) {
            uint32_t accTermCount = 0;
            if (curLayer < _queryCount) {
                for (uint32_t i = 0; i < curLayer; ++i) {
                    accTermCount += _queryExecutors[i].size();
                }
            }
            _fetcher->setAccTermCount(accTermCount);
        }
        if (_simpleFetcher) {
            uint32_t accTermCount = 0;
            if (curLayer < _queryCount) {
                for (uint32_t i = 0; i < curLayer; ++i) {
                    accTermCount += _queryExecutors[i].size();
                }
            }
            _simpleFetcher->setAccTermCount(accTermCount);
        }
        if (_subFetcher) {
            uint32_t accTermCount = 0;
            if (curLayer < _queryCount) {
                for (uint32_t i = 0; i < curLayer; ++i) {
                    accTermCount += _queryExecutors[i].size();
                }
            }
            _subFetcher->setAccTermCount(accTermCount);
        }
        if (_matchValuesFetcher) {
            uint32_t accTermCount = 0;
            if (curLayer < _queryCount) {
                for (uint32_t i = 0; i < curLayer; ++i) {
                    accTermCount += _queryExecutors[i].size();
                }
            }
            _matchValuesFetcher->setAccTermCount(accTermCount);
        }
    }
    void setQueryCount(uint32_t queryCount) {
        _queryCount = queryCount;
    }
public:
    void reset();
    MatchDataCollectorCenter& getMatchDataCollectorCenter() {
        return _matchDataCollectorCenter;
    }
public:
    // interface for initialize
    void addQueryExecutor(QueryExecutor *executor);
    void addRankQueryExecutor(QueryExecutor *queryExecutor);
    void beginLayer() {
        ++_curInitLayer;
        _queryExecutors.resize(_curInitLayer + 1);
        _seekQueryExecutors.resize(_curInitLayer + 1);
    }

    const SingleLayerExecutors &getExecutors(uint32_t layer) {
        assert(layer < _queryExecutors.size());
        return _queryExecutors[layer];
    }
public:
    // public for test
    size_t getQueryNum() const { return _queryExecutors[_curSearchLayer].size(); }
private:
    MatchDataFetcher *_fetcher;
    MatchDataFetcher *_simpleFetcher;
    MatchDataFetcher *_subFetcher;
    MatchValuesFetcher *_matchValuesFetcher;
    int32_t _curInitLayer;
    uint32_t _curSearchLayer;
    uint32_t _queryCount;
    uint64_t _scope;
    MatchDataCollectorCenter _matchDataCollectorCenter;
    bool _needMatchData;
    std::vector<SingleLayerExecutors> _queryExecutors;
    std::vector<SingleLayerSeekExecutors> _seekQueryExecutors;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MatchDataManager> MatchDataManagerPtr;
/////////////////////////////////////////////////////////////

} // namespace search
} // namespace isearch
