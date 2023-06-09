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
#include "ha3/search/MatchDataManager.h"

#include <cstddef>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/common/CommonDef.h"
#include "ha3/isearch.h"
#include "ha3/search/FullMatchDataFetcher.h"
#include "ha3/search/MatchDataFetcher.h"
#include "ha3/search/MatchValuesFetcher.h"
#include "ha3/search/SimpleMatchDataFetcher.h"
#include "ha3/search/SubSimpleMatchDataFetcher.h"
#include "ha3/search/TermMetaInfo.h"
#include "ha3/search/MatchData.h"
#include "ha3/search/MatchValues.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/search/SimpleMatchData.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"

using namespace std;
using namespace autil::mem_pool;

using namespace isearch::common;
using namespace isearch::rank;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, MatchDataManager);

MatchDataManager::MatchDataManager()
    : _fetcher(NULL)
    , _simpleFetcher(NULL)
    , _subFetcher(NULL)
    , _matchValuesFetcher(NULL)
    , _matchDataCollectorCenter()

{
    reset();
}

MatchDataManager::~MatchDataManager() {
    reset();
}

void MatchDataManager::reset() {
    POOL_DELETE_CLASS(_fetcher);
    POOL_DELETE_CLASS(_simpleFetcher);
    POOL_DELETE_CLASS(_subFetcher);
    POOL_DELETE_CLASS(_matchValuesFetcher);
    _fetcher = NULL;
    _simpleFetcher = NULL;
    _subFetcher = NULL;
    _matchValuesFetcher = NULL;
    _curInitLayer = -1;
    _curSearchLayer = 0;
    _queryCount = 0;
    _scope = 0;
    _queryExecutors.clear();
    _matchDataCollectorCenter.reset();
    _needMatchData = false;
    for (size_t i = 0; i < _seekQueryExecutors.size(); ++i) {
        for (size_t j = 0; j < _seekQueryExecutors[i].size(); ++j) {
            POOL_DELETE_CLASS(_seekQueryExecutors[i][j]);
        }
    }
    _seekQueryExecutors.clear();
}

indexlib::index::ErrorCode MatchDataManager::fillMatchData(matchdoc::MatchDoc matchDoc) {
    docid_t docId = matchDoc.getDocId();
    SingleLayerSeekExecutors &curSeekExecutors = _seekQueryExecutors[_curSearchLayer];
    for (SingleLayerSeekExecutors::iterator it = curSeekExecutors.begin();
         it != curSeekExecutors.end(); ++it)
    {
        docid_t tempid = INVALID_DOCID;
        auto ec = (*it)->seek(docId, tempid);
        IE_RETURN_CODE_IF_ERROR(ec);
    }
    SingleLayerExecutors &curLayerExecutors = _queryExecutors[_curSearchLayer];
    indexlib::index::ErrorCode ec = indexlib::index::ErrorCode::OK;
    if (_fetcher) {
        ec = _fetcher->fillMatchData(curLayerExecutors, matchDoc, matchdoc::MatchDoc());
    }
    if (_simpleFetcher && ec == indexlib::index::ErrorCode::OK) {
        ec = _simpleFetcher->fillMatchData(curLayerExecutors, matchDoc, matchdoc::MatchDoc());
    }
    return ec;
}

indexlib::index::ErrorCode MatchDataManager::fillMatchValues(matchdoc::MatchDoc matchDoc) {
    docid_t docId = matchDoc.getDocId();
    SingleLayerSeekExecutors &curSeekExecutors = _seekQueryExecutors[_curSearchLayer];
    for (SingleLayerSeekExecutors::iterator it = curSeekExecutors.begin();
         it != curSeekExecutors.end(); ++it)
    {
        docid_t tempid = INVALID_DOCID;
        auto ec = (*it)->seek(docId, tempid);
        IE_RETURN_CODE_IF_ERROR(ec);
    }
    SingleLayerExecutors &curLayerExecutors = _queryExecutors[_curSearchLayer];
    return _matchValuesFetcher->fillMatchValues(curLayerExecutors, matchDoc);
}

indexlib::index::ErrorCode MatchDataManager::fillSubMatchData(matchdoc::MatchDoc matchDoc,
                                        matchdoc::MatchDoc subDoc,
                                        docid_t startSubDocid,
                                        docid_t endSubDocId)
{
    docid_t docId = matchDoc.getDocId();
    SingleLayerSeekExecutors &curSeekExecutors = _seekQueryExecutors[_curSearchLayer];
    for (SingleLayerSeekExecutors::iterator it = curSeekExecutors.begin();
         it != curSeekExecutors.end(); ++it)
    {
        docid_t curDocId = INVALID_DOCID;
        auto ec = (*it)->seek(docId, curDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (curDocId == docId) {
            docid_t tempid = INVALID_DOCID;
            auto ec = (*it)->seekSub(docId, startSubDocid, endSubDocId, true, tempid);
            IE_RETURN_CODE_IF_ERROR(ec);
        } else {
            (*it)->setCurrSub(END_DOCID);
        }
    }
    SingleLayerExecutors &curLayerExecutors = _queryExecutors[_curSearchLayer];
    return _subFetcher->fillMatchData(curLayerExecutors, matchDoc, subDoc);
}

void MatchDataManager::addQueryExecutor(QueryExecutor *executor) {
    _queryExecutors[_curInitLayer].push_back(executor);
}

void MatchDataManager::addRankQueryExecutor(QueryExecutor *queryExecutor) {
    _seekQueryExecutors[_curInitLayer].push_back(queryExecutor);
}

void MatchDataManager::getQueryTermMetaInfo(rank::MetaInfo *metaInfo) const {
    for (uint32_t i = 0; i < _queryCount && i < _queryExecutors.size(); ++i) {
        const SingleLayerExecutors &queryExecutors = _queryExecutors[i];
        for (const auto &queryExecutor: queryExecutors) {
            queryExecutor->getMetaInfo(metaInfo);
        }
    }
}

#define DO_REQUIRE_MATCHDATA(fetcher, FetcherType)              \
    if (fetcher) {                                              \
        AUTIL_LOG(WARN, "SimpleMatchData and MatchData"         \
                " cannot be required in one query");            \
        return NULL;                                            \
    }                                                           \
    fetcher = POOL_NEW_CLASS(pool, FetcherType);                \
    uint32_t termCount = 0;                                     \
    for (uint32_t i = 0;                                        \
         i < _queryCount && i < _queryExecutors.size(); ++i)    \
    {                                                           \
        termCount += _queryExecutors[i].size();                 \
    }                                                           \
    auto ret = fetcher->require(allocator, refName, termCount);

matchdoc::Reference<SimpleMatchData> *MatchDataManager::requireSimpleMatchData(
        matchdoc::MatchDocAllocator *allocator,
        const std::string &refName,
        SubDocDisplayType subDocDisplayType,
        Pool *pool)
{
    if (subDocDisplayType == SUB_DOC_DISPLAY_FLAT) {
        AUTIL_LOG(WARN, "require simpleMatchData do not support match data in sub doc flat mode");
        return NULL;
    }
    DO_REQUIRE_MATCHDATA(_simpleFetcher, SimpleMatchDataFetcher);
    return dynamic_cast<matchdoc::Reference<SimpleMatchData> *>(ret);
}

matchdoc::Reference<MatchData> *MatchDataManager::requireMatchData(
        matchdoc::MatchDocAllocator *allocator,
        const std::string &refName,
        SubDocDisplayType subDocDisplayType,
        Pool *pool)
{
    if (subDocDisplayType == SUB_DOC_DISPLAY_FLAT) {
        AUTIL_LOG(WARN, "require matchData do not support match data in sub doc flat mode");
        return NULL;
    }
    DO_REQUIRE_MATCHDATA(_fetcher, FullMatchDataFetcher);
    return dynamic_cast<matchdoc::Reference<MatchData> *>(ret);
}

matchdoc::Reference<SimpleMatchData> *MatchDataManager::requireSubSimpleMatchData(
        matchdoc::MatchDocAllocator *allocator,
        const std::string &refName,
        SubDocDisplayType subDocDisplayType,
        Pool *pool)
{
    if (subDocDisplayType != SUB_DOC_DISPLAY_GROUP) {
        AUTIL_LOG(WARN, "require subSimpleMatchData only support match data in sub doc group mode");
        return NULL;
    }
    DO_REQUIRE_MATCHDATA(_subFetcher, SubSimpleMatchDataFetcher);
    return dynamic_cast<matchdoc::Reference<SimpleMatchData> *>(ret);
}

matchdoc::Reference<isearch::rank::MatchValues> *MatchDataManager::requireMatchValues(
        matchdoc::MatchDocAllocator *allocator,
        const std::string &refName,
        SubDocDisplayType subDocDisplayType,
        Pool *pool)
{
    if (subDocDisplayType == SUB_DOC_DISPLAY_FLAT) {
        AUTIL_LOG(WARN, "require matchValue do not support match value in sub doc flat mode");
        return NULL;
    }
    DO_REQUIRE_MATCHDATA(_matchValuesFetcher, MatchValuesFetcher);
    return dynamic_cast<matchdoc::Reference<isearch::rank::MatchValues> *>(ret);
}

} // namespace search
} // namespace isearch
