#include <ha3/search/MatchDataManager.h>
#include <ha3/search/SimpleMatchDataFetcher.h>
#include <ha3/search/FullMatchDataFetcher.h>
#include <ha3/search/SubSimpleMatchDataFetcher.h>

using namespace std;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, MatchDataManager);

MatchDataManager::MatchDataManager() {
    _fetcher = NULL;
    _subFetcher = NULL;
    _matchValuesFetcher = NULL;
    _curInitLayer = -1;
    _curSearchLayer = 0;
    _queryCount = 0;
    _scope = 0;
}

MatchDataManager::~MatchDataManager() {
    POOL_DELETE_CLASS(_fetcher);
    for (size_t i = 0; i < _seekQueryExecutors.size(); ++i) {
        for (size_t j = 0; j < _seekQueryExecutors[i].size(); ++j) {
            POOL_DELETE_CLASS(_seekQueryExecutors[i][j]);
        }
    }
}

IE_NAMESPACE(common)::ErrorCode MatchDataManager::fillMatchData(matchdoc::MatchDoc matchDoc) {
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
    return _fetcher->fillMatchData(curLayerExecutors, matchDoc, matchdoc::MatchDoc());
}

IE_NAMESPACE(common)::ErrorCode MatchDataManager::fillMatchValues(matchdoc::MatchDoc matchDoc) {
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

IE_NAMESPACE(common)::ErrorCode MatchDataManager::fillSubMatchData(matchdoc::MatchDoc matchDoc,
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
        HA3_LOG(WARN, "SimpleMatchData and MatchData"           \
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
        Ha3MatchDocAllocator *allocator,
        const std::string &refName,
        SubDocDisplayType subDocDisplayType,
        Pool *pool)
{
    if (subDocDisplayType == SUB_DOC_DISPLAY_FLAT) {
        HA3_LOG(WARN, "require simpleMatchData do not support match data in sub doc flat mode");
        return NULL;
    }
    DO_REQUIRE_MATCHDATA(_fetcher, SimpleMatchDataFetcher);
    return dynamic_cast<matchdoc::Reference<SimpleMatchData> *>(ret);
}

matchdoc::Reference<MatchData> *MatchDataManager::requireMatchData(
        Ha3MatchDocAllocator *allocator,
        const std::string &refName,
        SubDocDisplayType subDocDisplayType,
        Pool *pool)
{
    if (subDocDisplayType == SUB_DOC_DISPLAY_FLAT) {
        HA3_LOG(WARN, "require matchData do not support match data in sub doc flat mode");
        return NULL;
    }
    DO_REQUIRE_MATCHDATA(_fetcher, FullMatchDataFetcher);
    return dynamic_cast<matchdoc::Reference<MatchData> *>(ret);
}

matchdoc::Reference<SimpleMatchData> *MatchDataManager::requireSubSimpleMatchData(
        Ha3MatchDocAllocator *allocator,
        const std::string &refName,
        SubDocDisplayType subDocDisplayType,
        Pool *pool)
{
    if (subDocDisplayType != SUB_DOC_DISPLAY_GROUP) {
        HA3_LOG(WARN, "require subSimpleMatchData only support match data in sub doc group mode");
        return NULL;
    }
    DO_REQUIRE_MATCHDATA(_subFetcher, SubSimpleMatchDataFetcher);
    return dynamic_cast<matchdoc::Reference<SimpleMatchData> *>(ret);
}

matchdoc::Reference<HA3_NS(rank)::MatchValues> *MatchDataManager::requireMatchValues(
        Ha3MatchDocAllocator *allocator,
        const std::string &refName,
        SubDocDisplayType subDocDisplayType,
        Pool *pool)
{
    if (subDocDisplayType == SUB_DOC_DISPLAY_FLAT) {
        HA3_LOG(WARN, "require matchValue do not support match value in sub doc flat mode");
        return NULL;
    }
    DO_REQUIRE_MATCHDATA(_matchValuesFetcher, MatchValuesFetcher);
    return dynamic_cast<matchdoc::Reference<HA3_NS(rank)::MatchValues> *>(ret);
}

END_HA3_NAMESPACE(search);

