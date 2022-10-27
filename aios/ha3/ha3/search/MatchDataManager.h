#ifndef ISEARCH_MATCHDATAMANAGER_H
#define ISEARCH_MATCHDATAMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/MatchData.h>
#include <ha3/rank/SimpleMatchData.h>
#include <ha3/rank/MetaInfo.h>
#include <ha3/search/TermQueryExecutor.h>
#include <ha3/search/MatchDataFetcher.h>
#include <ha3/search/MatchValuesFetcher.h>

BEGIN_HA3_NAMESPACE(search);

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
            common::Ha3MatchDocAllocator *allocator,
            const std::string &refName,
            SubDocDisplayType subDocDisplayType,
            autil::mem_pool::Pool *pool);
    matchdoc::Reference<rank::MatchData> *requireMatchData(
            common::Ha3MatchDocAllocator *allocator,
            const std::string &refName,
            SubDocDisplayType subDocDisplayType,
            autil::mem_pool::Pool *pool);
    matchdoc::Reference<rank::SimpleMatchData> *requireSubSimpleMatchData(
            common::Ha3MatchDocAllocator *allocator,
            const std::string &refName,
            SubDocDisplayType subDocDisplayType,
            autil::mem_pool::Pool *pool);
    matchdoc::Reference<rank::MatchValues> *requireMatchValues(
            common::Ha3MatchDocAllocator *allocator,
            const std::string &refName,
            SubDocDisplayType subDocDisplayType,
            autil::mem_pool::Pool *pool);
    bool hasMatchData() const { return _fetcher != NULL; }
    bool hasSubMatchData() const { return _subFetcher != NULL; }
    bool hasMatchValues() const { return _matchValuesFetcher != NULL; }
    void setAttributeExprScope(AttributeExprScope scope) { _scope |= scope; }
    bool filterNeedMatchData() { return _scope & AE_FILTER; }
public:
    IE_NAMESPACE(common)::ErrorCode fillMatchData(matchdoc::MatchDoc matchDoc);
    IE_NAMESPACE(common)::ErrorCode fillMatchValues(matchdoc::MatchDoc matchDoc);
    IE_NAMESPACE(common)::ErrorCode fillSubMatchData(matchdoc::MatchDoc matchDoc,
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
    void resetMatchData() {
        POOL_DELETE_CLASS(_fetcher);
        POOL_DELETE_CLASS(_subFetcher);
        _fetcher = NULL;
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
    MatchDataFetcher *_subFetcher;
    MatchValuesFetcher *_matchValuesFetcher;
    int32_t _curInitLayer;
    uint32_t _curSearchLayer;
    uint32_t _queryCount;
    std::vector<SingleLayerExecutors> _queryExecutors;
    std::vector<SingleLayerSeekExecutors> _seekQueryExecutors;
    uint64_t _scope;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MatchDataManager);
/////////////////////////////////////////////////////////////

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MATCHDATAMANAGER_H
