#ifndef ISEARCH_CACHEDMATCHDOCSEARCHERTEST_H
#define ISEARCH_CACHEDMATCHDOCSEARCHERTEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDocSearcher.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/search/test/MatchDocSearcherCreator.h>
#include <unittest/unittest.h>

BEGIN_HA3_NAMESPACE(search);

class CachedMatchDocSearcherTest : public TESTBASE {
public:
    CachedMatchDocSearcherTest();
    ~CachedMatchDocSearcherTest();
public:
    void setUp();
    void tearDown();
    void TestBody() {}

    common::RequestPtr prepareRequest(const std::string &requestStr);
private:
    common::ResultPtr innerTestQuery(
            const IndexPartitionReaderWrapperPtr& indexPartitionReaderWrapper,
            const SearcherCachePtr& cachePtr,
            const std::string &query, const std::string &resultStr,
            uint32_t totalHit);
    void compareCacheResult(SearcherCachePtr cachePtr,
                            common::RequestPtr requestPtr,
                            const IndexPartitionReaderWrapperPtr& indexPartitionReaderWrapper,
                            const std::string &cacheIDStr,
                            const std::string &subDocGidStr = "");
    IndexPartitionReaderWrapperPtr makeFakeIndexPartReader();
private:
    std::string _requestStr;
    suez::turing::TableInfoPtr _tableInfoPtr;
    autil::mem_pool::Pool *_pool;
    MatchDocSearcherCreatorPtr _matchDocSearcherCreatorPtr;
    suez::turing::CavaPluginManagerPtr _cavaPluginManagerPtr;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_CACHEDMATCHDOCSEARCHERTEST_H
