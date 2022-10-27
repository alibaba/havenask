#ifndef ISEARCH_SEARCHERTEST_H
#define ISEARCH_SEARCHERTEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <unittest/unittest.h>
#include <ha3/rank/Comparator.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/queryparser/QueryParser.h>
#include <ha3/common/SortClause.h>
#include <ha3/common/AggregateClause.h>
#include <ha3/common/Hits.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/index/index.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/search/test/MatchDocSearcherCreator.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <indexlib/partition/index_application.h>

BEGIN_HA3_NAMESPACE(search);

class MatchDocSearcherTest : public TESTBASE {
public:
    MatchDocSearcherTest();
    ~MatchDocSearcherTest();
public:
    static build_service::analyzer::AnalyzerFactoryPtr initFactory();
public:
    void setUp();
    void tearDown();
private:
    void initRankProfileManager(rank::RankProfileManagerPtr &rankProfileMgrPtr,
                                uint32_t rankSize = 10, uint32_t rerankSize = 10);
    void initMultiRoundRankProfileManager(rank::RankProfileManagerPtr &rankProfileMgrPtr,
            int32_t rankSize, int32_t rerankSize);

    common::Request* prepareRequest();
    void checkFillAttributePhaseOneResult(const common::ResultPtr& result);
    common::Query* parseQuery(const char*query, const char *msg);
    void checkTrace(const common::ResultPtr& result,
                    int idx, const std::string &traces,
                    const std::string &notExistTraces);
    common::ResultPtr innerTestQuery(const std::string &query,
            const std::string &resultStr,
            uint32_t totalHit, const std::string &aggResultStr = "",
            bool searchSuccess = true);
    void innerTestSeek(const std::string &query, const std::string &resultStr,
                       const LayerMetasPtr &layerMetas = LayerMetasPtr(),
	const std::string &aggResultStr = "", bool searchSuccess = true);

    void innerTestSeekAndJoin(const std::string &query,
            const std::string &resultStr,
            HA3_NS(common)::HashJoinInfo *hashJoinInfo,
            const LayerMetasPtr &layerMetas = LayerMetasPtr(),
            const std::string &aggResultStr = "",
            bool searchSuccess = true);
    HA3_NS(common)::HashJoinInfoPtr buildHashJoinInfo(
            const std::unordered_map<size_t, std::vector<int32_t>>
                    &hashJoinMap);

public:
    static bool compareVariableReference(matchdoc::ReferenceBase* left,
            matchdoc::ReferenceBase* right);
    static common::RequestPtr prepareRequest(const std::string &query,
            const suez::turing::TableInfoPtr &tableInfoPtr,
            const MatchDocSearcherCreatorPtr &matchDocSearcherCreator,
            const std::string &defaultIndexName = "phrase");
    static IndexPartitionReaderWrapperPtr makeIndexPartitionReaderWrapper(
            const IE_NAMESPACE(partition)::IndexPartitionReaderPtr &indexPartReaderPtr);
private:
    static build_service::analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr;
    queryparser::QueryParser *_queryParser;
    queryparser::ParserContext *_ctx;
    common::RequestPtr _request;
    IndexPartitionReaderWrapperPtr _partitionReaderPtr;
    std::string _testPath;
    IE_NAMESPACE(partition)::IndexApplication::IndexPartitionMap _indexPartitionMap;
    IE_NAMESPACE(partition)::IndexApplicationPtr _indexApp;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    suez::turing::TableInfoPtr _tableInfoPtr;
    rank::RankProfileManagerPtr _rankProfileMgrPtr;
    config::AggSamplerConfigInfo *_aggSamplerConfigInfo;
    index::FakeIndex _fakeIndex;
    autil::mem_pool::Pool *_pool;
    common::Tracer *_requestTracer;
    MatchDocSearcherCreatorPtr _matchDocSearcherCreatorPtr;
    suez::turing::CavaPluginManagerPtr _cavaPluginManagerPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
    std::string _auxTableName{"store"};
    std::string _joinFieldName{"store_id"};
private:
    HA3_LOG_DECLARE();
};

#define PARSE_QUERY(q) parseQuery(q, __FUNCTION__)

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SEARCHERTEST_H
