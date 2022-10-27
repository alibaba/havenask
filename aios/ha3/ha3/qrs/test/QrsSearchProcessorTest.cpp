#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsSearchProcessor.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/qrs/QrsSearchProcessor.h>
#include <ha3/qrs/test/FakeQrsSearcherProcessDelegation.h>
#include <ha3/proto/test/ARPCMessageCreator.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/monitor/TracerMetricsReporter.h>
#include <ha3/qrs/test/ExceptionQrsProcessor.h>
#include <autil/HashFuncFactory.h>
#include <autil/HashFunctionBase.h>
#include <indexlib/util/hash_string.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3/turing/qrs/QrsBiz.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(turing);
using namespace multi_call;

BEGIN_HA3_NAMESPACE(qrs);

class QrsSearchProcessorTest : public TESTBASE {
public:
    QrsSearchProcessorTest();
    ~QrsSearchProcessorTest();
public:
    void setUp();
    void tearDown();
protected:
    common::RequestPtr prepareCorrectRequest();
    common::Query* prepareComplicatedQuery();
    common::ResultPtr prepareResult();
protected:
    QrsSearchProcessorPtr _searchProcessorPtr;
    service::HitSummarySchemaCachePtr _hitSummarySchemaCache;
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
    ClusterSorterManagersPtr _clusterSorterManagersPtr;
    ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    turing::QrsBizPtr _qrsBiz;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QrsSearchProcessorTest);


QrsSearchProcessorTest::QrsSearchProcessorTest() {
}

QrsSearchProcessorTest::~QrsSearchProcessorTest() {
}

void QrsSearchProcessorTest::setUp() {
    _clusterConfigMapPtr.reset(new ClusterConfigMap);
    ClusterConfigInfo clusterInfo;
    clusterInfo._tableName = "table";
    clusterInfo._hashMode._hashField = "pk";
    clusterInfo._hashMode._hashFunction = "HASH";
    _clusterConfigMapPtr->insert(make_pair("cluster.default", clusterInfo));

    IndexInfo* indexInfo = new IndexInfo();
    indexInfo->setIndexName("pk");
    indexInfo->setIndexType(it_primarykey64);
    TableInfoPtr tableInfoPtr(new TableInfo());
    IndexInfos* indexInfos = new IndexInfos();
    indexInfos->addIndexInfo(indexInfo);
    tableInfoPtr->setIndexInfos(indexInfos);
    _clusterTableInfoMapPtr.reset(new ClusterTableInfoMap());
    (*_clusterTableInfoMapPtr)["cluster.default"] = tableInfoPtr;

    _hitSummarySchemaCache.reset(new HitSummarySchemaCache(_clusterTableInfoMapPtr));
    _qrsBiz.reset(new QrsBiz);

    _searchProcessorPtr.reset(new QrsSearchProcessor(
                    1000000, _clusterTableInfoMapPtr,
                    _clusterConfigMapPtr, _clusterSorterManagersPtr,
                    NO_COMPRESS, SearchTaskQueueRule()));
    _searchProcessorPtr->setSearcherDelegation(
            new FakeQrsSearcherProcessDelegation(_hitSummarySchemaCache));
    monitor::SessionMetricsCollectorPtr
        sessionMetricsCollectorPtr(
                new monitor::SessionMetricsCollector(NULL));
    _searchProcessorPtr->setSessionMetricsCollector(
            sessionMetricsCollectorPtr);
    HA3_LOG(DEBUG, "setUp!");
}

void QrsSearchProcessorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QrsSearchProcessorTest, testFlatten) {
    HA3_LOG(DEBUG, "Begin Test!");

    auto requestPtr = prepareCorrectRequest();
    auto resultPtr = prepareResult();
    auto inputQuery = prepareComplicatedQuery();
    requestPtr->getQueryClause()->setRootQuery(inputQuery);

    _searchProcessorPtr->process(requestPtr, resultPtr);

    auto queryClause = requestPtr->getQueryClause();
    ASSERT_TRUE(queryClause);
    auto query = queryClause->getRootQuery();
    ASSERT_TRUE(query);
    ASSERT_EQ(string("AndQuery"), query->getQueryName());
    const std::vector<QueryPtr>* children = query->getChildQuery();
    ASSERT_TRUE(children);
    ASSERT_EQ((size_t)3, children->size());
}

TEST_F(QrsSearchProcessorTest, testFlattenWithAuxQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    auto requestPtr = prepareCorrectRequest();
    auto resultPtr = prepareResult();
    auto inputQuery = prepareComplicatedQuery();
    requestPtr->getQueryClause()->setRootQuery(inputQuery);

    auto inputAuxQuery = prepareComplicatedQuery();
    auto auxQueryClause = new AuxQueryClause();
    auxQueryClause->setRootQuery(inputAuxQuery);
    requestPtr->setAuxQueryClause(auxQueryClause);

    _searchProcessorPtr->process(requestPtr, resultPtr);

    {
        auto queryClause = requestPtr->getQueryClause();
        ASSERT_TRUE(queryClause);
        auto query = queryClause->getRootQuery();
        ASSERT_TRUE(query);
        ASSERT_EQ(string("AndQuery"), query->getQueryName());
        const std::vector<QueryPtr>* children = query->getChildQuery();
        ASSERT_TRUE(children);
        ASSERT_EQ((size_t)3, children->size());
    }

    {
        auto queryClause = requestPtr->getAuxQueryClause();
        ASSERT_TRUE(queryClause);
        auto query = queryClause->getRootQuery();
        ASSERT_TRUE(query);
        ASSERT_EQ(string("AndQuery"), query->getQueryName());
        const std::vector<QueryPtr>* children = query->getChildQuery();
        ASSERT_TRUE(children);
        ASSERT_EQ((size_t)3, children->size());
    }
}

TEST_F(QrsSearchProcessorTest, testFlattenMultiQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    auto requestPtr = prepareCorrectRequest();
    auto resultPtr = prepareResult();
    requestPtr->getQueryClause()->setRootQuery(prepareComplicatedQuery(), 0);
    requestPtr->getQueryClause()->setRootQuery(prepareComplicatedQuery(), 1);

    _searchProcessorPtr->process(requestPtr, resultPtr);

    QueryClause *queryClause = requestPtr->getQueryClause();
    ASSERT_TRUE(queryClause);
    Query *query0 = queryClause->getRootQuery(0);
    ASSERT_TRUE(query0);
    ASSERT_EQ(string("AndQuery"), query0->getQueryName());
    const std::vector<QueryPtr>* children = query0->getChildQuery();
    ASSERT_TRUE(children);
    ASSERT_EQ((size_t)3, children->size());

    Query *query1 = queryClause->getRootQuery(1);
    ASSERT_TRUE(query1);
    ASSERT_EQ(string("AndQuery"), query1->getQueryName());
    children = query1->getChildQuery();
    ASSERT_TRUE(children);
    ASSERT_EQ((size_t)3, children->size());

    ASSERT_TRUE(query1 != query0);
}

TEST_F(QrsSearchProcessorTest, testProcessWithFetchSummaryClause) {
    HA3_LOG(DEBUG, "Begin Test!");

    auto requestPtr = prepareCorrectRequest();
    auto fetchSummaryClause = new FetchSummaryClause;
    fetchSummaryClause->addGid("cluster1", GlobalIdentifier(5, 4, 3, 0, 0, 2));
    fetchSummaryClause->addGid("cluster2", GlobalIdentifier(9, 8, 7, 0, 0, 6));
    requestPtr->setFetchSummaryClause(fetchSummaryClause);

    Tracer tracer;
    tracer.setLevel("TRACE3");
    _searchProcessorPtr->setTracer(&tracer);
    auto resultPtr = prepareResult();
    _searchProcessorPtr->process(requestPtr, resultPtr);

    auto hits = resultPtr->getHits();
    ASSERT_TRUE(hits);
    ASSERT_EQ((uint32_t)2, hits->size());
    HitPtr hit1 = hits->getHit(0);
    ASSERT_EQ(string("cluster1.default"), hit1->getClusterName());
    GlobalIdentifier expectGlobalId1(5, 4, 3, INVALID_CLUSTERID, 0, 2);
    ASSERT_EQ(expectGlobalId1, hit1->getGlobalIdentifier());

    HitPtr hit2 = hits->getHit(1);
    ASSERT_EQ(string("cluster2.default"), hit2->getClusterName());
    GlobalIdentifier expectGlobalId2(9, 8, 7, INVALID_CLUSTERID, 0, 6);
    ASSERT_EQ(expectGlobalId2, hit2->getGlobalIdentifier());

    string traceStr = tracer.getTraceInfo();
    ASSERT_TRUE(traceStr.find("TRACE3") != string::npos);
    ASSERT_TRUE(traceStr.find("Cluster[cluster1.default] FetchSummaryCluster[cluster1.default]") != string::npos);
    ASSERT_TRUE(traceStr.find("Cluster[cluster2.default] FetchSummaryCluster[cluster2.default]") != string::npos);
    ASSERT_TRUE(traceStr.find("GlobalIdentifier: [" +
                                 hit1->getGlobalIdentifier().toString()+ "]") != string::npos);
    ASSERT_TRUE(traceStr.find("GlobalIdentifier: [" +
                                 hit2->getGlobalIdentifier().toString()+ "]") != string::npos);
}

TEST_F(QrsSearchProcessorTest, testProcessWithFetchSummaryClauseWithRawPk) {
    HA3_LOG(DEBUG, "Begin Test!");

    auto requestPtr = prepareCorrectRequest();
    requestPtr->getConfigClause()->setFetchSummaryType(BY_RAWPK);
    auto fetchSummaryClause = new FetchSummaryClause;
    string clusterName = "cluster.default";
    string rawPk1 = "rawPk1";
    string rawPk2 = "rawPk2";
    fetchSummaryClause->addRawPk(clusterName, rawPk1);
    fetchSummaryClause->addRawPk(clusterName, rawPk2);
    requestPtr->setFetchSummaryClause(fetchSummaryClause);

    ResultPtr resultPtr = prepareResult();
    _searchProcessorPtr->process(requestPtr, resultPtr);

    Hits *hits = resultPtr->getHits();
    ASSERT_TRUE(hits);
    ASSERT_EQ((uint32_t)2, hits->size());
    HitPtr hit1 = hits->getHit(0);

    HashFunctionBasePtr hashFuncPtr =
        HashFuncFactory::createHashFunc("HASH");
    assert(hashFuncPtr);
    hashid_t hashId1 = hashFuncPtr->getHashId(rawPk1);

    IE_NAMESPACE(util)::HashString hashFunc;
    primarykey_t primaryKey1 = 0;
    hashFunc.Hash(primaryKey1.value[1], rawPk1.c_str(), rawPk1.size());
    GlobalIdentifier expectGlobalId1(0, hashId1, 0, INVALID_CLUSTERID, primaryKey1, 0);
    ASSERT_EQ(clusterName, hit1->getClusterName());
    ASSERT_EQ(expectGlobalId1, hit1->getGlobalIdentifier());
}

common::RequestPtr QrsSearchProcessorTest::prepareCorrectRequest() {
    common::RequestPtr requestPtr(new common::Request());

    auto configClause = new ConfigClause();
    configClause->addClusterName("cluster1");
    requestPtr->setConfigClause(configClause);
    RequiredFields requiredField;
    auto queryClause = new QueryClause(new TermQuery("aaa bbb",
                    "default", requiredField, ""));
    requestPtr->setQueryClause(queryClause);

    return requestPtr;
}

ResultPtr QrsSearchProcessorTest::prepareResult() {
    ResultPtr resultPtr(new Result());

    return resultPtr;
}

Query* QrsSearchProcessorTest::prepareComplicatedQuery() {
    RequiredFields requiredField;
    QueryPtr queryPtr1(new TermQuery("aaa", "default", requiredField, ""));
    QueryPtr queryPtr2(new TermQuery("bbb", "default", requiredField, ""));
    QueryPtr leftQueryPtr(new AndQuery(""));
    leftQueryPtr->addQuery(queryPtr1);
    leftQueryPtr->addQuery(queryPtr2);
    QueryPtr rightQueryPtr(new TermQuery("ccc", "default", requiredField, ""));

    AndQuery *retQuery = new AndQuery("");
    retQuery->addQuery(leftQueryPtr);
    retQuery->addQuery(rightQueryPtr);

    return retQuery;
}

TEST_F(QrsSearchProcessorTest, testClone) {
    HA3_LOG(DEBUG, "Begin Test!");
    int32_t timeout = 10000000;
    QrsSearchProcessor processor(timeout, _clusterTableInfoMapPtr,
                                 _clusterConfigMapPtr, _clusterSorterManagersPtr,
                                 NO_COMPRESS, SearchTaskQueueRule());
    ASSERT_EQ(timeout, processor._qrsSearcherProcessDelegation->_connectionTimeout);

    QrsProcessorPtr processorCloned = processor.clone();
    QrsSearchProcessor *searchProcessor = dynamic_cast<QrsSearchProcessor*>(processorCloned.get());
    ASSERT_EQ(timeout, searchProcessor->_qrsSearcherProcessDelegation->_connectionTimeout);
    ASSERT_EQ(timeout, processor._qrsSearcherProcessDelegation->_connectionTimeout);
}

TEST_F(QrsSearchProcessorTest, testFillSummary)
{
    HA3_LOG(DEBUG, "Begin Test!");

    auto tracer = new common::Tracer;
    tracer->setLevel(0);
    _searchProcessorPtr->setTracer(tracer);

    monitor::TracerMetricsReporter reporter;
    reporter.setTracer(tracer);
    reporter.setRoleType(ROLE_QRS);
    reporter.setMetricsCollector(_searchProcessorPtr->
                                  getSessionMetricsCollector());
    _searchProcessorPtr->getSessionMetricsCollector()->setTracer(tracer);

    auto resultPtr = prepareResult();
    auto requestPtr = prepareCorrectRequest();
    _searchProcessorPtr->fillSummary(requestPtr, resultPtr);
    reporter.reportMetrics();
    string expectResult1("Begin fill summary");
    string notExpectResult1("Begin fill attribute");
    string result = tracer->getTraceInfo();

    ASSERT_TRUE(result.find(expectResult1) != string::npos);
    ASSERT_TRUE(result.find(notExpectResult1) == string::npos);
    delete tracer;
}

TEST_F(QrsSearchProcessorTest, testWithExceptionProcessor) {
    QrsSearchProcessor processor(1000000, _clusterTableInfoMapPtr,
                                 _clusterConfigMapPtr, _clusterSorterManagersPtr,
                                 NO_COMPRESS, SearchTaskQueueRule());
    ExceptionQrsProcessorPtr exceptionQrsProcessor(new ExceptionQrsProcessor);
    processor.setNextProcessor(exceptionQrsProcessor);
    auto requestPtr = prepareCorrectRequest();
    auto fetchSummaryClause = new FetchSummaryClause;
    fetchSummaryClause->addGid("cluster1", GlobalIdentifier(5, 4, 3, 0, 0, 2));
    fetchSummaryClause->addGid("cluster2", GlobalIdentifier(9, 8, 7, 0, 0, 6));
    requestPtr->setFetchSummaryClause(fetchSummaryClause);
    auto resultPtr = prepareResult();
    bool catchException = false;
    try {
        processor.process(requestPtr, resultPtr);
    } catch (...) {
        catchException = true;
    }
    ASSERT_TRUE(catchException);
}

TEST_F(QrsSearchProcessorTest, testFillSearcherCacheKey) {
    HA3_LOG(DEBUG, "Begin Test!");
    string requestStr = "config=config=cluster:mainse_searcher&&query=simple";
    auto requestPtr = RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    QrsSearchProcessor::fillSearcherCacheKey(requestPtr);
    auto searcherCacheClause = requestPtr->getSearcherCacheClause();
    ASSERT_TRUE(NULL == searcherCacheClause);
    requestPtr.reset();
    requestStr += "&&searcher_cache=use:yes";
    requestPtr = RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    QrsSearchProcessor::fillSearcherCacheKey(requestPtr);
    searcherCacheClause = requestPtr->getSearcherCacheClause();
    ASSERT_TRUE(searcherCacheClause);
    ASSERT_TRUE(0L != searcherCacheClause->getKey());
}

END_HA3_NAMESPACE(qrs);
