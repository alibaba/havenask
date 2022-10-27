#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/CommonDefine.h>
#include <ha3/common/Request.h>
#include <ha3/common/DocIdClause.h>
#include <ha3/common/Result.h>
#include <ha3/proto/BasicDefs.pb.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/service/HitSummarySchemaCache.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/common/MatchDocs.h>
#include <ha3/common/TermQuery.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/service/QrsResponse.h>
#include <ha3/service/QrsSearcherProcessDelegation.h>
#include <ha3/proto/ProtoClassUtil.h>
#include <ha3/common/test/ResultConstructor.h>
#include <arpc/ANetRPCController.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <ha3/util/HaCompressUtil.h>
#include <ha3/worker/test/MockSearchService.h>
#include <ha3/service/RpcContextUtil.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <sap_eagle/RpcContextImpl.h>
#include <ha3/turing/qrs/QrsRunGraphContext.h>

using namespace std;
using namespace arpc;
using namespace multi_call;
using namespace autil::legacy::json;
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(worker);
USE_HA3_NAMESPACE(search);
// USE_HA3_NAMESPACE(sorter);
BEGIN_HA3_NAMESPACE(service);

class FakeMultiCallReply : public multi_call::Reply {
public:
    std::vector<ResponsePtr> getResponses(size_t &lackCount) override {
        lackCount = 0;
        return _responseVec;
    }
    std::vector<ResponsePtr> _responseVec;
};

class QrsSearcherProcessDelegationTest : public TESTBASE {
public:
    QrsSearcherProcessDelegationTest();
    ~QrsSearcherProcessDelegationTest();

public:
    void setUp();
    void tearDown();
protected:
    common::ResultPtr createResultWithMatchDocs(int docCount, int hashId = 0);
    void destroyDocIdClauseMap(common::DocIdClauseMap docIdClauseMap);
    matchdoc::MatchDoc prepareMatchDoc(common::Ha3MatchDocAllocator *matchDocAllocator,
            docid_t docId, hashid_t hashId);
    common::MatchDocs* prepareMatchDocs(std::vector<docid_t> docIdVec,
            std::vector<hashid_t> hashIdVec);
    common::LevelClause* createLevelClause() const;
    template<typename T>
    multi_call::ResponsePtr prepareMultiCallResponse(
            T* docs, const string& bizName, multi_call::PartIdTy partId);
    void FakeSearch(const SearchParam &param, ReplyPtr &reply);
    void prepareResponseVec();
protected:
    common::RequestPtr _requestPtr;
    HitSummarySchemaCachePtr _hitSummarySchemaCache;
    monitor::SessionMetricsCollectorPtr _metricsCollectorPtr;
    autil::mem_pool::Pool *_pool;
    suez::turing::ClusterSorterManagersPtr _clusterSorterManagerPtr;
    MockSearchServicePtr _searchService;
    std::vector<multi_call::ResponsePtr> _responseVec;
    std::vector<std::string> _clusterNames;
    QrsSearcherProcessDelegation::ClusterIdMap _clusterIdMap;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, QrsSearcherProcessDelegationTest);

#define ASSERT_ONE_HIT(index, clusterName, clusterId, docId, hashId)     \
    ASSERT_EQ(string(clusterName), hits->getHit(index)->getClusterName()); \
    ASSERT_EQ((clusterid_t)clusterId, hits->getHit(index)->getClusterId()); \
    ASSERT_EQ((docid_t)docId, hits->getHit(index)->getDocId());  \
    ASSERT_EQ((hashid_t)hashId, hits->getHit(index)->getHashId()); \

QrsSearcherProcessDelegationTest::QrsSearcherProcessDelegationTest() {
    _metricsCollectorPtr.reset(new SessionMetricsCollector());
}

QrsSearcherProcessDelegationTest::~QrsSearcherProcessDelegationTest() {
}

 void QrsSearcherProcessDelegationTest::prepareResponseVec() {
     _clusterNames.push_back("simple.default");
     _clusterNames.push_back("simple2.default");
     _clusterNames.push_back("simple3.default");
     _clusterNames.push_back("simple4.default");
     for (auto it = _clusterNames.begin(); it != _clusterNames.end(); ++it)
     {
         const string &clusterName = *it;
         if (_clusterIdMap.find(clusterName) != _clusterIdMap.end()) {
             continue;
         }
         clusterid_t id = _clusterIdMap.size();
         _clusterIdMap[clusterName] = id;
     }

     // simple 0
     {
         vector<docid_t> docIdVec;
         vector<hashid_t> hashIdVec;
         docIdVec.push_back(0);
         docIdVec.push_back(1);
         docIdVec.push_back(2);

         hashIdVec.push_back(0);
         hashIdVec.push_back(0);
         hashIdVec.push_back(0);
         MatchDocs* matchDocs = prepareMatchDocs(docIdVec, hashIdVec);
         _responseVec.push_back(prepareMultiCallResponse(matchDocs, "simple.default", 0));
     }
     // simple 1
     {
         vector<docid_t> docIdVec;
         vector<hashid_t> hashIdVec;
         docIdVec.push_back(1);

         hashIdVec.push_back(1);
         MatchDocs* matchDocs = prepareMatchDocs(docIdVec, hashIdVec);
         _responseVec.push_back(prepareMultiCallResponse(matchDocs, "simple.default", 1));
     }

     // simple2 0
     {
         vector<docid_t> docIdVec;
         vector<hashid_t> hashIdVec;
         docIdVec.push_back(0);
         docIdVec.push_back(3);

         hashIdVec.push_back(0);
         hashIdVec.push_back(0);
         MatchDocs* matchDocs = prepareMatchDocs(docIdVec, hashIdVec);
         _responseVec.push_back(prepareMultiCallResponse(matchDocs, "simple2.default", 0));
     }

     // simple2 1
     {
         vector<docid_t> docIdVec;
         vector<hashid_t> hashIdVec;
         docIdVec.push_back(1);

         hashIdVec.push_back(1);
         MatchDocs* matchDocs = prepareMatchDocs(docIdVec, hashIdVec);
         _responseVec.push_back(prepareMultiCallResponse(matchDocs, "simple2.default", 1));
     }
     // simple2 2
     {
         vector<docid_t> docIdVec;
         vector<hashid_t> hashIdVec;
         docIdVec.push_back(2);

         hashIdVec.push_back(2);
         MatchDocs* matchDocs = prepareMatchDocs(docIdVec, hashIdVec);
         _responseVec.push_back(prepareMultiCallResponse(matchDocs, "simple2.default", 2));
     }
     // simple3 1
     {
         vector<docid_t> docIdVec;
         vector<hashid_t> hashIdVec;
         docIdVec.push_back(2);
         docIdVec.push_back(3);

         hashIdVec.push_back(1);
         hashIdVec.push_back(1);
         MatchDocs* matchDocs = prepareMatchDocs(docIdVec, hashIdVec);
         _responseVec.push_back(prepareMultiCallResponse(matchDocs, "simple3.default", 1));
     }
     // simple3 2
     {
         vector<docid_t> docIdVec;
         vector<hashid_t> hashIdVec;
         docIdVec.push_back(0);
         docIdVec.push_back(2);

         hashIdVec.push_back(2);
         hashIdVec.push_back(2);
         MatchDocs* matchDocs = prepareMatchDocs(docIdVec, hashIdVec);
         _responseVec.push_back(prepareMultiCallResponse(matchDocs, "simple3.default", 2));
     }
     // simple4 1
     {
         vector<docid_t> docIdVec;
         vector<hashid_t> hashIdVec;
         docIdVec.push_back(0);
         docIdVec.push_back(3);

         hashIdVec.push_back(1);
         hashIdVec.push_back(1);
         MatchDocs* matchDocs = prepareMatchDocs(docIdVec, hashIdVec);
         _responseVec.push_back(prepareMultiCallResponse(matchDocs, "simple4.default", 1));
     }
     // simple4 2
     {
         vector<docid_t> docIdVec;
         vector<hashid_t> hashIdVec;
         docIdVec.push_back(1);
         docIdVec.push_back(2);

         hashIdVec.push_back(2);
         hashIdVec.push_back(2);
         MatchDocs* matchDocs = prepareMatchDocs(docIdVec, hashIdVec);
         _responseVec.push_back(prepareMultiCallResponse(matchDocs, "simple4.default", 2));
     }
 }

 void QrsSearcherProcessDelegationTest::setUp() {
     HA3_LOG(DEBUG, "setUp!");
     _pool = new autil::mem_pool::Pool;
     _requestPtr.reset(new common::Request(_pool));
     QueryClause *queryClause = new QueryClause();
     RequiredFields requiredFields;
     queryClause->setRootQuery(new TermQuery("aaa", "indexName", requiredFields));
     _requestPtr->setQueryClause(queryClause);

     ConfigClause *configClause = new ConfigClause;
     configClause->setStartOffset(0);
     configClause->setHitCount(5);
     _requestPtr->setConfigClause(configClause);

     SortClause *sortClause = new SortClause;
     sortClause->setOriginalString("id; RANK");
     sortClause->addSortDescription(new suez::turing::AtomicSyntaxExpr(
                     "id", vt_int32, suez::turing::ATTRIBUTE_NAME), false);
     sortClause->addSortDescription(NULL, true);
     _requestPtr->setSortClause(sortClause);

     _hitSummarySchemaCache.reset(new HitSummarySchemaCache(
                     suez::turing::ClusterTableInfoMapPtr()));
     HitSummarySchemaPtr hitSummarySchema(new HitSummarySchema());
     hitSummarySchema->declareSummaryField("field1", ft_string, false);
     hitSummarySchema->declareSummaryField("field2", ft_string, false);
     hitSummarySchema->calcSignature();
     _hitSummarySchemaCache->addHitSummarySchema("simple.default",
             hitSummarySchema.get());
     _hitSummarySchemaCache->addHitSummarySchema("simple1.default",
             hitSummarySchema.get());
     _hitSummarySchemaCache->addHitSummarySchema("simple2.default",
             hitSummarySchema.get());
     _hitSummarySchemaCache->addHitSummarySchema("cluster1.default",
             hitSummarySchema.get());
     _hitSummarySchemaCache->addHitSummarySchema("cluster2.default",
             hitSummarySchema.get());
     _hitSummarySchemaCache->addHitSummarySchema("cluster_1.default",
             hitSummarySchema.get());
     _hitSummarySchemaCache->addHitSummarySchema("cluster_2.default",
             hitSummarySchema.get());
     _hitSummarySchemaCache->addHitSummarySchema("proxyCluster.default",
             hitSummarySchema.get());

     ClusterConfigInfo clusterConfigInfo;

     _clusterSorterManagerPtr.reset(new suez::turing::ClusterSorterManagers);
     auto sorterManager =
         suez::turing::SorterManager::create(".", suez::turing::SorterConfig());
     assert(sorterManager);

     _clusterSorterManagerPtr->insert(make_pair(string("simple.default"), sorterManager));
     _clusterSorterManagerPtr->insert(make_pair(string("proxyCluster.default"), sorterManager));

     _searchService.reset(new MockSearchService());
     prepareResponseVec();
     sap::RpcContextFactoryImpl::init(NULL, NULL);
}

isearch::turing::Ha3QrsRequestGenerator *castGeneartor(const multi_call::RequestGeneratorPtr &generator) {
    return dynamic_cast<isearch::turing::Ha3QrsRequestGenerator *>(generator.get());
}

void QrsSearcherProcessDelegationTest::FakeSearch(
        const multi_call::SearchParam &param,
        multi_call::ReplyPtr &reply)
{
    auto fakeReply = new FakeMultiCallReply;
    reply.reset(fakeReply);
    if (param.generatorVec.empty()) {
        return;
    }
    // auto rpcContext = sap::RpcContext::fromTLS();
    // auto benchMarkKey = rpcContext->getUserData(HA_RESERVED_BENCHMARK_KEY);
    // if (HA_RESERVED_BENCHMARK_VALUE_1 == benchMarkKey
    //         || HA_RESERVED_BENCHMARK_VALUE_2 == benchMarkKey)
    // {
    //     for (const auto &generator : param.generatorVec) {
    //         assert(generator->getFlowControlStrategy() ==
    //                QrsBiz::getBenchmarkBizName(generator->getBizName()));
    //     }
    // }

    auto type = castGeneartor(param.generatorVec[0])->_generateType;
    switch (type) {
    case isearch::turing::GT_NORMAL: {
        for (size_t i = 0; i < _responseVec.size(); ++i) {
            for (const auto &generator : param.generatorVec) {
                if (generator->getBizName() == _responseVec[i]->getBizName()) {
                    fakeReply->_responseVec.push_back(_responseVec[i]);
                }
            }
        }
        break;
    }
    case isearch::turing::GT_PID: {
        for (size_t i = 0; i < _responseVec.size(); ++i) {
            for (const auto &generator : param.generatorVec) {
                if (generator->getBizName() == _responseVec[i]->getBizName()) {
                    auto haGenerator = dynamic_cast<isearch::turing::Ha3QrsRequestGenerator *>(generator.get());
                    auto &partIdsMap = haGenerator->_requestPtr->getClusterClause()->getClusterPartIdsMap();
                    vector<uint16_t> hashIds;
                    auto iter = partIdsMap.find(_responseVec[i]->getBizName());
                    if (iter != partIdsMap.end()) {
                        hashIds = iter->second;
                    }
                    if (hashIds.empty()
                            || find(hashIds.begin(), hashIds.end(), _responseVec[i]->getPartId()) != hashIds.end())
                    {
                        fakeReply->_responseVec.push_back(_responseVec[i]);
                    }
                }
            }
        }
        break;
    }
    case isearch::turing::GT_SUMMARY: {
        for (const auto &generator : param.generatorVec) {
            auto hits = new Hits();
            auto haGenerator = dynamic_cast<isearch::turing::Ha3QrsRequestGenerator *>(generator.get());
            auto docIdClause = haGenerator->_docIdClause;
            GlobalIdVector gids;
            proto::Range range;
            range.set_from(0);
            range.set_to(0);
            docIdClause->getGlobalIdVector(range, gids);
            sort(gids.begin(), gids.end());
            for (const auto &gid : gids) {
                HitPtr hitPtr(new Hit(gid.getDocId()));
                hitPtr->setGlobalIdentifier(gid);
                hits->addHit(hitPtr);
            }
            hits->setSummaryFilled();
            int hitSize = hits->size();
            auto hitSummarySchema = new config::HitSummarySchema(NULL);
            hitSummarySchema->declareSummaryField("field1", ft_string, false);
            hitSummarySchema->declareSummaryField("field2", ft_string, false);
            hits->addHitSummarySchema(hitSummarySchema);
            for (int i = 0; i < hitSize; ++i) {
                ostringstream oss;
                oss << hits->getHit(i)->getDocId() << " ";
                auto summaryHit = new SummaryHit(hitSummarySchema, NULL);
                auto field1Value = oss.str() + "field1 summary";
                summaryHit->setFieldValue(0, field1Value.data(), field1Value.size());
                auto field2Value = oss.str() + "field2 summary";
                summaryHit->setFieldValue(1, field2Value.data(), field2Value.size());
                hits->getHit(i)->setSummaryHit(summaryHit);
            }
            fakeReply->_responseVec.push_back(prepareMultiCallResponse(hits, generator->getBizName(), 0));
        }
        break;
    }
    default:
        return;
    }
    param.closure->Run();
}

template<typename T>
multi_call::ResponsePtr QrsSearcherProcessDelegationTest::prepareMultiCallResponse(
        T* docs, const string& bizName, multi_call::PartIdTy partId)
{
    auto resultTensor = tensorflow::Tensor(tensorflow::DT_VARIANT, tensorflow::TensorShape({}));
    resultTensor.scalar<tensorflow::Variant>()() = isearch::turing::Ha3ResultVariant(ResultPtr(new Result(docs)), NULL);

    auto searchResponse = new suez::turing::GraphResponse();
    auto namedTensor = searchResponse->add_outputs();
    namedTensor->set_name(HA3_RESULT_TENSOR_NAME);
    resultTensor.AsProtoField(namedTensor->mutable_tensor());

    QrsResponsePtr response(new QrsResponse);
    response->_message.reset(searchResponse);
    response->setErrorCode(MULTI_CALL_ERROR_NONE);
    return response;
}

void QrsSearcherProcessDelegationTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    _clusterSorterManagerPtr.reset();
    delete _pool;
    _pool = NULL;
    sap::RpcContextFactoryImpl::release();
}

TEST_F(QrsSearcherProcessDelegationTest, testSearchPhase1OneCluster) {
    using namespace ::isearch::service;
    HA3_LOG(DEBUG, "begin test!");
    string clusterName = "simple";
    string address("address1");
//    searchService->setMatchDocs(matchDocs);
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele._runGraphContext.reset(new isearch::turing::QrsRunGraphContext(
                    isearch::turing::QrsRunGraphContextArgs()));
    dele._rpcContext = RpcContext::fromMap(map<string, string>());
    assert(dele._rpcContext);
    dele._rpcContext->putUserData(HA_RESERVED_BENCHMARK_KEY,
                                  HA_RESERVED_BENCHMARK_VALUE_1);
    dele._rpcContext->toTLS();
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);
    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");
    EXPECT_CALL(*_searchService, search(_, _))
        .Times(1)
        .WillOnce(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == false);
    ASSERT_TRUE(resultPtr);
    ASSERT_TRUE(!resultPtr->hasError());
    Hits *hits = resultPtr->getHits();
    ASSERT_EQ((uint32_t)4, hits->size());

    ASSERT_ONE_HIT(0, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(1, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(2, "simple.default", 0, 1, 1);
    ASSERT_ONE_HIT(3, "simple.default", 0, 2, 0);
}

TEST_F(QrsSearcherProcessDelegationTest, testSelectByHashIds) {
    using namespace ::isearch::service;
    HA3_LOG(DEBUG, "begin test!");
    string clusterName = "simple";
    string address("address1");
//    searchService->setMatchDocs(matchDocs);
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);
    ConfigClause *configClause = _requestPtr->getConfigClause();
    ClusterClause *clusterClause = new ClusterClause;
    vector<hashid_t> partids;
    partids.push_back(0);
    clusterClause->addClusterPartIds("simple", partids);
    _requestPtr->setClusterClause(clusterClause);
    configClause->addClusterName("simple");
    EXPECT_CALL(*_searchService, search(_, _))
        .Times(1)
        .WillOnce(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == false);
    ASSERT_TRUE(resultPtr);
    ASSERT_TRUE(!resultPtr->hasError());
    Hits *hits = resultPtr->getHits();
    ASSERT_EQ((uint32_t)3, hits->size());

    ASSERT_ONE_HIT(0, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(1, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(2, "simple.default", 0, 2, 0);
}

TEST_F(QrsSearcherProcessDelegationTest, testSearchPhase1MultiCluster) {
    using namespace ::isearch::service;
    HA3_LOG(DEBUG, "begin test!");

    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");
    configClause->addClusterName("simple2");

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(1)
        .WillOnce(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == false);
    ASSERT_TRUE(resultPtr);
    Hits *hits = resultPtr->getHits();
    ASSERT_EQ((uint32_t)5, hits->size());

    ASSERT_ONE_HIT(0, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(1, "simple2.default", 1, 0, 0);
    ASSERT_ONE_HIT(2, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(3, "simple2.default", 1, 1, 1);
    ASSERT_ONE_HIT(4, "simple.default", 0, 1, 1);
}

TEST_F(QrsSearcherProcessDelegationTest, testSearchBothLevelOldInterface) {
    HA3_LOG(DEBUG, "begin test!");

    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");

    LevelClause *levelClause = new LevelClause();
    levelClause->setLevelQueryType(BOTH_LEVEL);
    levelClause->setSecondLevelCluster("simple2");
    _requestPtr->setLevelClause(levelClause);

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(1)
        .WillOnce(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == false);
    ASSERT_TRUE(resultPtr);
    Hits *hits = resultPtr->getHits();

    ASSERT_EQ((uint32_t)5, hits->size());
    ASSERT_ONE_HIT(0, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(1, "simple2.default", 1, 0, 0);
    ASSERT_ONE_HIT(2, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(3, "simple2.default", 1, 1, 1);
    ASSERT_ONE_HIT(4, "simple.default", 0, 1, 1);

    ASSERT_TRUE(dele._metricsCollectorPtr->getMultiLevelLatency() < 0);
    ASSERT_TRUE(!dele._metricsCollectorPtr->isMultiLevelSearched());
    ASSERT_EQ((uint32_t)8, dele._metricsCollectorPtr->getRecievedMatchDocCount());
}

LevelClause* QrsSearcherProcessDelegationTest::createLevelClause() const {
    LevelClause *levelClause = new LevelClause();

    vector<vector<string> > clusterLists;
    vector<string> clusterList;
    clusterList.push_back("simple2");
    clusterList.push_back("simple3");
    clusterLists.push_back(clusterList);
    clusterList.clear();
    clusterList.push_back("simple4");
    clusterLists.push_back(clusterList);
    levelClause->setLevelClusters(clusterLists);

    return levelClause;
}

TEST_F(QrsSearcherProcessDelegationTest, testSearchBothLevelNewInterface) {
    HA3_LOG(DEBUG, "begin test!");

    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->setHitCount(8);
    configClause->addClusterName("simple");

    LevelClause* levelClause = createLevelClause();
    levelClause->setLevelQueryType(BOTH_LEVEL);
    _requestPtr->setLevelClause(levelClause);

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(1)
        .WillOnce(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == false);
    ASSERT_TRUE(resultPtr);
    Hits *hits = resultPtr->getHits();

    ASSERT_EQ((uint32_t)8, hits->size());
    ASSERT_ONE_HIT(0, "simple2.default", 1, 0, 0);
    ASSERT_ONE_HIT(1, "simple3.default", 2, 0, 2);
    ASSERT_ONE_HIT(2, "simple4.default", 3, 0, 1);
    ASSERT_ONE_HIT(3, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(4, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(5, "simple4.default", 3, 1, 2);
    ASSERT_ONE_HIT(6, "simple.default", 0, 1, 1);
    ASSERT_ONE_HIT(7, "simple2.default", 1, 1, 1);

    ASSERT_TRUE(dele._metricsCollectorPtr->getMultiLevelLatency() < 0);
    ASSERT_TRUE(!dele._metricsCollectorPtr->isMultiLevelSearched());
    ASSERT_EQ((uint32_t)16, dele._metricsCollectorPtr->getRecievedMatchDocCount());
 }

TEST_F(QrsSearcherProcessDelegationTest, testSearchOnlyFirstLevelOldInterface) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");

    LevelClause *levelClause = new LevelClause();
    levelClause->setLevelQueryType(ONLY_FIRST_LEVEL);
    levelClause->setSecondLevelCluster("simple2");
    levelClause->setMinDocs(100);
    _requestPtr->setLevelClause(levelClause);

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(1)
        .WillOnce(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == false);
    ASSERT_TRUE(resultPtr);
    Hits *hits = resultPtr->getHits();
    ASSERT_EQ((uint32_t)4, hits->size());

    ASSERT_ONE_HIT(0, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(1, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(2, "simple.default", 0, 1, 1);
    ASSERT_ONE_HIT(3, "simple.default", 0, 2, 0);

    ASSERT_TRUE(dele._metricsCollectorPtr->getMultiLevelLatency() < 0);
    ASSERT_TRUE(!dele._metricsCollectorPtr->isMultiLevelSearched());
    ASSERT_EQ((uint32_t)4, dele._metricsCollectorPtr->getRecievedMatchDocCount());
}

TEST_F(QrsSearcherProcessDelegationTest, testSearchOnlyFirstLevelNewInterface) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");

    LevelClause *levelClause = createLevelClause();
    levelClause->setLevelQueryType(ONLY_FIRST_LEVEL);
    levelClause->setMinDocs(100);
    _requestPtr->setLevelClause(levelClause);

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(1)
        .WillOnce(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == false);
    ASSERT_TRUE(resultPtr);
    Hits *hits = resultPtr->getHits();
    ASSERT_EQ((uint32_t)4, hits->size());

    ASSERT_ONE_HIT(0, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(1, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(2, "simple.default", 0, 1, 1);
    ASSERT_ONE_HIT(3, "simple.default", 0, 2, 0);

    ASSERT_TRUE(dele._metricsCollectorPtr->getMultiLevelLatency() < 0);
    ASSERT_TRUE(!dele._metricsCollectorPtr->isMultiLevelSearched());
    ASSERT_EQ((uint32_t)4, dele._metricsCollectorPtr->getRecievedMatchDocCount());
}

TEST_F(QrsSearcherProcessDelegationTest, testNoNeedSearchSecondLevel) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");

    LevelClause *levelClause = new LevelClause();
    levelClause->setLevelQueryType(CHECK_FIRST_LEVEL);
    levelClause->setSecondLevelCluster("simple2");
    levelClause->setMinDocs(3);
    _requestPtr->setLevelClause(levelClause);

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(1)
        .WillOnce(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == false);
    ASSERT_TRUE(resultPtr);
    Hits *hits = resultPtr->getHits();
    ASSERT_EQ((uint32_t)4, hits->size());

    ASSERT_ONE_HIT(0, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(1, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(2, "simple.default", 0, 1, 1);
    ASSERT_ONE_HIT(3, "simple.default", 0, 2, 0);

    ASSERT_TRUE(dele._metricsCollectorPtr->getMultiLevelLatency() < 0);
    ASSERT_TRUE(!dele._metricsCollectorPtr->isMultiLevelSearched());
    ASSERT_EQ((uint32_t)4, dele._metricsCollectorPtr->getRecievedMatchDocCount());
}

TEST_F(QrsSearcherProcessDelegationTest, testSearchNoNeedSecondLevelNewInterface) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");

    LevelClause *levelClause = createLevelClause();
    levelClause->setLevelQueryType(CHECK_FIRST_LEVEL);
    levelClause->setMinDocs(3);
    _requestPtr->setLevelClause(levelClause);

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(1)
        .WillOnce(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == false);
    ASSERT_TRUE(resultPtr);
    Hits *hits = resultPtr->getHits();
    ASSERT_EQ((uint32_t)4, hits->size());

    ASSERT_ONE_HIT(0, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(1, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(2, "simple.default", 0, 1, 1);
    ASSERT_ONE_HIT(3, "simple.default", 0, 2, 0);

    ASSERT_TRUE(dele._metricsCollectorPtr->getMultiLevelLatency() < 0);
    ASSERT_TRUE(!dele._metricsCollectorPtr->isMultiLevelSearched());
    ASSERT_EQ((uint32_t)4, dele._metricsCollectorPtr->getRecievedMatchDocCount());
}

TEST_F(QrsSearcherProcessDelegationTest, testSearchSecondLevelNewInterface) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");

    LevelClause *levelClause = createLevelClause();
    levelClause->setLevelQueryType(CHECK_FIRST_LEVEL);
    levelClause->setMinDocs(5);
    _requestPtr->setLevelClause(levelClause);

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(2)
        .WillRepeatedly(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == true);
    ASSERT_TRUE(resultPtr);
    Hits *hits = resultPtr->getHits();

    ASSERT_EQ((uint32_t)5, hits->size());
    ASSERT_ONE_HIT(0, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(1, "simple2.default", 1, 0, 0);
    ASSERT_ONE_HIT(2, "simple3.default", 2, 0, 2);
    ASSERT_ONE_HIT(3, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(4, "simple2.default", 1, 1, 1);

    ASSERT_TRUE(dele._metricsCollectorPtr->getMultiLevelLatency() >= 0);
    ASSERT_TRUE(dele._metricsCollectorPtr->isMultiLevelSearched());
    ASSERT_EQ((uint32_t)12, dele._metricsCollectorPtr->getRecievedMatchDocCount());
}

TEST_F(QrsSearcherProcessDelegationTest, testSearchThirdLevelNewInterface) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");
    configClause->setHitCount(7);

    LevelClause *levelClause = createLevelClause();
    levelClause->setLevelQueryType(CHECK_FIRST_LEVEL);
    levelClause->setMinDocs(13);
    _requestPtr->setLevelClause(levelClause);

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(3)
        .WillRepeatedly(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == true);
    ASSERT_TRUE(resultPtr);
    Hits *hits = resultPtr->getHits();

    ASSERT_EQ((uint32_t)7, hits->size());
    ASSERT_ONE_HIT(0, "simple2.default", 1, 0, 0);
    ASSERT_ONE_HIT(1, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(2, "simple4.default", 3, 0, 1);
    ASSERT_ONE_HIT(3, "simple3.default", 2, 0, 2);
    ASSERT_ONE_HIT(4, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(5, "simple.default", 0, 1, 1);
    ASSERT_ONE_HIT(6, "simple2.default", 1, 1, 1);

    ASSERT_TRUE(dele._metricsCollectorPtr->getMultiLevelLatency() >= 0);
    ASSERT_TRUE(dele._metricsCollectorPtr->isMultiLevelSearched());
    ASSERT_EQ((uint32_t)16, dele._metricsCollectorPtr->getRecievedMatchDocCount());
}

TEST_F(QrsSearcherProcessDelegationTest, testSearchSecondLevel) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");

    LevelClause *levelClause = new LevelClause();
    levelClause->setLevelQueryType(CHECK_FIRST_LEVEL);
    levelClause->setSecondLevelCluster("simple2");
    levelClause->setMinDocs(6);
    _requestPtr->setLevelClause(levelClause);

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(2)
        .WillRepeatedly(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));

    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    ResultPtr resultPtr = dele.search(_requestPtr);
    ASSERT_TRUE(dele._hasReSearch == true);
    ASSERT_TRUE(resultPtr);
    Hits *hits = resultPtr->getHits();
    ASSERT_EQ((uint32_t)5, hits->size());

    ASSERT_ONE_HIT(0, "simple.default", 0, 0, 0);
    ASSERT_ONE_HIT(1, "simple2.default", 1, 0, 0);
    ASSERT_ONE_HIT(2, "simple.default", 0, 1, 0);
    ASSERT_ONE_HIT(3, "simple2.default", 1, 1, 1);
    ASSERT_ONE_HIT(4, "simple.default", 0, 1, 1);

    ASSERT_TRUE(dele._metricsCollectorPtr->getMultiLevelLatency() >= 0);
    ASSERT_TRUE(dele._metricsCollectorPtr->isMultiLevelSearched());
    ASSERT_EQ((uint32_t)8, dele._metricsCollectorPtr->getRecievedMatchDocCount());
}

TEST_F(QrsSearcherProcessDelegationTest, testCreateDocIdClauseMapNullPtr) {
    HA3_LOG(DEBUG, "Begin Test!");
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    ResultPtr resultPtr;
    vector<HitSummarySchemaPtr> hitSummarySchemas;
    DocIdClauseMap docIdClauseMap = dele.createDocIdClauseMap(_requestPtr, resultPtr, hitSummarySchemas);
    ASSERT_EQ((size_t)0, docIdClauseMap.size());
    ASSERT_EQ((size_t)0, hitSummarySchemas.size());
}

TEST_F(QrsSearcherProcessDelegationTest, testCreateDocIdClauseMapEmptyResult) {
    HA3_LOG(DEBUG, "Begin Test!");
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    ResultPtr resultPtr(new Result);
    vector<HitSummarySchemaPtr> hitSummarySchemas;
    DocIdClauseMap docIdClauseMap = dele.createDocIdClauseMap(_requestPtr, resultPtr, hitSummarySchemas);
    ASSERT_EQ((size_t)0, docIdClauseMap.size());
    ASSERT_EQ((size_t)0, hitSummarySchemas.size());
}

TEST_F(QrsSearcherProcessDelegationTest, testCreateDocIdClauseMapOneResult) {
    HA3_LOG(DEBUG, "Begin Test!");
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    ResultPtr resultPtr(new Result);
    MatchDocs *matchDocs = new MatchDocs;
    common::Ha3MatchDocAllocatorPtr matchDocAllocator(new common::Ha3MatchDocAllocator(_pool));
    matchDocs->setMatchDocAllocator(matchDocAllocator);
    auto hashIdRef = matchDocAllocator->declare<hashid_t>(HASH_ID_REF, common::HA3_GLOBAL_INFO_GROUP,
            SL_PROXY);
    auto matchDoc = matchDocAllocator->allocate(1);
    hashIdRef->set(matchDoc, (hashid_t)0);
    matchDocs->addMatchDoc(matchDoc);
    resultPtr->setMatchDocs(matchDocs);
    vector<string> clisterList;
    clisterList.push_back(string("simple.default"));
    resultPtr->setClusterInfo("simple.default", (clusterid_t)0);
    dele.convertMatchDocsToHits(_requestPtr, resultPtr, clisterList);
    vector<HitSummarySchemaPtr> hitSummarySchemas;
    DocIdClauseMap docIdClauseMap = dele.createDocIdClauseMap(_requestPtr,
            resultPtr, hitSummarySchemas);
    ASSERT_EQ((size_t)1, docIdClauseMap.size());
    ASSERT_EQ((size_t)1, hitSummarySchemas.size());

    DocIdClause *docIdClause = docIdClauseMap.begin()->second;

    const GlobalIdVector &globalIdVector = docIdClause->getGlobalIdVector();
    ASSERT_EQ((size_t)1, globalIdVector.size());

    GlobalIdVector gids;
    proto::Range range;
    range.set_from(0);
    range.set_to(0);
    docIdClause->getGlobalIdVector(range, 0, gids);
    ASSERT_EQ((size_t)1, gids.size());
    ASSERT_EQ((docid_t)1, gids[0].getDocId());

    const TermVector &terms = docIdClause->getTermVector();
    ASSERT_EQ((size_t)1, terms.size());
    RequiredFields requiredFields;
    ASSERT_EQ(Term("aaa", "indexName", requiredFields), terms[0]);

    destroyDocIdClauseMap(docIdClauseMap);
}

TEST_F(QrsSearcherProcessDelegationTest, testCreateDocIdClauseMapFourResult) {
    HA3_LOG(DEBUG, "Begin Test!");
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    ResultPtr resultPtr(new Result);
    MatchDocs *matchDocs = new MatchDocs;

    common::Ha3MatchDocAllocatorPtr matchDocAllocator(new common::Ha3MatchDocAllocator(_pool));
    matchDocs->setMatchDocAllocator(matchDocAllocator);
    auto hashIdRef = matchDocAllocator->declare<hashid_t>(HASH_ID_REF, common::HA3_GLOBAL_INFO_GROUP,
            SL_PROXY);
    matchdoc::MatchDoc matchDoc = matchDocAllocator->allocate(0);
    hashIdRef->set(matchDoc, (hashid_t)0);
    matchDocs->addMatchDoc(matchDoc);

    matchDoc = matchDocAllocator->allocate(1);
    hashIdRef->set(matchDoc, (hashid_t)0);
    matchDocs->addMatchDoc(matchDoc);

    matchDoc = matchDocAllocator->allocate(2);
    hashIdRef->set(matchDoc, (hashid_t)1);
    matchDocs->addMatchDoc(matchDoc);

    matchDoc = matchDocAllocator->allocate(3);
    hashIdRef->set(matchDoc, (hashid_t)1);
    matchDocs->addMatchDoc(matchDoc);

    resultPtr->setMatchDocs(matchDocs);
    vector<string> clisterList;
    clisterList.push_back(string("simple.default"));
    resultPtr->setClusterInfo("simple.default", (clusterid_t)0);
    dele.convertMatchDocsToHits(_requestPtr, resultPtr, clisterList);

    vector<HitSummarySchemaPtr> hitSummarySchemas;
    DocIdClauseMap docIdClauseMap = dele.createDocIdClauseMap(_requestPtr, resultPtr, hitSummarySchemas);
    ASSERT_EQ((size_t)1, docIdClauseMap.size());
    ASSERT_EQ((size_t)1, hitSummarySchemas.size());

    DocIdClause *docIdClause = docIdClauseMap.begin()->second;
    ASSERT_TRUE(docIdClause);

    const GlobalIdVector &globalIdVector = docIdClause->getGlobalIdVector();
    ASSERT_EQ((size_t)4, globalIdVector.size());

    GlobalIdVector gids;
    proto::Range range;
    range.set_from(0);
    range.set_to(0);
    docIdClause->getGlobalIdVector(range, 0, gids);
    ASSERT_EQ((size_t)2, gids.size());
    ASSERT_EQ((docid_t)0, gids[0].getDocId());
    ASSERT_EQ((docid_t)1, gids[1].getDocId());

    gids.clear();
    range.set_from(1);
    range.set_to(1);
    docIdClause->getGlobalIdVector(range, 0, gids);
    ASSERT_EQ((size_t)2, gids.size());
    ASSERT_EQ((docid_t)2, gids[0].getDocId());
    ASSERT_EQ((docid_t)3, gids[1].getDocId());

    const TermVector &terms = docIdClause->getTermVector();
    ASSERT_EQ((size_t)1, terms.size());
    RequiredFields requiredFields;
    ASSERT_EQ(Term("aaa", "indexName", requiredFields), terms[0]);

    destroyDocIdClauseMap(docIdClauseMap);
}

TEST_F(QrsSearcherProcessDelegationTest, testCreateDocIdClauseMapWithSummaryExist) {
    HA3_LOG(DEBUG, "Begin Test!");
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    ResultPtr resultPtr(new Result);
    MatchDocs *matchDocs = new MatchDocs;
    common::Ha3MatchDocAllocatorPtr matchDocAllocator(new common::Ha3MatchDocAllocator(_pool));
    matchDocs->setMatchDocAllocator(matchDocAllocator);
    auto hashIdRef = matchDocAllocator->declare<hashid_t>(HASH_ID_REF, common::HA3_GLOBAL_INFO_GROUP,
            SL_PROXY);
    matchdoc::MatchDoc matchDoc = matchDocAllocator->allocate(1);
    hashIdRef->set(matchDoc, (hashid_t)0);
    matchDocs->addMatchDoc(matchDoc);
    resultPtr->setMatchDocs(matchDocs);
    vector<string> clusterList;
    clusterList.push_back(string("simple.default"));
    resultPtr->setClusterInfo("simple.default", (clusterid_t)0);
    dele.convertMatchDocsToHits(_requestPtr, resultPtr, clusterList);
    Hits *hits = resultPtr->getHits();
    ASSERT_TRUE(hits);
    hits->setSummaryFilled();

    vector<HitSummarySchemaPtr> hitSummarySchemas;
    DocIdClauseMap docIdClauseMap = dele.createDocIdClauseMap(_requestPtr, resultPtr, hitSummarySchemas);
    ASSERT_EQ((size_t)0, docIdClauseMap.size());
    ASSERT_EQ((size_t)0, hitSummarySchemas.size());
}

TEST_F(QrsSearcherProcessDelegationTest, testConvertMatchDocsToHits)
{
    HA3_LOG(DEBUG, "Begin Test!");
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);

    ResultPtr resultPtr = createResultWithMatchDocs(13);
    vector<string> clusterList;
    clusterList.push_back(string("simple.default"));
    dele.convertMatchDocsToHits(_requestPtr, resultPtr, clusterList);
    resultPtr->setClusterInfo("simple.default", (clusterid_t)0);
    ASSERT_TRUE(!resultPtr->getMatchDocs());
    Hits *hits = resultPtr->getHits();
    ASSERT_TRUE(hits);
    ASSERT_EQ((uint32_t)5, hits->size());
    ASSERT_TRUE(!hits->hasSummary());
    ASSERT_EQ((uint32_t)123, hits->totalHits());

    for (int i = 0; i < 5; ++i) {
        HitPtr hitPtr = hits->getHit(i);
        ASSERT_EQ((docid_t)(12345 - i), hitPtr->getDocId());
        ASSERT_EQ((hashid_t)0, hitPtr->getHashId());
        ASSERT_TRUE(!hitPtr->hasPrimaryKey());
        ASSERT_EQ((uint32_t)2, hitPtr->getSortExprCount());
        ostringstream id_oss;
        id_oss << i * 3;
        ASSERT_EQ(id_oss.str(), hitPtr->getSortExprValue(0));
        ostringstream type_oss;
        type_oss << i + 0.5f;
        ASSERT_EQ(type_oss.str(), hitPtr->getSortExprValue(1));
    }

    resultPtr = createResultWithMatchDocs(1);
    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->setActualHitsLimit(50);
    resultPtr->setActualMatchDocs(49);
    dele.convertMatchDocsToHits(_requestPtr, resultPtr, clusterList);
    ASSERT_EQ((uint32_t)49, resultPtr->getHits()->totalHits());
    resultPtr = createResultWithMatchDocs(1);
    resultPtr->setActualMatchDocs(50);
    dele.convertMatchDocsToHits(_requestPtr, resultPtr, clusterList);
    ASSERT_EQ((uint32_t)123, resultPtr->getHits()->totalHits());
}

ResultPtr QrsSearcherProcessDelegationTest::createResultWithMatchDocs(int docCount, int hashId)
{
    MatchDocs *matchDocs = new MatchDocs;
    Result *result = new Result(matchDocs);
    ResultPtr resultPtr(result);

    ResultConstructor resultConstructor;
    ostringstream oss;
    for (int i = 0; i < docCount; ++i) {
        if (i) {
            oss << " # ";
        }
        oss << 12345 - i << "," << hashId; //docid && hashid
        oss << ",";
        oss << i * 3 << "," << i + 0.5f; // id && type
    }
    HA3_LOG(ERROR, "ss: [%s]", oss.str().c_str());
    resultConstructor.fillMatchDocs2(result->getMatchDocs(), 2, 0, 123, _pool, oss.str());
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    matchdoc::ReferenceVector sortReferVec;
    resultConstructor.getSortReferences(allocatorPtr.get(), sortReferVec);

    vector<SortExprMeta> sortExprMetaVec;
    for (auto it = sortReferVec.begin(); it != sortReferVec.end(); ++it) {
        SortExprMeta sortExprMeta;
        sortExprMeta.sortExprName = (*it)->getName();
        sortExprMeta.sortRef = *it;
        sortExprMeta.sortFlag = false;
        sortExprMetaVec.push_back(sortExprMeta);
    }
    resultPtr->setSortExprMeta(sortExprMetaVec);
    return resultPtr;
}


TEST_F(QrsSearcherProcessDelegationTest, testFillSummary)
{
    HA3_LOG(DEBUG, "Begin Test!");
    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->setHitCount(20);

    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele._clusterNames = _clusterNames;
    dele._clusterIdMap = _clusterIdMap;
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    int docCount = 10;
    ResultPtr resultPtr = createResultWithMatchDocs(docCount/2);
    //hash id 4
    ResultPtr resultPtr2 = createResultWithMatchDocs(docCount/2, 4);
    MatchDocs* matchDocs = resultPtr-> getMatchDocs();
    MatchDocs* matchDocs2 = resultPtr2-> getMatchDocs();
    auto &docVec1 = matchDocs->getMatchDocsVect();
    auto &docVec2 = matchDocs2->getMatchDocsVect();
    const auto &allocator1 = matchDocs->getMatchDocAllocator();
    const auto &allocator2 = matchDocs2->getMatchDocAllocator();
    ASSERT_TRUE(allocator1->mergeAllocator(allocator2.get(), docVec2, docVec1));
    docVec2.clear();
    ASSERT_EQ((uint32_t)docCount, matchDocs->size());
    vector<string> clusterList;
    clusterList.push_back(string("simple.default"));
    dele.convertMatchDocsToHits(_requestPtr, resultPtr, clusterList);
    resultPtr->setClusterInfo("simple.default", (clusterid_t)0);
    Hits *hits = resultPtr->getHits();
    ASSERT_TRUE(hits);
    ASSERT_EQ((uint32_t)docCount, hits->size());
    ASSERT_TRUE(!hits->hasSummary());
    for (size_t i = 0; i < hits->size(); i++) {
        hits->getHit(i)->setPosition(i);
    }

    EXPECT_CALL(*_searchService, search(_, _))
        .Times(1)
        .WillRepeatedly(Invoke(std::bind(&QrsSearcherProcessDelegationTest::FakeSearch,
                                this, placeholders::_1, placeholders::_2)));

    dele.fillSummary(_requestPtr, resultPtr);
    ASSERT_TRUE(dele._hasReSearch == false);
    ASSERT_EQ((uint32_t)docCount, hits->size());
    ASSERT_TRUE(hits->hasSummary());
    for (int i = 0; i < docCount / 2; ++i) {
        HitPtr hitPtr = hits->getHit(i);
        docid_t docid = hitPtr->getDocId();

        ASSERT_EQ((uint32_t)2, hitPtr->getSummaryCount());
        ostringstream oss;
        oss << docid << " ";
        ASSERT_EQ(oss.str() + "field1 summary",
                             hitPtr->getSummaryValue("field1"));
        ASSERT_EQ(oss.str() + "field2 summary",
                             hitPtr->getSummaryValue("field2"));
    }

    for (int i = docCount / 2; i < docCount; ++i) {
        HitPtr hitPtr = hits->getHit(i);
        ASSERT_EQ((uint32_t)0, hitPtr->getSummaryCount());
    }
}

TEST_F(QrsSearcherProcessDelegationTest, testCreateDocIdClauseMapOneCluster) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);

    Hits *hits = new Hits;
    hits->addHit(HitPtr(new Hit((docid_t)0, 0, "cluster_1.default")));
    hits->addHit(HitPtr(new Hit((docid_t)1, 1, "cluster_1.default")));
    ResultPtr resultPtr(new Result(hits));

    vector<HitSummarySchemaPtr> hitSummarySchemas;
    DocIdClauseMap docIdClauseMap = dele.createDocIdClauseMap(_requestPtr, resultPtr, hitSummarySchemas);
    ASSERT_EQ((size_t)1, docIdClauseMap.size());
    ASSERT_EQ((size_t)1, hitSummarySchemas.size());

    DocIdClause *docIdClause = docIdClauseMap["cluster_1.default"];
    const GlobalIdVector &globalIdVector = docIdClause->getGlobalIdVector();
    ASSERT_EQ((size_t)2, globalIdVector.size());

    GlobalIdVector gids;
    proto::Range range;
    range.set_from(0);
    range.set_to(0);
    docIdClause->getGlobalIdVector(range, 0, gids);
    ASSERT_EQ((size_t)1, gids.size());
    ASSERT_EQ((docid_t)0, gids[0].getDocId());

    gids.clear();
    range.set_from(1);
    range.set_to(1);
    docIdClause->getGlobalIdVector(range, 0, gids);
    ASSERT_EQ((size_t)1, gids.size());
    ASSERT_EQ((docid_t)1, gids[0].getDocId());

    destroyDocIdClauseMap(docIdClauseMap);
}

TEST_F(QrsSearcherProcessDelegationTest, testCreateDocIdClauseMapTwoCluster) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);

    Hits *hits = new Hits;
    hits->addHit(HitPtr(new Hit((docid_t)0, 0, "cluster_1.default")));
    hits->addHit(HitPtr(new Hit((docid_t)1, 1, "cluster_1.default")));
    hits->addHit(HitPtr(new Hit((docid_t)2, 0, "cluster_2.default")));
    hits->addHit(HitPtr(new Hit((docid_t)3, 1, "cluster_2.default")));
    ResultPtr resultPtr(new Result(hits));

    vector<HitSummarySchemaPtr> hitSummarySchemas;
    DocIdClauseMap docIdClauseMap = dele.createDocIdClauseMap(_requestPtr, resultPtr, hitSummarySchemas);
    ASSERT_EQ((size_t)2, docIdClauseMap.size());
    ASSERT_EQ((size_t)2, hitSummarySchemas.size());

    DocIdClause *docIdClause = docIdClauseMap["cluster_1.default"];
    ASSERT_TRUE(docIdClause);

    const GlobalIdVector &globalIdVector = docIdClause->getGlobalIdVector();
    ASSERT_EQ((size_t)2, globalIdVector.size());

    GlobalIdVector gids;
    Range range;
    range.set_from(0);
    range.set_to(0);
    docIdClause->getGlobalIdVector(range, 0, gids);
    ASSERT_EQ((size_t)1, gids.size());
    ASSERT_EQ((docid_t)0, gids[0].getDocId());

    gids.clear();
    range.set_from(1);
    range.set_to(1);
    docIdClause->getGlobalIdVector(range, 0, gids);
    ASSERT_EQ((size_t)1, gids.size());
    ASSERT_EQ((docid_t)1, gids[0].getDocId());

    docIdClause = docIdClauseMap["cluster_2.default"];
    ASSERT_TRUE(docIdClause);

    const GlobalIdVector &globalIdVector1 = docIdClause->getGlobalIdVector();
    ASSERT_EQ((size_t)2, globalIdVector1.size());

    gids.clear();
    range.set_from(0);
    range.set_to(0);
    docIdClause->getGlobalIdVector(range, 0, gids);
    ASSERT_EQ((size_t)1, gids.size());
    ASSERT_EQ((docid_t)2, gids[0].getDocId());

    gids.clear();
    range.set_from(1);
    range.set_to(1);
    docIdClause->getGlobalIdVector(range, 0, gids);
    ASSERT_EQ((size_t)1, gids.size());
    ASSERT_EQ((docid_t)3, gids[0].getDocId());

    destroyDocIdClauseMap(docIdClauseMap);
}

void QrsSearcherProcessDelegationTest::destroyDocIdClauseMap(DocIdClauseMap docIdClauseMap) {
    for (DocIdClauseMap::iterator it = docIdClauseMap.begin();
         it != docIdClauseMap.end(); ++it)
    {
        delete it->second;
    }
    docIdClauseMap.clear();
}

matchdoc::MatchDoc QrsSearcherProcessDelegationTest::
prepareMatchDoc(common::Ha3MatchDocAllocator *matchDocAllocator, docid_t docId, hashid_t hashId) {
    matchdoc::MatchDoc matchDoc = matchDocAllocator->allocate(docId);
    auto hashIdRef = matchDocAllocator->findReference<hashid_t>(HASH_ID_REF);
    hashIdRef->set(matchDoc, hashId);
    return matchDoc;
}

MatchDocs* QrsSearcherProcessDelegationTest::
prepareMatchDocs(vector<docid_t> docIdVec, vector<hashid_t> hashIdVec)
{
    MatchDocs *matchDocs = new MatchDocs;
    matchDocs->setTotalMatchDocs(4);
    matchDocs->setActualMatchDocs(docIdVec.size());
    common::Ha3MatchDocAllocator *matchDocAllocator = new common::Ha3MatchDocAllocator(_pool);

    auto ref1 = matchDocAllocator->declare<int32_t>("id", SL_CACHE);
    auto ref2 = matchDocAllocator->declare<score_t>(suez::turing::SCORE_REF, SL_CACHE);

    matchDocAllocator->declare<hashid_t>(HASH_ID_REF, common::HA3_GLOBAL_INFO_GROUP,
            SL_PROXY);
    assert(docIdVec.size() == hashIdVec.size());
    for (size_t i = 0; i < docIdVec.size(); i++) {
        docid_t docId = docIdVec[i];
        hashid_t hashId = hashIdVec[i];
        matchdoc::MatchDoc matchDoc = prepareMatchDoc(matchDocAllocator, docId, hashId);
        ref1->set(matchDoc, 0);
        ref2->set(matchDoc, 0.0f);
        matchDocs->addMatchDoc(matchDoc);
    }

    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
    matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);
    return matchDocs;
}

TEST_F(QrsSearcherProcessDelegationTest, testConstructor) {
    QrsSearcherProcessDelegation *dele1 = NULL;
    {
      QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
      ASSERT_EQ(_hitSummarySchemaCache, dele._hitSummarySchemaCache);
      ASSERT_EQ(_searchService, dele._searchService);
      ASSERT_EQ(QRS_ARPC_CONNECTION_TIMEOUT, dele._connectionTimeout);
      ASSERT_EQ(NO_COMPRESS, dele._requestCompressType);
      ASSERT_EQ(DEFAULT_TASK_QUEUE_NAME,
                dele._searchTaskqueueRule.phaseOneTaskQueue);
      ASSERT_EQ(DEFAULT_TASK_QUEUE_NAME,
                dele._searchTaskqueueRule.phaseTwoTaskQueue);
      ASSERT_FALSE(dele._qrsClosure);
      ASSERT_FALSE(dele._lackResult);
      ASSERT_FALSE(dele._memPool);
      ASSERT_FALSE(dele._hasReSearch);
      ASSERT_FALSE(dele._tracer);
      ASSERT_FALSE(dele._timeoutTerminator);
      ASSERT_EQ(multi_call::INVALID_VERSION_ID, dele._phase1Version);
      ASSERT_EQ(multi_call::INVALID_VERSION_ID, dele._phase2Version);
      dele1 = new QrsSearcherProcessDelegation(dele);
    }
    ASSERT_EQ(_hitSummarySchemaCache, dele1->_hitSummarySchemaCache);
    ASSERT_EQ(_searchService, dele1->_searchService);
    ASSERT_EQ(QRS_ARPC_CONNECTION_TIMEOUT, dele1->_connectionTimeout);
    ASSERT_EQ(NO_COMPRESS, dele1->_requestCompressType);
    ASSERT_EQ(DEFAULT_TASK_QUEUE_NAME, dele1->_searchTaskqueueRule.phaseOneTaskQueue);
    ASSERT_EQ(DEFAULT_TASK_QUEUE_NAME, dele1->_searchTaskqueueRule.phaseTwoTaskQueue);
    ASSERT_FALSE(dele1->_qrsClosure);
    ASSERT_FALSE(dele1->_lackResult);
    ASSERT_FALSE(dele1->_memPool);
    ASSERT_FALSE(dele1->_hasReSearch);
    ASSERT_FALSE(dele1->_tracer);
    ASSERT_FALSE(dele1->_timeoutTerminator);
    ASSERT_EQ(multi_call::INVALID_VERSION_ID, dele1->_phase1Version);
    ASSERT_EQ(multi_call::INVALID_VERSION_ID, dele1->_phase2Version);
    delete dele1;
}

TEST_F(QrsSearcherProcessDelegationTest, testCheckCoveredRanges) {
    HA3_LOG(DEBUG, "Begin Test!");
    Result::ClusterPartitionRanges ranges;
    ranges["cluster1"].push_back(make_pair((uint32_t)0, (uint32_t)1));
    vector<string> clusters;
    clusters.push_back("cluster1");
    clusters.push_back("cluster2");
    QrsSearcherProcessDelegation delegation(_hitSummarySchemaCache, _searchService);
    delegation.checkCoveredRanges(clusters, ranges);
    ASSERT_EQ((size_t)2, ranges.size());
    ASSERT_EQ((size_t)0, ranges["cluster2"].size());
}

TEST_F(QrsSearcherProcessDelegationTest, testUpdateHitSummarySchemaAndFillCache) {
    QrsSearcherProcessDelegation delegation(_hitSummarySchemaCache, _searchService);

    common::RequestPtr requestPtr(new common::Request);
    ConfigClause* configClause = new ConfigClause();
    configClause->setStartOffset(0);
    configClause->setHitCount(10);
    configClause->setHitSummarySchemaCacheKey("wap");
    requestPtr->setConfigClause(configClause);
    // construct hitSummarySchema in cache.
    vector<HitSummarySchemaPtr> hitSummarySchemas;
    hitSummarySchemas.push_back(_hitSummarySchemaCache->getHitSummarySchema("simple1.default"));
    hitSummarySchemas.push_back(_hitSummarySchemaCache->getHitSummarySchema("simple2.default"));

    // construct hitSummarySchema returned.
    Hits hits;
    HitSummarySchema *hitSummarySchema1 = new HitSummarySchema();
    hitSummarySchema1->declareSummaryField("field1", ft_string, false);
    hitSummarySchema1->declareSummaryField("field2", ft_string, false);
    hits.addHitSummarySchema(hitSummarySchema1);

    HitSummarySchema *hitSummarySchema2 = new HitSummarySchema();
    hitSummarySchema2->declareSummaryField("field1", ft_string, false);
    hitSummarySchema2->declareSummaryField("field2", ft_string, false);
    hitSummarySchema2->declareSummaryField("field3", ft_string, false);
    hits.addHitSummarySchema(hitSummarySchema2);

    uint32_t hitCount = 10;

    for (uint32_t i = 0; i < hitCount; ++i) {
        HitPtr hitPtr(new Hit);
        SummaryHit *summaryHit = new SummaryHit(NULL, NULL);
        int64_t signature = i <= 4 || i % 2 == 0 ?
                            hitSummarySchema1->getSignature() :
                            hitSummarySchema2->getSignature();
        summaryHit->setSignature(signature);
        if (i <= 4) {
            hitPtr->setClusterName("simple1.default");
        } else {
            hitPtr->setClusterName("simple2.default");
        }
        hitPtr->setSummaryHit(summaryHit);
        hits.addHit(hitPtr);
    }

    delegation.updateHitSummarySchemaAndFillCache(&hits, hitSummarySchemas, requestPtr);

    // signature is the same, should not update cache.
    HitSummarySchemaPtr hitSummarySchemaPtr =
        _hitSummarySchemaCache->getHitSummarySchema("simple1.default&wap");
    ASSERT_TRUE(hitSummarySchemas[0].get() != hitSummarySchemaPtr.get());
    ASSERT_EQ(hitSummarySchemas[0]->getSignature(), hitSummarySchemaPtr->getSignature());

    hitSummarySchemaPtr = _hitSummarySchemaCache->getHitSummarySchema("simple2.default&wap");
    ASSERT_TRUE(hitSummarySchemas[1].get() != hitSummarySchemaPtr.get());
    ASSERT_TRUE(hitSummarySchema2 != hitSummarySchemaPtr.get());
    ASSERT_EQ(hitSummarySchema2->getSignature(), hitSummarySchemaPtr->getSignature());

    for (uint32_t i = 0; i < hitCount; ++i) {
        HitPtr hitPtr = hits.getHit(i);
        SummaryHit *summaryHit = hitPtr->getSummaryHit();
        ASSERT_TRUE(summaryHit);
        if (i <= 4) {
            ASSERT_EQ(string("simple1.default"), hitPtr->getClusterName());
            ASSERT_EQ(hitSummarySchema1, summaryHit->getHitSummarySchema());
        } else {
            ASSERT_EQ(string("simple2.default"), hitPtr->getClusterName());
            if (i % 2 == 0) {
                ASSERT_EQ(hitSummarySchema1, summaryHit->getHitSummarySchema());
            } else {
                ASSERT_EQ(hitSummarySchema2, summaryHit->getHitSummarySchema());
            }
        }
    }
}

TEST_F(QrsSearcherProcessDelegationTest, testSelectMatchDocsToFormat) {
    vector<docid_t> docIdVec;
    vector<hashid_t> hashIdVec;
    docIdVec.push_back(0);
    docIdVec.push_back(1);
    docIdVec.push_back(2);
    docIdVec.push_back(3);

    hashIdVec.push_back(0);
    hashIdVec.push_back(0);
    hashIdVec.push_back(1);
    hashIdVec.push_back(0);
    MatchDocs* matchDocs = prepareMatchDocs(docIdVec, hashIdVec);
    ResultPtr resultPtr(new Result(matchDocs));

    common::RequestPtr requestPtr(new common::Request);
    ConfigClause* configClause = new ConfigClause();
    configClause->setStartOffset(0);
    configClause->setHitCount(10);
    requestPtr->setConfigClause(configClause);
    service::HitSummarySchemaCachePtr cachePtr;
    service::QrsSearcherProcessDelegation delegation(cachePtr, _searchService);
    delegation.selectMatchDocsToFormat(requestPtr, resultPtr);
    vector<matchdoc::MatchDoc> resultMatchDocs = resultPtr->getMatchDocsForFormat();
    ASSERT_EQ((size_t)4, resultMatchDocs.size());
    ASSERT_EQ((docid_t)0, resultMatchDocs[0].getDocId());
    ASSERT_EQ((docid_t)1, resultMatchDocs[1].getDocId());
    ASSERT_EQ((docid_t)2, resultMatchDocs[2].getDocId());
    ASSERT_EQ((docid_t)3, resultMatchDocs[3].getDocId());

    ConfigClause* configClause2 = new ConfigClause();
    configClause2->setStartOffset(0);
    configClause2->setHitCount(2);
    requestPtr->setConfigClause(configClause2);
    delegation.selectMatchDocsToFormat(requestPtr, resultPtr);
    resultMatchDocs = resultPtr->getMatchDocsForFormat();
    ASSERT_EQ((size_t)2, resultMatchDocs.size());
    ASSERT_EQ((docid_t)0, resultMatchDocs[0].getDocId());
    ASSERT_EQ((docid_t)1, resultMatchDocs[1].getDocId());

    ConfigClause* configClause3 = new ConfigClause();
    configClause3->setStartOffset(1);
    configClause3->setHitCount(10);
    requestPtr->setConfigClause(configClause3);
    delegation.selectMatchDocsToFormat(requestPtr, resultPtr);
    resultMatchDocs = resultPtr->getMatchDocsForFormat();
    ASSERT_EQ((size_t)3, resultMatchDocs.size());
    ASSERT_EQ((docid_t)1, resultMatchDocs[0].getDocId());
    ASSERT_EQ((docid_t)2, resultMatchDocs[1].getDocId());
    ASSERT_EQ((docid_t)3, resultMatchDocs[2].getDocId());

    ConfigClause* configClause4 = new ConfigClause();
    configClause4->setStartOffset(5);
    configClause4->setHitCount(2);
    requestPtr->setConfigClause(configClause4);
    delegation.selectMatchDocsToFormat(requestPtr, resultPtr);
    resultMatchDocs = resultPtr->getMatchDocsForFormat();
    ASSERT_EQ((size_t)0, resultMatchDocs.size());
}

TEST_F(QrsSearcherProcessDelegationTest, testSelectMatchDocsToFormatWithEmptyMatchDocs) {
    {
        MatchDocs* matchDocs = new MatchDocs();
        ResultPtr resultPtr(new Result(matchDocs));

        common::RequestPtr requestPtr(new common::Request);
        ConfigClause* configClause = new ConfigClause();
        configClause->setStartOffset(0);
        configClause->setHitCount(10);
        requestPtr->setConfigClause(configClause);
        service::HitSummarySchemaCachePtr cachePtr;
        service::QrsSearcherProcessDelegation delegation(cachePtr, _searchService);
        delegation.selectMatchDocsToFormat(requestPtr, resultPtr);
        vector<matchdoc::MatchDoc> resultMatchDocs = resultPtr->getMatchDocsForFormat();
        ASSERT_EQ((size_t)0, resultMatchDocs.size());
    }

    {
        ResultPtr resultPtr(new Result((MatchDocs*)NULL));

        common::RequestPtr requestPtr(new common::Request);
        ConfigClause* configClause = new ConfigClause();
        configClause->setStartOffset(0);
        configClause->setHitCount(10);
        requestPtr->setConfigClause(configClause);
        service::HitSummarySchemaCachePtr cachePtr;
        service::QrsSearcherProcessDelegation delegation(cachePtr, _searchService);
        delegation.selectMatchDocsToFormat(requestPtr, resultPtr);
        vector<matchdoc::MatchDoc> resultMatchDocs = resultPtr->getMatchDocsForFormat();
        ASSERT_EQ((size_t)0, resultMatchDocs.size());
    }

}

TEST_F(QrsSearcherProcessDelegationTest, testAssociateClusterNames)
{
    {
        service::HitSummarySchemaCachePtr cachePtr;
        service::QrsSearcherProcessDelegation delegation(cachePtr, _searchService);
        ResultPtr resultPtr(new Result);
        std::vector<std::string> clusterNames;
        clusterNames.push_back("simple");
        EXPECT_CALL(*_searchService, getBizNames())
            .Times(1)
            .WillOnce(Return(clusterNames));
        delegation.createClusterIdMap();
        ASSERT_TRUE(delegation.associateClusterName(resultPtr));
        ASSERT_TRUE(resultPtr->_clusterNames.size() == 1);
        ASSERT_TRUE(resultPtr->_clusterNames[0] == "simple");
    }
}

TEST_F(QrsSearcherProcessDelegationTest, testNeedResearch) {
    service::HitSummarySchemaCachePtr cachePtr;
    service::QrsSearcherProcessDelegation delegation(_hitSummarySchemaCache, _searchService);

    // no optimizer clause
    ResultPtrVector results;
    ASSERT_TRUE(!delegation.needResearch(_requestPtr, results));

    // researchThreshold = 0
    OptimizerClause *optimizerClause = new OptimizerClause;
    _requestPtr->setOptimizerClause(optimizerClause);
    ASSERT_TRUE(!delegation.needResearch(_requestPtr, results));

    // does not trigger truncate
    ResultPtr result(new Result);
    results.push_back(result);
    ASSERT_TRUE(!delegation.needResearch(_requestPtr, results));

    // matchCount >= researchThreshold
    result->setUseTruncateOptimizer(true);
    _requestPtr->getConfigClause()->setResearchThreshold(200);
    MatchDocs *matchDocs = new MatchDocs;
    result->setMatchDocs(matchDocs);
    result->setActualMatchDocs(201);
    ASSERT_TRUE(!delegation.needResearch(_requestPtr, results));

    // need research
    _requestPtr->getConfigClause()->setResearchThreshold(10001);
    ASSERT_TRUE(delegation.needResearch(_requestPtr, results));
}

TEST_F(QrsSearcherProcessDelegationTest, testClusterNotExist) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("not_exist_cluster");

    ResultPtr resultPtr = dele.search(_requestPtr);
    ErrorResults errorResults;
    errorResults = resultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ(size_t(1), errorResults.size());
    ASSERT_EQ(ERROR_CLUSTER_NAME_NOT_EXIST, errorResults[0].getErrorCode());

    ASSERT_TRUE(resultPtr->getHits());
}

TEST_F(QrsSearcherProcessDelegationTest, testCreateSorterFailed) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    dele.setSessionMetricsCollector(_metricsCollectorPtr);
    dele.setClusterSorterManagers(_clusterSorterManagerPtr);

    ConfigClause *configClause = _requestPtr->getConfigClause();
    configClause->addClusterName("simple");

    FinalSortClause *sortClause = new FinalSortClause();
    sortClause->setSortName("not_exist_sorter");
    _requestPtr->setFinalSortClause(sortClause);

    ResultPtr resultPtr = dele.search(_requestPtr);
    ErrorResults errorResults;
    errorResults = resultPtr->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ(size_t(1), errorResults.size());
    ASSERT_EQ(ERROR_CREATE_SORTER, errorResults[0].getErrorCode());

    ASSERT_TRUE(resultPtr->getHits());
}

TEST_F(QrsSearcherProcessDelegationTest, testGetAndAdjustTimeOut) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    ConfigClause *configClause = _requestPtr->getConfigClause();
    dele._connectionTimeout = 6000;

    configClause->setRpcTimeOut(0);
    ASSERT_EQ(6000, dele.getAndAdjustTimeOut(_requestPtr));

    configClause->setRpcTimeOut(5000);
    ASSERT_EQ(5000, dele.getAndAdjustTimeOut(_requestPtr));

    {
        int64_t currentTime = autil::TimeUtility::currentTime();
        int64_t timeout = 10000000;
        TimeoutTerminator terminator(timeout, currentTime);
        dele.setTimeoutTerminator(&terminator);
        configClause->setRpcTimeOut(6000);
        ASSERT_EQ(6000, dele.getAndAdjustTimeOut(_requestPtr));

        configClause->setRpcTimeOut(16000);
        int64_t rpcTime = dele.getAndAdjustTimeOut(_requestPtr);
        ASSERT_TRUE(rpcTime <= timeout/1000);
    }
    {
        int64_t currentTime = autil::TimeUtility::currentTime();
        int64_t timeout = -1;
        TimeoutTerminator terminator(timeout, currentTime);
        dele.setTimeoutTerminator(&terminator);
        configClause->setRpcTimeOut(6000);
        ASSERT_EQ(6000, dele.getAndAdjustTimeOut(_requestPtr));

        configClause->setRpcTimeOut(16000);
        int64_t rpcTime = dele.getAndAdjustTimeOut(_requestPtr);
        ASSERT_EQ(16000, rpcTime);
    }
    {
        int64_t currentTime = autil::TimeUtility::currentTime();
        int64_t timeout = 0;
        TimeoutTerminator terminator(timeout, currentTime);
        dele.setTimeoutTerminator(&terminator);
        configClause->setRpcTimeOut(6000);
        ASSERT_EQ(6000, dele.getAndAdjustTimeOut(_requestPtr));

        configClause->setRpcTimeOut(16000);
        int64_t rpcTime = dele.getAndAdjustTimeOut(_requestPtr);
        ASSERT_EQ(16000, rpcTime);
    }
    {
        int64_t currentTime = autil::TimeUtility::currentTime();
        int64_t timeout = 0;
        TimeoutTerminator terminator(timeout, currentTime);
        dele.setTimeoutTerminator(&terminator);
        configClause->setRpcTimeOut(0);
        ASSERT_EQ(6000, dele.getAndAdjustTimeOut(_requestPtr));

        configClause->setRpcTimeOut(16000);
        int64_t rpcTime = dele.getAndAdjustTimeOut(_requestPtr);
        ASSERT_EQ(16000, rpcTime);
    }
}

TEST_F(QrsSearcherProcessDelegationTest, testLackResult) {
    QrsSearcherProcessDelegation dele(_hitSummarySchemaCache, _searchService);
    EXPECT_FALSE(dele._lackResult);
    dele._lackResult = true;
    dele.clearResource();
    EXPECT_FALSE(dele._lackResult);
}

END_HA3_NAMESPACE(service);
