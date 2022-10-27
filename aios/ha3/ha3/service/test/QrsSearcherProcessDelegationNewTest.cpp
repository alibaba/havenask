#include <unittest/unittest.h>
#include <sap_eagle/RpcContextImpl.h>
#include <ha3/test/test.h>
#include <ha3/service/QrsSearcherProcessDelegation.h>
#include <ha3/worker/test/MockSearchService.h>
#include <ha3/service/RpcContextUtil.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/qrs/QrsBiz.h>

using namespace std;
using namespace testing;
using namespace multi_call;

USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(worker);
USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(turing);

class QrsSearcherProcessDelegationNewTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    common::RequestPtr _requestPtr;
    HitSummarySchemaCachePtr _hitSummarySchemaCache;
    autil::mem_pool::Pool *_pool;
    MockSearchServicePtr _searchService;
    std::vector<multi_call::ResponsePtr> _responseVec;
    multi_call::QuerySessionPtr _querySession;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, QrsSearcherProcessDelegationNewTest);

void QrsSearcherProcessDelegationNewTest::setUp() {
     _pool = new autil::mem_pool::Pool;
     _requestPtr.reset(new common::Request(_pool));
     QueryClause *queryClause = new QueryClause();
     _requestPtr->setQueryClause(queryClause);

     ConfigClause *configClause = new ConfigClause;
     _requestPtr->setConfigClause(configClause);

     _searchService.reset(new MockSearchService());
     sap::RpcContextFactoryImpl::init(NULL, NULL);
     _querySession.reset(new multi_call::QuerySession(_searchService));
}

void QrsSearcherProcessDelegationNewTest::tearDown() {
    delete _pool;
    _pool = NULL;
    sap::RpcContextFactoryImpl::release();
}

TEST_F(QrsSearcherProcessDelegationNewTest, testDoAsyncProcessCallPhase1) {
    HA3_LOG(DEBUG, "begin test!");
    {
        QrsSearcherProcessDelegation dele(_hitSummarySchemaCache);
        dele._querySession = _querySession.get();
        dele._rpcContext = RpcContext::fromMap(map<string, string>());
        assert(dele._rpcContext);
        dele._rpcContext->toTLS();
        common::MultiErrorResultPtr errors(new common::MultiErrorResult());
        std::vector<std::string> clusterNames;
        clusterNames.push_back("cluster1");
        dele.doAsyncProcessCallPhase1(clusterNames, _requestPtr, errors);
        EXPECT_EQ(1, dele._generatorVec.size());
        EXPECT_EQ("cluster1", dele._generatorVec[0]->getFlowControlStrategy());
    }
    {
        QrsSearcherProcessDelegation dele(_hitSummarySchemaCache);
        dele._querySession = _querySession.get();
        dele._rpcContext = RpcContext::fromMap(map<string, string>());
        assert(dele._rpcContext);
        dele._rpcContext->putUserData(HA_RESERVED_BENCHMARK_KEY,
                                      HA_RESERVED_BENCHMARK_VALUE_1);
        dele._rpcContext->toTLS();
        common::MultiErrorResultPtr errors(new common::MultiErrorResult());
        std::vector<std::string> clusterNames;
        clusterNames.push_back("cluster1");
        dele.doAsyncProcessCallPhase1(clusterNames, _requestPtr, errors);
        EXPECT_EQ(1, dele._generatorVec.size());
        EXPECT_EQ(QrsBiz::getBenchmarkBizName("cluster1"),
                  dele._generatorVec[0]->getFlowControlStrategy());
    }
    {
        QrsSearcherProcessDelegation dele(_hitSummarySchemaCache);
        dele._querySession = _querySession.get();
        dele._rpcContext = RpcContext::fromMap(map<string, string>());
        assert(dele._rpcContext);
        dele._rpcContext->putUserData(HA_RESERVED_BENCHMARK_KEY,
                                      HA_RESERVED_BENCHMARK_VALUE_2);
        dele._rpcContext->toTLS();
        common::MultiErrorResultPtr errors(new common::MultiErrorResult());
        std::vector<std::string> clusterNames;
        clusterNames.push_back("cluster1");
        dele.doAsyncProcessCallPhase1(clusterNames, _requestPtr, errors);
        EXPECT_EQ(1, dele._generatorVec.size());
        EXPECT_EQ(QrsBiz::getBenchmarkBizName("cluster1"),
                  dele._generatorVec[0]->getFlowControlStrategy());
    }
}

TEST_F(QrsSearcherProcessDelegationNewTest, testDoAsyncProcessCallPhase2) {
    HA3_LOG(DEBUG, "begin test!");
    {
        QrsSearcherProcessDelegation dele(_hitSummarySchemaCache);
        dele._querySession = _querySession.get();
        dele._rpcContext = RpcContext::fromMap(map<string, string>());
        assert(dele._rpcContext);
        dele._rpcContext->toTLS();
        common::MultiErrorResultPtr errors(new common::MultiErrorResult());
        DocIdClauseMap docIdClauseMap;
        docIdClauseMap["cluster1"] = new DocIdClause();
        dele.doAsyncProcessCallPhase2(docIdClauseMap, _requestPtr, errors);
        EXPECT_EQ(1, dele._generatorVec.size());
        EXPECT_EQ("cluster1", dele._generatorVec[0]->getFlowControlStrategy());
    }
    {
        QrsSearcherProcessDelegation dele(_hitSummarySchemaCache);
        dele._querySession = _querySession.get();
        dele._rpcContext = RpcContext::fromMap(map<string, string>());
        assert(dele._rpcContext);
        dele._rpcContext->putUserData(HA_RESERVED_BENCHMARK_KEY,
                                      HA_RESERVED_BENCHMARK_VALUE_1);
        dele._rpcContext->toTLS();
        common::MultiErrorResultPtr errors(new common::MultiErrorResult());
        DocIdClauseMap docIdClauseMap;
        docIdClauseMap["cluster1"] = new DocIdClause();
        dele.doAsyncProcessCallPhase2(docIdClauseMap, _requestPtr, errors);
        EXPECT_EQ(1, dele._generatorVec.size());
        EXPECT_EQ(QrsBiz::getBenchmarkBizName("cluster1"),
                  dele._generatorVec[0]->getFlowControlStrategy());
    }
    {
        QrsSearcherProcessDelegation dele(_hitSummarySchemaCache);
        dele._querySession = _querySession.get();
        dele._rpcContext = RpcContext::fromMap(map<string, string>());
        assert(dele._rpcContext);
        dele._rpcContext->putUserData(HA_RESERVED_BENCHMARK_KEY,
                                      HA_RESERVED_BENCHMARK_VALUE_2);
        dele._rpcContext->toTLS();
        common::MultiErrorResultPtr errors(new common::MultiErrorResult());
        DocIdClauseMap docIdClauseMap;
        docIdClauseMap["cluster1"] = new DocIdClause();
        dele.doAsyncProcessCallPhase2(docIdClauseMap, _requestPtr, errors);
        EXPECT_EQ(1, dele._generatorVec.size());
        EXPECT_EQ(QrsBiz::getBenchmarkBizName("cluster1"),
                  dele._generatorVec[0]->getFlowControlStrategy());
    }
}

TEST_F(QrsSearcherProcessDelegationNewTest, testCreateClusterIdMap) {
    {
        MockSearchService2 *mockSearchService(new MockSearchService2);
        mockSearchService->_bizNames.push_back("cluster1");
        mockSearchService->_bizNames.push_back("cluster2");
        mockSearchService->_bizNames.push_back("cluster3");
        SearchServicePtr searchService(mockSearchService);
        QrsSearcherProcessDelegation dele(_hitSummarySchemaCache);
        _querySession.reset(new multi_call::QuerySession(searchService));
        dele._querySession = _querySession.get();
        dele.createClusterIdMap();
        auto &clusterIdMap = dele._clusterIdMap;
        ASSERT_EQ(3u, clusterIdMap.size());
        auto it = clusterIdMap.find("cluster1");
        ASSERT_TRUE(it != clusterIdMap.end());
        ASSERT_EQ(0, it->second);

        it = clusterIdMap.find("cluster2");
        ASSERT_TRUE(it != clusterIdMap.end());
        ASSERT_EQ(1, it->second);

        it = clusterIdMap.find("cluster3");
        ASSERT_TRUE(it != clusterIdMap.end());
        ASSERT_EQ(2, it->second);
    }
    {
        MockSearchService2 *mockSearchService(new MockSearchService2);
        mockSearchService->_bizNames.push_back("cluster3");
        mockSearchService->_bizNames.push_back("cluster2");
        mockSearchService->_bizNames.push_back("cluster1");
        SearchServicePtr searchService(mockSearchService);
        QrsSearcherProcessDelegation dele(_hitSummarySchemaCache);
        _querySession.reset(new multi_call::QuerySession(searchService));
        dele._querySession = _querySession.get();
        dele.createClusterIdMap();
        auto &clusterIdMap = dele._clusterIdMap;
        ASSERT_EQ(3u, clusterIdMap.size());
        auto it = clusterIdMap.find("cluster1");
        ASSERT_TRUE(it != clusterIdMap.end());
        ASSERT_EQ(0, it->second);

        it = clusterIdMap.find("cluster2");
        ASSERT_TRUE(it != clusterIdMap.end());
        ASSERT_EQ(1, it->second);

        it = clusterIdMap.find("cluster3");
        ASSERT_TRUE(it != clusterIdMap.end());
        ASSERT_EQ(2, it->second);
    }
    {
        MockSearchService2 *mockSearchService(new MockSearchService2);
        SearchServicePtr searchService(mockSearchService);
        QrsSearcherProcessDelegation dele(_hitSummarySchemaCache);
        _querySession.reset(new multi_call::QuerySession(searchService));
        dele._querySession = _querySession.get();
        dele.createClusterIdMap();
        auto &clusterIdMap = dele._clusterIdMap;
        ASSERT_EQ(0u, clusterIdMap.size());
    }
}
END_HA3_NAMESPACE();
