#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/ARPCSession.h>
#include <autil/mem_pool/Pool.h>
#include <google/protobuf/service.h>
#include <ha3/service/test/FakeClosure.h>
#include <autil/StringTokenizer.h>
#include <ha3/service/SearcherSession.h>
#include <ha3/service/SessionPool.h>
#include <ha3/service/QrsSearcherHandler.h>
#include <ha3/worker/SearchSuezWorker.h>
#include <ha3/worker/BizInfoManager.h>
#include <ha3/common/ConfigClause.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/Request.h>
#include <ha3/proto/ProtoClassUtil.h>
#include <sap/util/environ_util.h>

using namespace std;
using namespace autil;
using namespace sap;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(worker);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(service);

class ARPCSessionTest : public TESTBASE {
public:
    ARPCSessionTest();
    ~ARPCSessionTest();
public:
    void setUp();
    void tearDown();
protected:
    void assertResult(const std::string &expectResult,
                      const std::string &result);
    void checkRequestWithCompressType(common::RequestPtr requestPtr,
            HaCompressType type);
protected:
    autil::mem_pool::Pool _pool;
protected:
    WorkerResourcePtr _workerResource;
    suez::turing::CavaConfig _cavaConfigInfo;
    BizInfoPtr _bizInfo;
    SearchBizInfoManagerPtr _bizInfoManager;
    SearchBizInfo *_searchBizInfo;
    multi_call::GigAgentPtr _gigAgent;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, ARPCSessionTest);


ARPCSessionTest::ARPCSessionTest() {
}

ARPCSessionTest::~ARPCSessionTest() {
}

void ARPCSessionTest::setUp() {
    EnvironUtil::setEnv(WorkerParam::IP, "1.1.1.1");
    EnvironUtil::setEnv(WorkerParam::PORT, "80");
    EnvironUtil::setEnv(WorkerParam::USER_NAME, "sance");
    EnvironUtil::setEnv(WorkerParam::SERVICE_NAME, "mainse");
    EnvironUtil::setEnv(WorkerParam::ROLE_TYPE, "searcher");
    EnvironUtil::setEnv(WorkerParam::THREAD_NUMBER, "1");
    EnvironUtil::setEnv(WorkerParam::HEALTH_CHECK_PATH, "/status_test");
    EnvironUtil::setEnv(WorkerParam::AMONITOR_PATH, "amon");
    _workerResource.reset(new WorkerResource());
    ASSERT_TRUE(_workerResource->init());

    _searchBizInfo = new SearchBizInfo("default");
    PerThreadObjInitParam param("", _cavaConfigInfo, 10, 20, 8);
    _searchBizInfo->_threadObjCache = _searchBizInfo->createThreadObjCache(_workerResource, param);
    _bizInfo.reset(_searchBizInfo);
    _bizInfoManager.reset(new SearchBizInfoManager());
    _bizInfoManager->_bizInfoMap["default"] = _bizInfo;
    _gigAgent.reset(new multi_call::GigAgent());
    HA3_LOG(DEBUG, "setUp!");
}

void ARPCSessionTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ARPCSessionTest, testDrop) {
    HA3_LOG(DEBUG, "Begin Test!");

    SessionPoolImpl<SearcherSession> sessionPool(3);

    ASSERT_EQ((uint32_t)0, sessionPool.getCurrentCount());
    SearcherSession* session1 = sessionPool.get();
    ASSERT_EQ((uint32_t)1, sessionPool.getCurrentCount());

    FakeClosure done;
    SearchRequest *searchrequest = new SearchRequest;
    SearchResponse *searchresponse = new SearchResponse;
    searchresponse->set_multicall_ec(multi_call::MULTI_CALL_ERROR_RPC_FAILED);
    ASSERT_EQ(multi_call::MULTI_CALL_ERROR_RPC_FAILED,
              multi_call::MultiCallErrorCode(searchresponse->multicall_ec()));
    SearchSuezWorker worker;
    session1->setRequest(searchrequest, searchresponse, &done,
                         worker.getWorkerResource(), _bizInfoManager, _gigAgent);
    session1->dropRequest();
    ASSERT_TRUE(1 == done.getCount());
    ASSERT_EQ((uint32_t)0, sessionPool.getCurrentCount());
    ASSERT_EQ(multi_call::MULTI_CALL_ERROR_RPC_FAILED,
              multi_call::MultiCallErrorCode(searchresponse->multicall_ec()));
    delete searchrequest;
    delete searchresponse;
}

TEST_F(ARPCSessionTest, testDeserializeIncompatible) {
    SearcherSession session;
    SearchRequest searchRequest;
    auto request = new common::Request(&_pool);
    ConfigClause *configClause = new ConfigClause;
    EXPECT_EQ(0, configClause->getTotalRankSize());
    EXPECT_EQ(0, configClause->getTotalRerankSize());
    EXPECT_EQ("", configClause->getFetchSummaryGroup());
    request->setConfigClause(configClause);
    session._request = request;
    SearchResponse searchresponse;
    FakeClosure done;
    SearchSuezWorker worker;
    session.setRequest(&searchRequest, &searchresponse, &done,
                       worker.getWorkerResource(), _bizInfoManager, _gigAgent);
    session.deserializeIncompatible();
    EXPECT_EQ(0, configClause->getTotalRankSize());
    EXPECT_EQ(0, configClause->getTotalRerankSize());

    auto clauses = searchRequest.mutable_clauses();
    auto clauseRewrite = clauses->mutable_incompatible();
    clauseRewrite->set_totalranksize(100);
    clauseRewrite->set_totalreranksize(200);
    clauseRewrite->set_fetchsummarygroup("common|tpp");
    session.deserializeIncompatible();
    EXPECT_EQ(100, configClause->getTotalRankSize());
    EXPECT_EQ(200, configClause->getTotalRerankSize());
    EXPECT_EQ("common|tpp", configClause->getFetchSummaryGroup());
}

TEST_F(ARPCSessionTest, testEndSession) {
    HA3_LOG(DEBUG, "Begin Test!");

    common::TracerPtr tracer(new common::Tracer());
    tracer->setAddress("address1");
    tracer->setPartitionId("partitionId1");
    tracer->trace("traceInfo1");
    tracer->setLevel(0);
    SearcherSession *session1 = new SearcherSession();
    session1->setMemPool(&_pool);

    session1->_tracer = tracer;
    SearchRequest *searchrequest = new SearchRequest;
    SearchResponse *searchresponse = new SearchResponse;
    FakeClosure done;
    SearchSuezWorker worker;
    session1->setRequest(searchrequest, searchresponse, &done,
                         worker.getWorkerResource(), _bizInfoManager, _gigAgent);
    sap::RpcContextPtr rpcContext = sap::RpcContext::create("abcd", "abcd", "");
    std::string testTokenString = "test_token_string";
    rpcContext->putUserData(QrsSearcherHandler::RPC_TOCKEN_KEY, testTokenString);
    rpcContext->setAppId("xxx");
    session1->_rpcContext = rpcContext;
    session1->endSession();

    const string &result = searchresponse->assemblyresult();
    const string &expectResult = "[TRACE2] [address1],"
                                 "[partitionId1]End searcher";
    assertResult(expectResult, result);
    delete searchrequest;
    delete searchresponse;
}

void ARPCSessionTest::checkRequestWithCompressType(common::RequestPtr requestPtr,
        HaCompressType type)
{
    SessionPoolImpl<SearcherSession> sessionPool(3);
    auto session = sessionPool.get();
    SearchResponse *searchresponse = new SearchResponse;
    SearchRequest *searchrequest = new SearchRequest;
    FakeClosure done;
    SearchSuezWorker worker;
    string orignalStr;
    string requestStr;
    requestPtr->serializeToString(orignalStr);
    HaCompressType compressType = type;
    HaRequestGenerator::constructRequestString(orignalStr,
            requestStr, compressType);
    searchrequest->mutable_clauses()->set_assemblyclause(requestStr);
    searchrequest->set_compresstype(
            ProtoClassUtil::convertCompressType(compressType));
    session->setRequest(searchrequest, searchresponse, &done,
                        worker.getWorkerResource(), _bizInfoManager, _gigAgent);

    auto it = _searchBizInfo->_threadObjCache->_objs.begin();
    _searchBizInfo->_threadObjCache->_objs[pthread_self()] = it->second;
    session->beginSession();
    string expectedStr;
    session->_request->serializeToString(expectedStr);
    ASSERT_EQ(expectedStr, orignalStr);
    delete searchrequest;
    delete searchresponse;
}

TEST_F(ARPCSessionTest, testRequestCompression) {
    HA3_LOG(DEBUG, "Begin Test!");
    common::RequestPtr requestPtr;
    requestPtr.reset(new common::Request(&_pool));

    QueryClause *queryClause = new QueryClause();
    RequiredFields requiredFields;
    queryClause->setRootQuery(new TermQuery("aaa", "indexName", requiredFields));
    requestPtr->setQueryClause(queryClause);

    ConfigClause *configClause = new ConfigClause;
    configClause->setStartOffset(0);
    configClause->setHitCount(5);
    requestPtr->setConfigClause(configClause);
    checkRequestWithCompressType(requestPtr, Z_SPEED_COMPRESS);
    checkRequestWithCompressType(requestPtr, Z_DEFAULT_COMPRESS);
    checkRequestWithCompressType(requestPtr, SNAPPY);
    checkRequestWithCompressType(requestPtr, LZ4);
    checkRequestWithCompressType(requestPtr, NO_COMPRESS);
}

void ARPCSessionTest::assertResult(const std::string &expectResult,
                                   const std::string &result)
{
    StringTokenizer st(expectResult, ",",
                       StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    assert(result.find(st[0]) != std::string::npos);
    for (size_t i = 1; i < st.getNumTokens(); ++i) {
        assert(result.find(st[i]) != std::string::npos);
        assert(result.find(st[i-1]) < result.find(st[i]));
    }
}

TEST_F(ARPCSessionTest, testFillResponse) {
    SearcherSession session1;
    SearchRequest searchrequest;
    SearchResponse searchresponse;
    FakeClosure done;
    SearchSuezWorker worker;
    session1.setRequest(&searchrequest, &searchresponse, &done,
                        worker.getWorkerResource(), _bizInfoManager, _gigAgent);
    session1.fillResponse("test Content");
    ASSERT_EQ(std::string("test Content"), searchresponse.assemblyresult());
}

TEST_F(ARPCSessionTest, testPhaseStartTrigger) {
    SearcherSession session1;
    common::TracerPtr tracer(new common::Tracer());
    tracer->setAddress("1.2.3.4");
    tracer->setPartitionId("partitionId");
    tracer->trace("traceInfo");
    tracer->setLevel(0);
    session1._tracer = tracer;
    session1.phaseStartTrigger();

    const string &result = session1._tracer->getTraceInfo();
    const string &expectResult = "traceInfo,"
                                 "[TRACE2] [1.2.3.4],"
                                 "[partitionId]Begin searcher";
    assertResult(expectResult, result);
}

TEST_F(ARPCSessionTest, testFillMultiCallInfo) {
    {
        SessionPoolImpl<SearcherSession> sessionPool(3);
        auto session = sessionPool.get();
        EXPECT_FALSE(session->_isDegrade);
        EXPECT_FALSE(session->isDegrade());
        session->setDegrade(true);
        EXPECT_TRUE(session->_isDegrade);
        EXPECT_TRUE(session->isDegrade());
        session->reset();
        EXPECT_FALSE(session->isDegrade());
    }
    {
        // no error and no degrade
        SessionPoolImpl<SearcherSession> sessionPool(3);
        auto session = sessionPool.get();
        session->_resultPtr.reset(new Result());
        auto response = new proto::SearchResponse();
        response->set_multicall_ec(multi_call::MULTI_CALL_ERROR_NONE);
        EXPECT_FALSE(session->isDegrade());
        EXPECT_FALSE(response->has_weightinfo());
        EXPECT_FALSE(response->has_gigresponseinfo());
        session->_searchResponse = response;
        session->fillMultiCallInfo();
        EXPECT_TRUE(response->has_weightinfo());
        EXPECT_TRUE(response->has_multicall_ec());
        EXPECT_TRUE(response->has_gigresponseinfo());
        EXPECT_EQ(multi_call::MAX_WEIGHT, response->weightinfo());
        EXPECT_EQ(multi_call::MULTI_CALL_ERROR_NONE,
                  multi_call::MultiCallErrorCode(response->multicall_ec()));
        delete response;
    }
    {
        // error and no degrade
        SessionPoolImpl<SearcherSession> sessionPool(3);
        auto session = sessionPool.get();
        session->_resultPtr.reset(new Result());
        auto response = new proto::SearchResponse();
        response->set_multicall_ec(multi_call::MULTI_CALL_ERROR_RPC_FAILED);
        EXPECT_FALSE(session->isDegrade());
        EXPECT_FALSE(response->has_weightinfo());
        EXPECT_FALSE(response->has_gigresponseinfo());
        session->_searchResponse = response;
        session->fillMultiCallInfo();
        EXPECT_TRUE(response->has_weightinfo());
        EXPECT_TRUE(response->has_multicall_ec());
        EXPECT_TRUE(response->has_gigresponseinfo());
        EXPECT_EQ(multi_call::MAX_WEIGHT, response->weightinfo());
        EXPECT_EQ(multi_call::MULTI_CALL_ERROR_RPC_FAILED,
                  multi_call::MultiCallErrorCode(response->multicall_ec()));
        delete response;
    }
    {
        // no error but degrade
        SessionPoolImpl<SearcherSession> sessionPool(3);
        auto session = sessionPool.get();
        session->_resultPtr.reset(new Result());
        auto response = new proto::SearchResponse();
        response->set_multicall_ec(multi_call::MULTI_CALL_ERROR_NONE);
        session->_isDegrade = true;
        EXPECT_TRUE(session->isDegrade());
        EXPECT_FALSE(response->has_weightinfo());
        EXPECT_FALSE(response->has_gigresponseinfo());
        session->_searchResponse = response;
        session->fillMultiCallInfo();
        EXPECT_TRUE(response->has_weightinfo());
        EXPECT_TRUE(response->has_multicall_ec());
        EXPECT_TRUE(response->has_gigresponseinfo());
        EXPECT_EQ(multi_call::MAX_WEIGHT, response->weightinfo());
        EXPECT_EQ(multi_call::MULTI_CALL_ERROR_DEC_WEIGHT,
                  multi_call::MultiCallErrorCode(response->multicall_ec()));
        delete response;
    }
    {
        // error and degrade
        arpc::RPCServerClosure closure(0, NULL, NULL, NULL, NULL, 0);
        closure.SetTracer(new arpc::Tracer());
        SessionPoolImpl<SearcherSession> sessionPool(3);
        auto session = sessionPool.get();
        session->_resultPtr.reset(new Result());
        session->_done = &closure;
        session->_gigQueryInfo = _gigAgent->getQueryInfo("");
        auto response = new proto::SearchResponse();
        response->set_multicall_ec(multi_call::MULTI_CALL_ERROR_RPC_FAILED);
        session->_isDegrade = true;
        EXPECT_TRUE(session->isDegrade());
        EXPECT_FALSE(response->has_weightinfo());
        EXPECT_FALSE(response->has_gigresponseinfo());
        session->_searchResponse = response;
        session->fillMultiCallInfo();
        EXPECT_TRUE(response->has_weightinfo());
        EXPECT_TRUE(response->has_multicall_ec());
        EXPECT_TRUE(response->has_gigresponseinfo());
        EXPECT_EQ(multi_call::MAX_WEIGHT, response->weightinfo());
        EXPECT_EQ(multi_call::MULTI_CALL_ERROR_RPC_FAILED,
                  multi_call::MultiCallErrorCode(response->multicall_ec()));
        EXPECT_TRUE(response->gigresponseinfo().size() > 0);
        delete response;
    }
}

END_HA3_NAMESPACE(service);
