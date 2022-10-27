#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/QrsSearcherHandler.h>
#include <ha3/service/test/FakeQrsChainProcessor.h>
#include <ha3/test/test.h>
#include <ha3/service/QrsChainManagerCreator.h>
#include <ha3/common/Result.h>
#include <ha3/queryparser/RequestParser.h>
#include <ha3/common/XMLResultFormatter.h>
#include <ha3/common/PBResultFormatter.h>
#include <ha3/common/test/ResultConstructor.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <ha3/qrs/test/ExceptionQrsProcessor.h>
#include <sap_eagle/RpcContextImpl.h>

using namespace std;
using namespace suez;
using namespace multi_call;
using namespace google::protobuf;
using namespace google::protobuf::io;

USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(worker);

BEGIN_HA3_NAMESPACE(service);

class QrsSearcherHandlerTest : public TESTBASE {
public:
    QrsSearcherHandlerTest();
    ~QrsSearcherHandlerTest();
public:
    void setUp();
    void tearDown();
protected:
    qrs::QrsChainManagerPtr createQrsChainManagerPtr();
    config::ConfigAdapterPtr getConfigAdapter();
    QrsSearcherHandlerPtr createHandler();
    QrsSessionSearchRequest createSessionRequest(
            const std::string &requestStr);
protected:
    QrsSearchConfigPtr _qrsSearchConfigPtr;
    monitor::SessionMetricsCollectorPtr _collectorPtr;
    FakeQrsChainProcessorPtr _fakeProcessorChainPtr;
    autil::mem_pool::Pool _pool;
    QrsBizInfoManagerPtr _bizInfoManager;
    QrsBizInfoPtr _qrsBizInfo;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, QrsSearcherHandlerTest);


QrsSearcherHandlerTest::QrsSearcherHandlerTest() {
}

QrsSearcherHandlerTest::~QrsSearcherHandlerTest() {
}

void QrsSearcherHandlerTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _pool.release();
    _collectorPtr.reset(new SessionMetricsCollector);
    _qrsSearchConfigPtr.reset(new QrsSearchConfig);
    _qrsSearchConfigPtr->_qrsChainMgrPtr = createQrsChainManagerPtr();
    QrsChainManagerPtr &qrsChainManagerPtr = _qrsSearchConfigPtr->_qrsChainMgrPtr;

    qrsChainManagerPtr->addProcessorChain("empty_chain",
            FakeQrsChainProcessorPtr(new FakeQrsChainProcessor));

    qrsChainManagerPtr->addProcessorChain("exception_chain",
            QrsProcessorPtr(new ExceptionQrsProcessor));

    _fakeProcessorChainPtr.reset(new FakeQrsChainProcessor);
    qrsChainManagerPtr->addProcessorChain("empty_request_chain",
            _fakeProcessorChainPtr);

    _bizInfoManager.reset(new QrsBizInfoManager());
    _qrsBizInfo.reset(new QrsBizInfo("default"));
    _qrsBizInfo->_qrsSearchConfig = _qrsSearchConfigPtr;
    _bizInfoManager->_bizInfoMap["default"] = _qrsBizInfo;
    sap::RpcContextFactoryImpl::init(NULL, NULL);
}

void QrsSearcherHandlerTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    sap::RpcContextFactoryImpl::release();
}

QrsSearcherHandlerPtr QrsSearcherHandlerTest::createHandler() {
    turing::QrsServiceSnapshotPtr snapshot(new QrsServiceSnapshot());
    QrsSearcherHandlerPtr handler =
        QrsSearcherHandlerPtr(new QrsSearcherHandler("undefine", snapshot));
    handler->_qrsSearchConfig = _qrsSearchConfigPtr;
    return handler;
}

QrsSessionSearchRequest QrsSearcherHandlerTest::createSessionRequest(
        const string &requestStr)
{
    return QrsSessionSearchRequest(requestStr, "8.8.8.8", &_pool, _collectorPtr);
}

TEST_F(QrsSearcherHandlerTest, testGetQrsProcessorChain) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();

    string chainName = DEFAULT_QRS_CHAIN;
    QrsProcessorPtr qrsProcessorPtr = searcherHandler->getQrsProcessorChain(
            chainName, NULL);
    ASSERT_TRUE(NULL != qrsProcessorPtr);
    ASSERT_TRUE(NULL == qrsProcessorPtr->getTracer());

    chainName = DEFAULT_DEBUG_QRS_CHAIN;
    qrsProcessorPtr = searcherHandler->getQrsProcessorChain(
            chainName, NULL);
    ASSERT_TRUE(NULL != qrsProcessorPtr);
    ASSERT_TRUE(NULL == qrsProcessorPtr->getTracer());


    chainName = "none_exist";
    qrsProcessorPtr = searcherHandler->getQrsProcessorChain(chainName, NULL);
    ASSERT_TRUE(NULL == qrsProcessorPtr);
}

TEST_F(QrsSearcherHandlerTest, testEmptyResultFromPlugin) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    QrsSessionSearchRequest request = createSessionRequest("config=cluster:daogou,"
            "qrs_chain:empty_chain,trace:all&&query=phrase:mp3");
    sap::RpcContextPtr rpcContext;
    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);
    ASSERT_TRUE(result.resultStr.find("unknow error") != string::npos);
    ASSERT_EQ(RF_XML, result.formatType);
}

TEST_F(QrsSearcherHandlerTest, testEmptyRequestFromPlugin) {
    _fakeProcessorChainPtr->setRequest(common::RequestPtr());

    QrsSearcherHandlerPtr searcherHandler = createHandler();
    QrsSessionSearchRequest request = createSessionRequest("config=cluster:daogou,"
            "qrs_chain:empty_request_chain,trace:all&&query=phrase:mp3");
    sap::RpcContextPtr rpcContext;
    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);
    ASSERT_TRUE(result.resultStr.find("unknow error") != string::npos);
    ASSERT_EQ(RF_XML, result.formatType);
}

TEST_F(QrsSearcherHandlerTest, testNewConfigClauseFromPlugin) {
    common::RequestPtr requestPtr(new common::Request);
    ConfigClause *configClause = new ConfigClause;
    configClause->setResultFormatSetting("xxxx");
    requestPtr->setConfigClause(configClause);

    _fakeProcessorChainPtr->setRequest(requestPtr);
    _fakeProcessorChainPtr->setResultPtr(ResultPtr(new Result));

    QrsSearcherHandlerPtr searcherHandler = createHandler();
    QrsSessionSearchRequest request = createSessionRequest("config=cluster:daogou,format:protobuf,"
            "qrs_chain:empty_request_chain,trace:all&&query=phrase:mp3");
    sap::RpcContextPtr rpcContext;
    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);
    HA3_LOG(INFO, "resultStr is [%s]", result.resultStr.c_str());
    ASSERT_TRUE(result.resultStr.find("Invalid result format type[xxxx]") != string::npos);
    ASSERT_EQ(RF_XML, result.formatType);
}

TEST_F(QrsSearcherHandlerTest, testExceptionQrsChain) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    QrsSessionSearchRequest request = createSessionRequest("config=cluster:daogou,"
            "qrs_chain:exception_chain,&&query=phrase:mp3");
    sap::RpcContextPtr rpcContext;
    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);
    ASSERT_TRUE(result.resultStr.find("exception happened in qrs processor.") != string::npos);
    ASSERT_EQ(RF_XML, result.formatType);
}

TEST_F(QrsSearcherHandlerTest, testGetResultString) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    string resultString;
    common::RequestPtr requestPtr(new common::Request);
    ConfigClause *configClause = new ConfigClause;
    requestPtr->setConfigClause(configClause);

    ResultPtr resultPtr;
    resultPtr.reset(new Result(new Hits()));
    ResultConstructor resultConstructor;
    resultConstructor.fillHit(resultPtr->getHits(),
                              1, 3, "title, body, description",
                              "cluster1, 11, title1, body1, des1"
                              " # cluster2, 22, title2, body2, des2");
    //protobuf
    ResultFormatType formatType = RF_PROTOBUF;
    resultString = searcherHandler->formatResultString(requestPtr, resultPtr, formatType);
    PBResultFormatter pbFormatter;
    stringstream ss;
    pbFormatter.format(resultPtr, ss);
    ASSERT_EQ(ss.str(), resultString);

    //fill summary one bytes
    resultString.clear();
    formatType = RF_PROTOBUF;
    uint32_t protoFormatOption = PBResultFormatter::SUMMARY_IN_BYTES;
    resultString = searcherHandler->formatResultString(requestPtr,
            resultPtr, formatType, protoFormatOption);
    stringstream ss2;
    pbFormatter.setOption(protoFormatOption);
    pbFormatter.format(resultPtr, ss2);
    ASSERT_EQ(ss2.str(), resultString);

    //xml
    resultString.clear();
    formatType = RF_XML;
    resultString = searcherHandler->formatResultString(requestPtr, resultPtr, formatType);
    XMLResultFormatter xmlFormatter;
    stringstream ss3;
    xmlFormatter.format(resultPtr, ss3);
    ASSERT_EQ(ss3.str(), resultString);
}

TEST_F(QrsSearcherHandlerTest, testProcess) {
    // check result empty
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    QrsSessionSearchRequest request = createSessionRequest(
            "config=cluster:daogou,qrs_chain:empty_chain&&query=phrase:mp3");
    sap::RpcContextPtr rpcContext;

    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);
    ASSERT_TRUE(result.resultStr.find("unknow error") != string::npos);
    ASSERT_EQ(RF_XML, result.formatType);
    EXPECT_EQ(multi_call::MULTI_CALL_ERROR_RPC_FAILED, result.multicallEc);
}

TEST_F(QrsSearcherHandlerTest, testProcessParseWrong) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    QrsSessionSearchRequest request = createSessionRequest("query=phrase:mp3");
    sap::RpcContextPtr rpcContext;
    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);
    ASSERT_TRUE(result.resultStr.find("ConfigClause: cluster name not exist") != string::npos);
    ASSERT_EQ(RF_XML, result.formatType);
    EXPECT_EQ(multi_call::MULTI_CALL_ERROR_NONE, result.multicallEc);
}

TEST_F(QrsSearcherHandlerTest, testProcessSearchRequestFail) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    QrsSessionSearchRequest request = createSessionRequest("config=cluster:daogou,"
            "qrs_chain:not_exist_chain&&query=phrase:mp3");
    sap::RpcContextPtr rpcContext;
    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);
    ASSERT_TRUE(result.resultStr.find("Not found chain") != string::npos);
    ASSERT_EQ(RF_XML, result.formatType);
    EXPECT_EQ(multi_call::MULTI_CALL_ERROR_RPC_FAILED, result.multicallEc);
}

TEST_F(QrsSearcherHandlerTest, testProcessWithTracer) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    ResultPtr resultPtr(new Result(new Hits()));
    _fakeProcessorChainPtr->setResultPtr(resultPtr);
    QrsSessionSearchRequest request = createSessionRequest("config=cluster:daogou,"
            "qrs_chain:empty_request_chain,trace:all&&query=phrase:mp3");
    sap::RpcContextPtr rpcContext = sap::RpcContext::create("abcd", "abcd", "");
    assert(rpcContext);
    std::string testTokenString = "test_token_string";
    rpcContext->putUserData(QrsSearcherHandler::RPC_TOCKEN_KEY, testTokenString);
    rpcContext->setAppId("xxx");

    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);

    ASSERT_EQ((uint32_t)result.resultStr.length(),
                         searcherHandler->_metricsCollectorPtr->getResultLength());
    ASSERT_EQ((uint32_t)resultPtr->getHits()->size(),
                         searcherHandler->_metricsCollectorPtr->getReturnCount());
    ASSERT_EQ((int32_t)resultPtr->getHits()->totalHits(),
                         searcherHandler->_metricsCollectorPtr->getEstimatedMatchCount());
    ASSERT_TRUE(result.resultStr.find("Begin qrs") != string::npos);
    ASSERT_EQ(RF_XML, result.formatType);

    ASSERT_TRUE(result.resultStr.find("eagle info") != string::npos);
    ASSERT_TRUE(result.resultStr.find(testTokenString) != string::npos);
    ASSERT_TRUE(result.resultStr.find("eagle group") != string::npos);
    EXPECT_EQ(multi_call::MULTI_CALL_ERROR_NONE, result.multicallEc);
}

TEST_F(QrsSearcherHandlerTest, testProcessWithTracerAndEmptyResult) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    _fakeProcessorChainPtr->setResultPtr(ResultPtr(new Result(new Hits())));
    QrsSessionSearchRequest request = createSessionRequest("config=cluster:daogou,"
            "qrs_chain:empty_chain,trace:all&&query=phrase:mp3");
    sap::RpcContextPtr rpcContext;
    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);
    ASSERT_TRUE(result.resultStr.find("unknow error") != string::npos);
}

TEST_F(QrsSearcherHandlerTest, testBadRequst) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    _fakeProcessorChainPtr->setResultPtr(ResultPtr(new Result(new Hits())));
    QrsSessionSearchRequest request = createSessionRequest("config=xxx");
    sap::RpcContextPtr rpcContext;
    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);
    ASSERT_TRUE(result.resultStr.find("key/value pair parse error.") != string::npos);
    ASSERT_EQ(RF_XML, result.formatType);
}

TEST_F(QrsSearcherHandlerTest, testCollectStatisticsWithMatchDoc) {
    QrsSearcherHandlerPtr qrsSearcherHandler = createHandler();
    qrsSearcherHandler->setSessionMetricsCollector(_collectorPtr);

    MatchDocs *matchDocs = new MatchDocs();
    matchdoc::MatchDoc matchDoc;
    matchDocs->addMatchDoc(matchDoc);
    uint32_t totalHit = 1;
    matchDocs->setTotalMatchDocs(totalHit);
    ResultPtr resultPtr(new Result(matchDocs));
    auto &formatedDocs = resultPtr->getMatchDocsForFormat();
    formatedDocs.push_back(matchDoc);
    qrsSearcherHandler->collectStatistics(resultPtr, "", true);
    ASSERT_EQ(totalHit, _collectorPtr->getReturnCount());
    ASSERT_EQ((int32_t)totalHit, _collectorPtr->getEstimatedMatchCount());
    ASSERT_EQ(false, _collectorPtr->isIncreaseEmptyQps());
}

TEST_F(QrsSearcherHandlerTest, testCollectStatisticsWithHits)
{
    QrsSearcherHandlerPtr qrsSearcherHandler = createHandler();
    qrsSearcherHandler->setSessionMetricsCollector(_collectorPtr);
    HitPtr hitPtr1(new Hit(0, 0, "daogou"));
    HitPtr hitPtr2(new Hit(1, 1, "daogou"));
    Hits *hits = new Hits();
    hits->addHit(hitPtr1);
    hits->addHit(hitPtr2);
    uint32_t totalHit = 2;
    hits->setTotalHits(totalHit);
    ResultPtr resultPtr(new Result(hits));
    qrsSearcherHandler->collectStatistics(resultPtr, "", true);
    ASSERT_EQ(totalHit, _collectorPtr->getReturnCount());
    ASSERT_EQ((int32_t)totalHit, _collectorPtr->getEstimatedMatchCount());
    ASSERT_EQ(false, _collectorPtr->isIncreaseEmptyQps());
}

TEST_F(QrsSearcherHandlerTest, testCollectStatisticsWithMatchDocAndHits)
{
    QrsSearcherHandlerPtr qrsSearcherHandler = createHandler();
    qrsSearcherHandler->setSessionMetricsCollector(_collectorPtr);

    HitPtr hitPtr1(new Hit(0, 0, "daogou"));
    HitPtr hitPtr2(new Hit(1, 1, "daogou"));
    Hits *hits = new Hits();
    hits->addHit(hitPtr1);
    hits->addHit(hitPtr2);
    uint32_t totalHit = 2;
    hits->setTotalHits(totalHit);
    ResultPtr resultPtr(new Result(hits));
    MatchDocs *matchDocs = new MatchDocs();
    matchdoc::MatchDoc matchDoc;
    matchDocs->addMatchDoc(matchDoc);
    totalHit = 1;
    matchDocs->setTotalMatchDocs(totalHit);
    resultPtr->setMatchDocs(matchDocs);
    auto &formatedDocs = resultPtr->getMatchDocsForFormat();
    formatedDocs.push_back(matchDoc);
    qrsSearcherHandler->collectStatistics(resultPtr, "", true);
    totalHit = 2;
    ASSERT_EQ(totalHit, _collectorPtr->getReturnCount());
    ASSERT_EQ((int32_t)totalHit, _collectorPtr->getEstimatedMatchCount());
    ASSERT_EQ(false, _collectorPtr->isIncreaseEmptyQps());
}

TEST_F(QrsSearcherHandlerTest, testCollectStatisticsNoHitsNoMatchDoc) {
    QrsSearcherHandlerPtr qrsSearcherHandler = createHandler();
    qrsSearcherHandler->setSessionMetricsCollector(_collectorPtr);

    ResultPtr resultPtr(new Result());
    qrsSearcherHandler->collectStatistics(resultPtr, "", true);
    uint32_t totalHit = 0;
    ASSERT_EQ(totalHit, _collectorPtr->getReturnCount());
    ASSERT_EQ((int32_t)totalHit, _collectorPtr->getEstimatedMatchCount());
    ASSERT_EQ(true, _collectorPtr->isIncreaseEmptyQps());
}

// after refine BizInfo lookup is mov to session.beginSession() and would not be null
// TEST_F(QrsSearcherHandlerTest, testNotGetBizInfo) {
//     QrsSearcherHandlerPtr handler = createHandler();
//     _bizInfoManager->_bizInfoMap.clear();
//     QrsSessionSearchRequest request = createSessionRequest("xxx");
//     sap::RpcContextPtr rpcContext;
//     QrsSessionSearchResult result = handler->process(request, rpcContext);
//     ASSERT_TRUE(result.resultStr.find("BizInfo Not Found") != string::npos);
//     ASSERT_EQ(RF_XML, result.formatType);
// }


TEST_F(QrsSearcherHandlerTest, testEagleEye) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    QrsSessionSearchRequest request = createSessionRequest("config=cluster:daogou,"
            "qrs_chain:empty_chain,trace:all&&query=phrase:mp3");
    sap::RpcContextPtr rpcContext = sap::RpcContext::create("abcd", "abcde", "");
    std::string testTokenString = "test_token_string";
    rpcContext->putUserData(QrsSearcherHandler::RPC_TOCKEN_KEY, testTokenString);
    QrsSessionSearchResult result = searcherHandler->process(request, rpcContext);
    ASSERT_TRUE(result.resultStr.find("unknow error") != string::npos);
    ASSERT_EQ(RF_XML, result.formatType);
    ASSERT_EQ(searcherHandler->_accessLog.getUserData(), QrsSearcherHandler::RPC_TOCKEN_KEY + ":" + testTokenString);
    ASSERT_EQ(searcherHandler->_accessLog.getTracerId(), "abcd");

}

TEST_F(QrsSearcherHandlerTest, testUpdateTimeoutTerminator) {
    QrsSearcherHandlerPtr searcherHandler = createHandler();
    TimeoutTerminator timeoutTerminator(10000, 100000);
    searcherHandler->setTimeoutTerminator(&timeoutTerminator);
    searcherHandler->updateTimeoutTerminator(0);
    ASSERT_EQ(110000, timeoutTerminator.getExpireTime());

    searcherHandler->updateTimeoutTerminator(11000);
    ASSERT_EQ(110000, timeoutTerminator.getExpireTime());

    searcherHandler->updateTimeoutTerminator(9000);
    ASSERT_EQ(109000, timeoutTerminator.getExpireTime());
    searcherHandler->setTimeoutTerminator(NULL);
}

config::ConfigAdapterPtr QrsSearcherHandlerTest::getConfigAdapter()
{
    HA3_LOG(DEBUG, "create ServerAdminAdapterPtr!!");

    string configRoot = TEST_DATA_CONF_PATH"/configurations/configuration_1/";
    config::ConfigAdapterPtr configAdapterPtr(new config::ConfigAdapter(configRoot));
    assert(configAdapterPtr.get());
    return configAdapterPtr;
}

QrsChainManagerPtr QrsSearcherHandlerTest::createQrsChainManagerPtr()
{
    QrsChainManagerCreatorPtr qrsChainManagerCreatorPtr(
            new QrsChainManagerCreator);

    ConfigAdapterPtr serverAdminAdapterPtr = getConfigAdapter();
    IndexProvider indexProvider;
    QrsZoneResourcePtr resource(new QrsZoneResource(indexProvider));
    resource->_configAdapter = serverAdminAdapterPtr;
    QrsChainManagerPtr qrsChainMgrPtr = qrsChainManagerCreatorPtr->createQrsChainMgr(resource);
    assert(qrsChainMgrPtr);
    return qrsChainMgrPtr;
}

END_HA3_NAMESPACE(service);
