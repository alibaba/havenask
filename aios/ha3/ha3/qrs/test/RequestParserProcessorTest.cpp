#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Result.h>
#include <ha3/common/Request.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <ha3/qrs/RequestParserProcessor.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/queryparser/test/AnalyzerFactoryInit.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/qrs/test/ExceptionQrsProcessor.h>

using namespace std;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(queryparser);
using namespace build_service::analyzer;
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(qrs);

class RequestParserProcessorTest : public TESTBASE {
public:
    RequestParserProcessorTest();
    ~RequestParserProcessorTest();
public:
    void setUp();
    void tearDown();
protected:
    common::RequestPtr prepareEmptyRequest();
    common::ResultPtr prepareEmptyResult();
protected:
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, RequestParserProcessorTest);


RequestParserProcessorTest::RequestParserProcessorTest() { 
}

RequestParserProcessorTest::~RequestParserProcessorTest() { 

}

void RequestParserProcessorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");

    ClusterConfigInfo clusterConfigInfo;
    clusterConfigInfo._tableName = "table1";
    _clusterConfigMapPtr.reset(new ClusterConfigMap);
    _clusterConfigMapPtr->insert(make_pair("web.default", clusterConfigInfo));
}

void RequestParserProcessorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(RequestParserProcessorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    ResultPtr resultPtr = prepareEmptyResult();
    RequestPtr requestPtr = prepareEmptyRequest();
    requestPtr->setOriginalString("config=cluster:web,start:0,hit:10&&query=default:iphone");

    ASSERT_TRUE(requestPtr->getQueryClause());
    ASSERT_TRUE(requestPtr->getConfigClause());
    
    RequestParserProcessor processor(_clusterConfigMapPtr);
    monitor::SessionMetricsCollectorPtr collectorPtr(new monitor::SessionMetricsCollector(NULL));
    processor.setSessionMetricsCollector(collectorPtr);

    processor.process(requestPtr, resultPtr);
    
    ConfigClause* configClause = requestPtr->getConfigClause();
    QueryClause* queryClause = requestPtr->getQueryClause();

    ASSERT_TRUE(configClause);
    ASSERT_EQ(string("web.default"), configClause->getClusterName());
    ASSERT_EQ((uint32_t)0, configClause->getStartOffset());
    ASSERT_EQ((uint32_t)10, configClause->getHitCount());
    
    ASSERT_TRUE(queryClause);
    const Query *query = queryClause->getRootQuery();
    ASSERT_TRUE(query);
    ASSERT_EQ(string("TermQuery"), query->getQueryName());
}

TEST_F(RequestParserProcessorTest, testWithExceptionProcessor) {
    RequestParserProcessor processor(_clusterConfigMapPtr);
    ExceptionQrsProcessorPtr exceptionQrsProcessor(new ExceptionQrsProcessor());
    monitor::SessionMetricsCollectorPtr collectorPtr(new monitor::SessionMetricsCollector(NULL));
    processor.setSessionMetricsCollector(collectorPtr);
    processor.setNextProcessor(exceptionQrsProcessor);
    RequestPtr requestPtr = prepareEmptyRequest();
    requestPtr->setOriginalString("config=cluster:web,start:0,hit:10&&query=default:iphone");
    ResultPtr resultPtr = prepareEmptyResult();
    bool catchException = false;
    try {
        processor.process(requestPtr, resultPtr);
    } catch (...) {
        catchException = true;
    }
    ASSERT_TRUE(catchException);
}

RequestPtr RequestParserProcessorTest::prepareEmptyRequest() {
    Request *request = new Request();
    RequestPtr requestPtr(request);
    return requestPtr;
}

ResultPtr RequestParserProcessorTest::prepareEmptyResult() {
    Result *result = new Result();
    ResultPtr resultPtr(result);
    return resultPtr;
}

END_HA3_NAMESPACE(qrs);

