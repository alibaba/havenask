#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/qrs/QrsProcessor.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/qrs/test/FakeRawStringProcessor.h>
#include <ha3/service/QrsSearcherProcessDelegation.h>
#include <string>

using namespace std;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(service);
BEGIN_HA3_NAMESPACE(qrs);

class QrsProcessorTest : public TESTBASE {
public:
    QrsProcessorTest();
    ~QrsProcessorTest();
public:
    void setUp();
    void tearDown();
protected:
    common::RequestPtr prepareEmptyRequest();
    common::ResultPtr prepareEmptyResult();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QrsProcessorTest);


QrsProcessorTest::QrsProcessorTest() { 
}

QrsProcessorTest::~QrsProcessorTest() { 
}

void QrsProcessorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void QrsProcessorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QrsProcessorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareEmptyRequest();
    ResultPtr resultPtr = prepareEmptyResult();
    FakeRawStringProcessor qrsProcessor("simple_test");
    qrsProcessor.process(requestPtr, resultPtr);
    ASSERT_TRUE(resultPtr.get());
}

TEST_F(QrsProcessorTest, testChainProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareEmptyRequest();
    ResultPtr resultPtr = prepareEmptyResult();

    QrsProcessorPtr qrsProcessorPtr1(new FakeRawStringProcessor("config=rewrite_1"));
    QrsProcessorPtr qrsProcessorPtr2(new FakeRawStringProcessor("config=rewrite_2"));
    QrsProcessorPtr qrsProcessorPtr3(new FakeRawStringProcessor("config=rewrite_3"));
    qrsProcessorPtr1->setNextProcessor(qrsProcessorPtr2);
    qrsProcessorPtr2->setNextProcessor(qrsProcessorPtr3);

    qrsProcessorPtr1->process(requestPtr, resultPtr);

    ASSERT_TRUE(resultPtr.get());    
    ASSERT_EQ(string("config=rewrite_3"), requestPtr->getOriginalString());
}

TEST_F(QrsProcessorTest, testInit) {
    HA3_LOG(DEBUG, "Begin Test!");

    KeyValueMap keyValues;
    keyValues["key1"] = "value1";
    keyValues["key2"] = "value2";

    QrsProcessorPtr qrsProcessorPtr(new FakeRawStringProcessor("init_test"));
    qrsProcessorPtr->init(keyValues, NULL);

    ASSERT_EQ(string("value1"), qrsProcessorPtr->getParam("key1"));
    ASSERT_EQ(string("value2"), qrsProcessorPtr->getParam("key2"));
}

RequestPtr QrsProcessorTest::prepareEmptyRequest() {
    Request *request = new Request();
    RequestPtr requestPtr(request);
    return requestPtr;
}

ResultPtr QrsProcessorTest::prepareEmptyResult() {
    Result *result = new Result();
    ResultPtr resultPtr(result);
    return resultPtr;
}

END_HA3_NAMESPACE(qrs);

