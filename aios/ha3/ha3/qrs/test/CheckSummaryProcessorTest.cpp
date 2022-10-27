#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/CheckSummaryProcessor.h>
#include <ha3/qrs/test/FakeFillSummaryProcessor.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/qrs/test/ExceptionQrsProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);

class CheckSummaryProcessorTest : public TESTBASE {
public:
    CheckSummaryProcessorTest();
    ~CheckSummaryProcessorTest();
public:
    void setUp();
    void tearDown();
protected:
    common::RequestPtr prepareEmptyRequest(const std::string &noSummary);
    common::ResultPtr prepareEmptyResult();
protected:
    HA3_LOG_DECLARE();
};

USE_HA3_NAMESPACE(common);
HA3_LOG_SETUP(qrs, CheckSummaryProcessorTest);


CheckSummaryProcessorTest::CheckSummaryProcessorTest() {
}

CheckSummaryProcessorTest::~CheckSummaryProcessorTest() {
}

void CheckSummaryProcessorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void CheckSummaryProcessorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(CheckSummaryProcessorTest, testWithNoSummary) {
    HA3_LOG(DEBUG, "Begin testWithNoSummary!");

    FakeFillSummaryProcessorPtr fakeFillSummaryProcessor(new FakeFillSummaryProcessor());
    CheckSummaryProcessorPtr checkSummaryProcessor(new CheckSummaryProcessor());
    checkSummaryProcessor->setNextProcessor(fakeFillSummaryProcessor);

    RequestPtr requestPtr = prepareEmptyRequest("yes");
    ResultPtr resultPtr = prepareEmptyResult();
    checkSummaryProcessor->process(requestPtr, resultPtr);
    Hits *hits = resultPtr->getHits();
    ASSERT_TRUE(hits);
    ASSERT_TRUE(!hits->hasSummary());
    ASSERT_TRUE(hits->isNoSummary());
    ASSERT_TRUE(hits->isIndependentQuery());

    requestPtr = prepareEmptyRequest("no");
    resultPtr = prepareEmptyResult();
    checkSummaryProcessor->process(requestPtr, resultPtr);
    hits = resultPtr->getHits();
    ASSERT_TRUE(hits);
    ASSERT_TRUE(hits->hasSummary());
    ASSERT_TRUE(!hits->isNoSummary());
    ASSERT_TRUE(!hits->isIndependentQuery());

    requestPtr = prepareEmptyRequest("yes");
    resultPtr.reset(new Result);
    checkSummaryProcessor->process(requestPtr, resultPtr);
    ASSERT_TRUE(!resultPtr->getHits());
}

TEST_F(CheckSummaryProcessorTest, testWithExceptionProcessor) {
    ExceptionQrsProcessorPtr exceptionQrsProcessor(new ExceptionQrsProcessor());
    CheckSummaryProcessorPtr checkSummaryProcessor(new CheckSummaryProcessor());
    checkSummaryProcessor->setNextProcessor(exceptionQrsProcessor);

    RequestPtr requestPtr = prepareEmptyRequest("yes");
    ResultPtr resultPtr = prepareEmptyResult();
    bool catchException = false;
    try {
        checkSummaryProcessor->process(requestPtr, resultPtr);
    } catch (...) {
        catchException = true;
    }
    ASSERT_TRUE(catchException);
}

RequestPtr CheckSummaryProcessorTest::prepareEmptyRequest(const std::string &noSummary) {
    ConfigClause *configClause = new ConfigClause;
    configClause->setNoSummary(noSummary);
    RequestPtr requestPtr(new Request);
    requestPtr->setConfigClause(configClause);
    return requestPtr;
}

ResultPtr CheckSummaryProcessorTest::prepareEmptyResult() {
    Result *result = new Result();
    Hits *hits = new Hits;
    result->setHits(hits);
    ResultPtr resultPtr(result);
    return resultPtr;
}

END_HA3_NAMESPACE(qrs);

