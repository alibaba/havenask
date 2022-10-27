#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/PageDistinctProcessor.h>
#include <ha3/qrs/test/FakeQrsProcessor.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/qrs/test/PageDistinctSelectorTest.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <ha3/qrs/test/ExceptionQrsProcessor.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);

class PageDistinctProcessorTest : public TESTBASE {
public:
    PageDistinctProcessorTest();
    ~PageDistinctProcessorTest();
public:
    void setUp();
    void tearDown();
protected:
    common::ResultPtr createResult();
    common::RequestPtr createRequest();
    template <typename T>
    void checkPageDistinctSelectorCreate(const PageDistParam &distParam, 
            common::HitPtr hitPtr);
    common::ConfigClause* createConfigClause();
    void checkError(common::ResultPtr resultPtr, ErrorCode errorCode, 
                    const std::string &errorMsg);
    
protected:
    PageDistinctProcessorPtr _processor;
    FakeQrsProcessorPtr _fakeProcessor;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, PageDistinctProcessorTest);


PageDistinctProcessorTest::PageDistinctProcessorTest() { 
}

PageDistinctProcessorTest::~PageDistinctProcessorTest() { 
}

void PageDistinctProcessorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _processor.reset(new PageDistinctProcessor);
    _fakeProcessor.reset(new FakeQrsProcessor);
    _processor->setNextProcessor(_fakeProcessor);
}

void PageDistinctProcessorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(PageDistinctProcessorTest, testWithExceptionProcessor) {
    PageDistinctProcessor processor;
    ExceptionQrsProcessorPtr exceptionQrsProcessor(new ExceptionQrsProcessor());
    processor.setNextProcessor(exceptionQrsProcessor);
    RequestPtr requestPtr = createRequest();
    ResultPtr resultPtr = createResult();
    bool catchException = false;
    try {
        processor.process(requestPtr, resultPtr);
    } catch (...) {
        catchException = true;
    }
    ASSERT_TRUE(catchException);
}

TEST_F(PageDistinctProcessorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = createRequest();
    ResultPtr resultPtr = createResult();
    
    string oldRequestStr;
    requestPtr->serializeToString(oldRequestStr);

    _fakeProcessor->setResult(resultPtr);
    _processor->process(requestPtr, resultPtr);

    RequestPtr retRequest = _fakeProcessor->getRequest();
    ASSERT_TRUE(retRequest);
    ConfigClause *retConfigClause = retRequest->getConfigClause();
    ASSERT_TRUE(retConfigClause);
    ASSERT_EQ((uint32_t)0, 
                         retConfigClause->getStartOffset());
    ASSERT_EQ((uint32_t)10, 
                         retConfigClause->getHitCount());
    DistinctClause *distClause = retRequest->getDistinctClause();
    ASSERT_TRUE(distClause);
    ASSERT_EQ((uint32_t)2, 
                         distClause->getDistinctDescriptionsCount());
    ASSERT_TRUE(!distClause->getDistinctDescription(0));
    DistinctDescription *distDesc = 
        distClause->getDistinctDescription(1);
    ASSERT_TRUE(distDesc);
    ASSERT_EQ((int32_t)2, distDesc->getDistinctCount());
    ASSERT_EQ((int32_t)1, distDesc->getDistinctTimes());
    SyntaxExpr *expectExpr = 
        new AtomicSyntaxExpr("distKey", vt_unknown, ATTRIBUTE_NAME);
    unique_ptr<SyntaxExpr> exprPtr(expectExpr);
    SyntaxExpr *keyExpr = distDesc->getRootSyntaxExpr();
    ASSERT_TRUE(*keyExpr == expectExpr);
    
    AttributeClause *attrClause = retRequest->getAttributeClause();
    ASSERT_TRUE(attrClause);
    const set<string> &attrNames = attrClause->getAttributeNames();
    ASSERT_EQ((size_t)1, attrNames.size());
    ASSERT_TRUE(attrNames.find("distKey") != attrNames.end());
    
    string newRequestStr;
    requestPtr->serializeToString(newRequestStr);
    ASSERT_EQ(oldRequestStr.size(), newRequestStr.size());
    ASSERT_TRUE(!memcmp(oldRequestStr.c_str(), 
                           newRequestStr.c_str(), 
                           oldRequestStr.size()));

    ASSERT_TRUE(resultPtr);
    ASSERT_TRUE(!resultPtr->hasError());
    
    Hits *retHits = resultPtr->getHits();
    ASSERT_TRUE(retHits);
    const vector<HitPtr>& resultHits = retHits->getHitVec();
    PageDistinctSelectorTest::checkHits(resultHits, "1, 3, 2, 4, 7");
}

TEST_F(PageDistinctProcessorTest, testProcessFailed) { 
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr;
    ResultPtr resultPtr;
    _processor->process(requestPtr, resultPtr);
    ASSERT_TRUE(NULL == resultPtr.get());
    
    requestPtr.reset(new Request);
    _processor->process(requestPtr, resultPtr);
    ASSERT_TRUE(resultPtr);
    ASSERT_TRUE(resultPtr->hasError());
    checkError(resultPtr, ERROR_GENERAL, 
               "ConfigClause is null");
    
    ConfigClause *configClause = createConfigClause();
    requestPtr->setConfigClause(configClause);
    requestPtr->setDistinctClause(new DistinctClause);
    _processor->process(requestPtr, resultPtr);
    ASSERT_TRUE(resultPtr);
    ASSERT_TRUE(resultPtr->hasError());
    checkError(resultPtr, ERROR_GENERAL, 
               "page distinct confict with the distinct clause "
               "in the original request");
    
    resultPtr->resetErrorResult();
    requestPtr->setDistinctClause(NULL);
    _processor->process(requestPtr, resultPtr);
    ASSERT_TRUE(!resultPtr);
    ASSERT_TRUE(!requestPtr->getAttributeClause());

    resultPtr.reset(new Result);
    AttributeClause *attrClause = new AttributeClause;
    attrClause->addAttribute("attr1");
    requestPtr->setAttributeClause(attrClause);
    _processor->process(requestPtr, resultPtr);
    ASSERT_TRUE(!resultPtr);
    ASSERT_TRUE(requestPtr->getAttributeClause());
    set<string> attrs = 
        requestPtr->getAttributeClause()->getAttributeNames();
    ASSERT_EQ((size_t)1, attrs.size());
    ASSERT_TRUE(attrs.find("attr1") != attrs.end());
}

TEST_F(PageDistinctProcessorTest, testCreatePageDistinctSelector) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    HitPtr hitPtr(new Hit(0));
    bool ret = hitPtr->addAttribute("dist_key_int8", (int8_t)1);
    ret = ret && hitPtr->addAttribute("dist_key_int16", (int16_t)1);
    ret = ret && hitPtr->addAttribute("dist_key_int32", (int32_t)1);
    ret = ret && hitPtr->addAttribute("dist_key_int64", (int64_t)1);
    ret = ret && hitPtr->addAttribute("dist_key_uint8", (uint8_t)1);
    ret = ret && hitPtr->addAttribute("dist_key_uint16", (uint16_t)1);
    ret = ret && hitPtr->addAttribute("dist_key_uint32", (uint32_t)1);
    ret = ret && hitPtr->addAttribute("dist_key_uint64", (uint64_t)1);
    ret = ret && hitPtr->addAttribute("dist_key_float", (float)1);
    ASSERT_TRUE(ret);

    PageDistParam distParam;
    distParam.pgHitNum = 10;
    distParam.distCount = 2;
    distParam.gradeThresholds.push_back(3.0);
    distParam.sortFlags.push_back(true);

    distParam.distKey = "not_exist_key";
    PageDistinctSelectorPtr selector = 
        _processor->createPageDistinctSelector(distParam, hitPtr);
    ASSERT_TRUE(NULL == selector.get());
    
    distParam.distKey = "dist_key_int8";
    checkPageDistinctSelectorCreate<int8_t>(distParam, hitPtr);
    distParam.distKey = "dist_key_int16";
    checkPageDistinctSelectorCreate<int16_t>(distParam, hitPtr);
    distParam.distKey = "dist_key_int32";
    checkPageDistinctSelectorCreate<int32_t>(distParam, hitPtr);
    distParam.distKey = "dist_key_int64";
    checkPageDistinctSelectorCreate<int64_t>(distParam, hitPtr);
    distParam.distKey = "dist_key_uint8";
    checkPageDistinctSelectorCreate<uint8_t>(distParam, hitPtr);
    distParam.distKey = "dist_key_uint16";
    checkPageDistinctSelectorCreate<uint16_t>(distParam, hitPtr);
    distParam.distKey = "dist_key_uint32";
    checkPageDistinctSelectorCreate<uint32_t>(distParam, hitPtr);
    distParam.distKey = "dist_key_uint64";
    checkPageDistinctSelectorCreate<uint64_t>(distParam, hitPtr);

    distParam.distKey = "dist_key_float";
    selector = _processor->createPageDistinctSelector(distParam, hitPtr);
    ASSERT_TRUE(NULL == selector.get());
}

TEST_F(PageDistinctProcessorTest, testGetSortFlags) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    RequestPtr requestPtr(new Request);
    vector<bool> sortFlags;
    
    _processor->getSortFlags(requestPtr, sortFlags);
    ASSERT_EQ((size_t)1, sortFlags.size());
    ASSERT_TRUE(!sortFlags[0]);

    SortClause *sortClause = new SortClause;
    requestPtr->setSortClause(sortClause);

    _processor->getSortFlags(requestPtr, sortFlags);
    ASSERT_EQ((size_t)1, sortFlags.size());
    ASSERT_TRUE(!sortFlags[0]);
    
    SortDescription *sortDesc = new SortDescription;
    sortClause->addSortDescription(sortDesc);
    sortDesc = new SortDescription();
    sortDesc->setSortAscendFlag(true);
    sortClause->addSortDescription(sortDesc);
    _processor->getSortFlags(requestPtr, sortFlags);
    ASSERT_EQ((size_t)2, sortFlags.size());
    ASSERT_TRUE(!sortFlags[0]);
    ASSERT_TRUE(sortFlags[1]);
}

TEST_F(PageDistinctProcessorTest, testGetKVPairValue) {
    HA3_LOG(DEBUG, "Begin Test!");
    ConfigClause configClause;
    ErrorResult errorResult;
    
    string key = "key";
    string value = "";
    bool ret = _processor->getKVPairValue(&configClause, key, value, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    string expectErrorMsg = string("not specify the [") + key + 
                            "] key in kvpair of ConfigClause";
    ASSERT_EQ(expectErrorMsg, errorResult.getErrorMsg());

    configClause.addKVPair(key, "");
    ret = _processor->getKVPairValue(&configClause, key, value, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    ASSERT_EQ(expectErrorMsg, errorResult.getErrorMsg());

    configClause.addKVPair(key, " ");
    ret = _processor->getKVPairValue(&configClause, key, value, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    ASSERT_EQ(expectErrorMsg, errorResult.getErrorMsg());

    configClause.addKVPair(key, "value");
    errorResult.resetError(ERROR_NONE, "");
    ret = _processor->getKVPairValue(&configClause, key, value, errorResult);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, errorResult.getErrorCode());
    ASSERT_EQ(string("value"), value);

    configClause.addKVPair(key, "  value ");
    ret = _processor->getKVPairValue(&configClause, key, value, errorResult);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, errorResult.getErrorCode());
    ASSERT_EQ(string("value"), value);
}

TEST_F(PageDistinctProcessorTest, testGetKVPairUint32Value) {
    HA3_LOG(DEBUG, "Begin Test!");
    ConfigClause configClause;
    uint32_t value;
    ErrorResult errorResult;
    
    string key = "key";
    bool ret = _processor->getKVPairUint32Value(&configClause, 
            key, value, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    ASSERT_EQ((string("not specify the [") + key +
                          "] key in kvpair of ConfigClause"), 
                         errorResult.getErrorMsg());

    configClause.addKVPair(key, "123aa");
    ret = _processor->getKVPairUint32Value(&configClause, 
            key, value, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    ASSERT_EQ((string("trans ") + key + 
                          " from string [123aa] to uint32_t failed"), 
                         errorResult.getErrorMsg());
    
    configClause.addKVPair(key, " 123 ");
    errorResult.resetError(ERROR_NONE, "");
    ret = _processor->getKVPairUint32Value(&configClause, 
            key, value, errorResult);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, errorResult.getErrorCode());
    ASSERT_EQ((uint32_t)123, value);
}

TEST_F(PageDistinctProcessorTest, testGetGradeThresholds) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    ConfigClause configClause;
    vector<double> grades;
    string originalGradeStr;

    ErrorResult errorResult;
    bool ret = _processor->getGradeThresholds(&configClause, 
            grades, originalGradeStr, errorResult);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, errorResult.getErrorCode());
    ASSERT_EQ((size_t)0, grades.size());
    ASSERT_EQ(string(""), originalGradeStr);

    string key = PageDistinctProcessor::PAGE_DIST_PARAM_GRADE_THRESHOLDS;
    configClause.addKVPair(key, "");
    ret = _processor->getGradeThresholds(&configClause, 
            grades, originalGradeStr, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    ASSERT_EQ(string("the grade threshold is empty"),
                         errorResult.getErrorMsg());

    configClause.addKVPair(key, " ");
    ret = _processor->getGradeThresholds(&configClause, 
            grades, originalGradeStr, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    ASSERT_EQ(string("the grade threshold is empty"),
                         errorResult.getErrorMsg());
    

    configClause.addKVPair(key, " 1|a");
    errorResult.resetError(ERROR_NONE, "");
    ret = _processor->getGradeThresholds(&configClause, 
            grades, originalGradeStr, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    ASSERT_EQ(string("trans grade value form string [a] to double failed"),
                         errorResult.getErrorMsg());

    configClause.addKVPair(key, "|");
    errorResult.resetError(ERROR_NONE, "");
    ret = _processor->getGradeThresholds(&configClause, 
            grades, originalGradeStr, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    ASSERT_EQ(string("the grade threshold is empty"),
                         errorResult.getErrorMsg());

    configClause.addKVPair(key, " 1 ");
    errorResult.resetError(ERROR_NONE, "");
    ret = _processor->getGradeThresholds(&configClause, 
            grades, originalGradeStr, errorResult);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, errorResult.getErrorCode());
    ASSERT_EQ((size_t)1, grades.size());
    ASSERT_DOUBLE_EQ((double)1, grades[0]);

    configClause.addKVPair(key, " 1| |2 | ");
    errorResult.resetError(ERROR_NONE, "");
    ret = _processor->getGradeThresholds(&configClause, 
            grades, originalGradeStr, errorResult);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, errorResult.getErrorCode());
    ASSERT_EQ((size_t)2, grades.size());
    ASSERT_DOUBLE_EQ((double)1, grades[0]);
    ASSERT_DOUBLE_EQ((double)2, grades[1]);
    ASSERT_EQ(string(" 1| |2 | "), originalGradeStr);
}

TEST_F(PageDistinctProcessorTest, testGetPageDistParams) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    RequestPtr requestPtr(new Request);
    PageDistParam distParam;
    ErrorResult errorResult;

    bool ret = _processor->getPageDistParams(requestPtr, distParam, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    ASSERT_EQ(string("ConfigClause is null"), 
                         errorResult.getErrorMsg());
    
    ConfigClause *configClause = new ConfigClause;
    requestPtr->setConfigClause(configClause);
    configClause->setStartOffset(1);
    configClause->setHitCount(11);
    ret = _processor->getPageDistParams(requestPtr, distParam, 
            errorResult);
    ASSERT_TRUE(!ret);
    
    configClause->addKVPair(
            PageDistinctProcessor::PAGE_DIST_PARAM_PAGE_SIZE, 
            "10");
    ret = _processor->getPageDistParams(requestPtr, distParam, 
            errorResult);
    ASSERT_TRUE(!ret);

    configClause->addKVPair(
            PageDistinctProcessor::PAGE_DIST_PARAM_DIST_COUNT, 
            "2");
    ret = _processor->getPageDistParams(requestPtr, distParam, 
            errorResult);
    ASSERT_TRUE(!ret);
    
    configClause->addKVPair(
            PageDistinctProcessor::PAGE_DIST_PARAM_DIST_KEY, 
            "dist_key");
    ret = _processor->getPageDistParams(requestPtr, distParam, 
            errorResult);
    ASSERT_TRUE(ret);

    ASSERT_EQ((uint32_t)1, distParam.startOffset);
    ASSERT_EQ((uint32_t)11, distParam.hitCount);
    ASSERT_EQ((uint32_t)10, distParam.pgHitNum);
    ASSERT_EQ((uint32_t)2, distParam.distCount);
    ASSERT_EQ(string("dist_key"), distParam.distKey);
    ASSERT_EQ((size_t)1, distParam.sortFlags.size());
    ASSERT_TRUE(!distParam.sortFlags[0]);
    ASSERT_EQ((size_t)0, distParam.gradeThresholds.size());
    
    configClause->addKVPair(
            PageDistinctProcessor::PAGE_DIST_PARAM_GRADE_THRESHOLDS, 
            "a");
    ret = _processor->getPageDistParams(requestPtr, distParam, 
            errorResult);
    ASSERT_TRUE(!ret);

    configClause->addKVPair(
            PageDistinctProcessor::PAGE_DIST_PARAM_GRADE_THRESHOLDS, 
            "1");
    ret = _processor->getPageDistParams(requestPtr, distParam, 
            errorResult);
    ASSERT_TRUE(ret);
    ASSERT_EQ((uint32_t)1, distParam.startOffset);
    ASSERT_EQ((uint32_t)11, distParam.hitCount);
    ASSERT_EQ((uint32_t)10, distParam.pgHitNum);
    ASSERT_EQ((uint32_t)2, distParam.distCount);
    ASSERT_EQ(string("dist_key"), distParam.distKey);
    ASSERT_EQ((size_t)1, distParam.sortFlags.size());
    ASSERT_TRUE(!distParam.sortFlags[0]);
    ASSERT_EQ((size_t)1, distParam.gradeThresholds.size());
    ASSERT_DOUBLE_EQ((double)1, 
                     distParam.gradeThresholds[0]);
    
    SortClause *sortClause = new SortClause;
    requestPtr->setSortClause(sortClause);
    SortDescription *sortDesc = new SortDescription;
    sortClause->addSortDescription(sortDesc);
    sortDesc = new SortDescription;
    sortDesc->setSortAscendFlag(true);
    sortClause->addSortDescription(sortDesc);
    ret = _processor->getPageDistParams(requestPtr, distParam, 
            errorResult);
    ASSERT_TRUE(ret);
    ASSERT_EQ((uint32_t)1, distParam.startOffset);
    ASSERT_EQ((uint32_t)11, distParam.hitCount);
    ASSERT_EQ((uint32_t)10, distParam.pgHitNum);
    ASSERT_EQ((uint32_t)2, distParam.distCount);
    ASSERT_EQ(string("dist_key"), distParam.distKey);
    ASSERT_TRUE(!distParam.sortFlags[0]);
    ASSERT_EQ((size_t)1, distParam.gradeThresholds.size());
    ASSERT_DOUBLE_EQ((double)1, 
                     distParam.gradeThresholds[0]);
    ASSERT_EQ((size_t)2, distParam.sortFlags.size());
    ASSERT_TRUE(!distParam.sortFlags[0]);
    ASSERT_TRUE(distParam.sortFlags[1]);
}

TEST_F(PageDistinctProcessorTest, testCalcWholePageCount) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PageDistParam distParam;
    distParam.pgHitNum = 10;
    distParam.startOffset = 0;
    distParam.hitCount = 9;
    
    uint32_t pageCount = _processor->calcWholePageCount(distParam);
    ASSERT_EQ((uint32_t)1, pageCount);

    distParam.hitCount = 10;
    pageCount = _processor->calcWholePageCount(distParam);
    ASSERT_EQ((uint32_t)1, pageCount);

    distParam.hitCount = 11;
    pageCount = _processor->calcWholePageCount(distParam);
    ASSERT_EQ((uint32_t)2, pageCount);
    
    distParam.hitCount = 21;
    pageCount = _processor->calcWholePageCount(distParam);
    ASSERT_EQ((uint32_t)3, pageCount);
}

TEST_F(PageDistinctProcessorTest, testGetDistClauseString) {
    HA3_LOG(DEBUG, "Begin Test!");

    PageDistParam distParam;
    distParam.pgHitNum = 10;
    distParam.startOffset = 0;
    distParam.hitCount = 11;
    distParam.distKey = "key";
    distParam.distCount = 2;
    
    string distStr = _processor->getDistClauseString(distParam);
    ASSERT_EQ(string("none_dist;dist_key:key,dist_count:4"), 
                         distStr);
    
    distParam.gradeThresholds.push_back(0.0000000000000123);
    distParam.gradeThresholds.push_back(-12300000000000000.0);
    distParam.originalGradeStr = "0.0000000000000123|-12300000000000000.0";
    distStr = _processor->getDistClauseString(distParam);
    ASSERT_EQ(string("none_dist;dist_key:key,dist_count:4,grade:"
                                "0.0000000000000123|-12300000000000000.0"),
                         distStr);
}

TEST_F(PageDistinctProcessorTest, testSetDistinctClause) {
    HA3_LOG(DEBUG, "Begin Test!");

    PageDistParam distParam;
    distParam.pgHitNum = 10;
    distParam.startOffset = 0;
    distParam.hitCount = 11;
    distParam.distKey = "key";
    distParam.distCount = 2;
    distParam.gradeThresholds.push_back(1.1);
    distParam.gradeThresholds.push_back(2.1);
    distParam.originalGradeStr="1.1|2.1";

    RequestPtr requestPtr(new Request);
    ErrorResult errorResult;
    bool ret = _processor->setDistinctClause(requestPtr, 
            distParam, errorResult);
    ASSERT_TRUE(ret);
    DistinctClause *distClause = requestPtr->getDistinctClause();
    ASSERT_TRUE(distClause);
    ASSERT_EQ((uint32_t)2, 
                         distClause->getDistinctDescriptionsCount());
    DistinctDescription *distDesc = distClause->getDistinctDescription(0);
    ASSERT_TRUE(!distDesc);
    distDesc = distClause->getDistinctDescription(1);
    ASSERT_TRUE(distDesc);
    ASSERT_EQ((int32_t)4, distDesc->getDistinctCount());
    ASSERT_EQ((int32_t)1, distDesc->getDistinctTimes());
    const vector<double> &grades = distDesc->getGradeThresholds();
    ASSERT_EQ((size_t)2, grades.size());
    ASSERT_DOUBLE_EQ((double)1.1, grades[0]);
    ASSERT_DOUBLE_EQ((double)2.1, grades[1]);
    SyntaxExpr *expectExpr = 
        new AtomicSyntaxExpr("key", vt_unknown, ATTRIBUTE_NAME);
    unique_ptr<SyntaxExpr> exprPtr(expectExpr);
    SyntaxExpr *keyExpr = distDesc->getRootSyntaxExpr();
    ASSERT_TRUE(keyExpr);
    ASSERT_TRUE(*keyExpr == expectExpr);

    errorResult.resetError(ERROR_NONE, "");
    ret = _processor->setDistinctClause(requestPtr, 
            distParam, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_GENERAL, errorResult.getErrorCode());
    ASSERT_EQ(string("page distinct confict with the distinct"
                                " clause in the original request"), 
                         errorResult.getErrorMsg());

    distParam.distKey = "key;";
    errorResult.resetError(ERROR_NONE, "");
    requestPtr->setDistinctClause(NULL);
    ret = _processor->setDistinctClause(requestPtr, 
            distParam, errorResult);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_DISTINCT_CLAUSE, 
                         errorResult.getErrorCode());
}

TEST_F(PageDistinctProcessorTest, testSetAttributeClause) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    PageDistParam distParam;
    distParam.distKey = "key";
    RequestPtr requestPtr(new Request);

    _processor->setAttributeClause(requestPtr, distParam);
    AttributeClause *attrClause = requestPtr->getAttributeClause();
    ASSERT_TRUE(attrClause);
    set<string> attrNames = attrClause->getAttributeNames();
    ASSERT_EQ((size_t)1, attrNames.size());
    ASSERT_TRUE(attrNames.find("key") != attrNames.end());

    _processor->setAttributeClause(requestPtr, distParam);
    attrClause = requestPtr->getAttributeClause();
    ASSERT_TRUE(attrClause);
    attrNames = attrClause->getAttributeNames();
    ASSERT_EQ((size_t)1, attrNames.size());
    ASSERT_TRUE(attrNames.find("key") != attrNames.end());

    distParam.distKey = "key2";
    _processor->setAttributeClause(requestPtr, distParam);
    attrClause = requestPtr->getAttributeClause();
    ASSERT_TRUE(attrClause);
    attrNames = attrClause->getAttributeNames();
    ASSERT_EQ((size_t)2, attrNames.size());
    ASSERT_TRUE(attrNames.find("key") != attrNames.end());
    ASSERT_TRUE(attrNames.find("key2") != attrNames.end());
}

TEST_F(PageDistinctProcessorTest, testResetRequest) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr(new Request);
    ConfigClause *configClause = new ConfigClause;
    configClause->setStartOffset(5);
    configClause->setHitCount(6);
    requestPtr->setConfigClause(configClause);
    PageDistParam distParam;
    distParam.pgHitNum = 10;
    distParam.startOffset = 5;
    distParam.hitCount = 6;
    distParam.distKey = "key";
    distParam.distCount = 2;
    ErrorResult errorResult;

    bool ret = _processor->resetRequest(requestPtr, distParam, errorResult);
    ASSERT_TRUE(ret);
    
    configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_EQ((uint32_t)0, configClause->getStartOffset());
    ASSERT_EQ((uint32_t)40, configClause->getHitCount());

    DistinctClause *distClause = requestPtr->getDistinctClause();
    ASSERT_TRUE(distClause);
    ASSERT_EQ((uint32_t)2, 
                         distClause->getDistinctDescriptionsCount());
    DistinctDescription *distDesc = distClause->getDistinctDescription(0);
    ASSERT_TRUE(!distDesc);
    distDesc = distClause->getDistinctDescription(1);
    ASSERT_TRUE(distDesc);
    ASSERT_EQ((int32_t)4, distDesc->getDistinctCount());
    ASSERT_EQ((int32_t)1, distDesc->getDistinctTimes());
    SyntaxExpr *expectExpr = 
        new AtomicSyntaxExpr("key", vt_unknown, ATTRIBUTE_NAME);
    unique_ptr<SyntaxExpr> exprPtr(expectExpr);
    SyntaxExpr *keyExpr = distDesc->getRootSyntaxExpr();
    ASSERT_TRUE(keyExpr);
    ASSERT_TRUE(*keyExpr == expectExpr);

    AttributeClause *attrClause = requestPtr->getAttributeClause();
    ASSERT_TRUE(attrClause);
    set<string> attrNames = attrClause->getAttributeNames();
    ASSERT_EQ((size_t)1, attrNames.size());
    ASSERT_TRUE(attrNames.find("key") != attrNames.end());

    ret = _processor->resetRequest(requestPtr, distParam, errorResult);
    ASSERT_TRUE(!ret);
}

TEST_F(PageDistinctProcessorTest, testSelectPages) {
    HA3_LOG(DEBUG, "Begin Test!");

    PageDistParam distParam;
    distParam.pgHitNum = 5;
    distParam.startOffset = 5;
    distParam.hitCount = 6;
    distParam.distKey = "key";
    distParam.distCount = 2;

    vector<HitPtr> hitVec; 
    hitVec = PageDistinctSelectorTest::createHits(distParam.distKey, 
            "1,1,5.0;"
            "2,1,4.0;"
            "3,3,3.0;"
            "4,2,2.0;"
            "5,1,1.0;"
            "6,3,0.5;"
            "7,1,0.4;"
            "8,1,0.3;"
            "9,2,0.2;"
            "10,3,0.1;");
    Hits *hits = new Hits;
    hits->setTotalHits(100);
    hits->setSummaryFilled();
    hits->setHitVec(hitVec);
    ResultPtr resultPtr(new Result(hits));
    
    _processor->selectPages(distParam, resultPtr);
    hits = resultPtr->getHits();
    ASSERT_TRUE(hits);
    ASSERT_EQ((uint32_t)100, hits->totalHits());
    ASSERT_TRUE(hits->hasSummary());
    
    const vector<HitPtr> &hitVec2 = hits->getHitVec();
    PageDistinctSelectorTest::checkHits(hitVec2, "5, 7, 8, 9, 10");
}

TEST_F(PageDistinctProcessorTest, testSelectPagesFailed) {
    HA3_LOG(DEBUG, "Begin Test!");

    ResultPtr resultPtr;
    PageDistParam distParam;
    distParam.pgHitNum = 5;
    distParam.startOffset = 5;
    distParam.hitCount = 6;
    distParam.distKey = "key";
    distParam.distCount = 2;

    _processor->selectPages(distParam, resultPtr);
    ASSERT_TRUE(NULL == resultPtr.get());

    resultPtr.reset(new Result(ERROR_UNKNOWN, "unknown error"));
    _processor->selectPages(distParam, resultPtr);
    ASSERT_TRUE(NULL == resultPtr->getHits());
    
    resultPtr.reset(new Result(new Hits()));
    _processor->selectPages(distParam, resultPtr);
    ASSERT_TRUE(resultPtr->getHits());
    ASSERT_EQ((size_t)0, 
                         resultPtr->getHits()->getHitVec().size());
    
    HitPtr hitPtr;
    resultPtr->getHits()->addHit(hitPtr);
    _processor->selectPages(distParam, resultPtr);
    ASSERT_TRUE(resultPtr->hasError());
    checkError(resultPtr, ERROR_GENERAL, 
               "create PageDistinctSelector failed");
}

TEST_F(PageDistinctProcessorTest, testRestoreRequest) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    RequestPtr requestPtr(new Request);
    ConfigClause *configClause = new ConfigClause;
    requestPtr->setConfigClause(configClause);
    configClause->setStartOffset(0);
    configClause->setHitCount(20);
    DistinctClause *distClause = new DistinctClause;
    requestPtr->setDistinctClause(distClause);
    
    set<string> attrNames;
    PageDistParam distParam;
    distParam.startOffset = 5;
    distParam.hitCount = 6;
    
    _processor->restoreRequest(requestPtr, distParam, attrNames);
    
    ASSERT_EQ((uint32_t)5, configClause->getStartOffset());
    ASSERT_EQ((uint32_t)6, configClause->getHitCount());
    
    ASSERT_TRUE(!requestPtr->getDistinctClause());
    ASSERT_TRUE(!requestPtr->getAttributeClause());

    AttributeClause *attrClause = new AttributeClause;
    attrClause->addAttribute("attr1");
    attrClause->addAttribute("attr2");
    attrClause->addAttribute("attr3");
    requestPtr->setAttributeClause(attrClause);

    attrNames.insert("attr1");
    attrNames.insert("attr2");
    _processor->restoreRequest(requestPtr, distParam, attrNames);
    ASSERT_TRUE(requestPtr->getAttributeClause());
    set<string> attrs = requestPtr->getAttributeClause()->getAttributeNames();
    ASSERT_EQ((size_t)2, attrs.size());
    ASSERT_TRUE(attrs.find("attr1") != attrs.end());
    ASSERT_TRUE(attrs.find("attr2") != attrs.end());
}

template <typename T>
void PageDistinctProcessorTest::checkPageDistinctSelectorCreate(
        const PageDistParam &distParam, HitPtr hitPtr)
{
    PageDistinctSelectorPtr selector = 
        _processor->createPageDistinctSelector(distParam, hitPtr);
    ASSERT_TRUE(selector.get());
    ASSERT_EQ(distParam.pgHitNum, selector->_pgHitNum);
    ASSERT_EQ(distParam.distKey, selector->_distKey);
    ASSERT_EQ(distParam.distCount, selector->_distCount);
    PageDistinctSelectorTyped<T> *selectorTyped = 
        dynamic_cast<PageDistinctSelectorTyped<T>*>(selector.get());
    ASSERT_TRUE(selectorTyped);
}

ConfigClause* PageDistinctProcessorTest::createConfigClause() {
    ConfigClause *configClause = new ConfigClause;
    configClause->setOriginalString(
            "config=cluster:daogou,start:0,hit:5,"
            "kvpairs:pgNum#5;distKey#distKey;distCout#2;"
            "gradeThresholds#3.0");
    configClause->addClusterName("daogou");
    configClause->setStartOffset(0);
    configClause->setHitCount(5);
    configClause->addKVPair(PageDistinctProcessor::PAGE_DIST_PARAM_PAGE_SIZE, "5");
    configClause->addKVPair(PageDistinctProcessor::PAGE_DIST_PARAM_DIST_KEY, "distKey");
    configClause->addKVPair(PageDistinctProcessor::PAGE_DIST_PARAM_DIST_COUNT, "2");
    configClause->addKVPair(PageDistinctProcessor::PAGE_DIST_PARAM_GRADE_THRESHOLDS, "3.0");
    return configClause;
}

void PageDistinctProcessorTest::checkError(ResultPtr resultPtr,
        ErrorCode errorCode, 
        const string &errorMsg) 
{
    MultiErrorResultPtr multError = resultPtr->getMultiErrorResult();
    ASSERT_TRUE(multError);
    const ErrorResults &errorResults = multError->getErrorResults();
    ASSERT_EQ((size_t)1, errorResults.size());
    ASSERT_EQ(errorCode, 
                         errorResults[0].getErrorCode());
    ASSERT_EQ(errorMsg,
                         errorResults[0].getErrorMsg());
}

ResultPtr PageDistinctProcessorTest::createResult() {
    vector<HitPtr> hitVec;
    hitVec = PageDistinctSelectorTest::createHits("distKey", 
            "1,1,5.0;"
            "2,1,4.0;"
            "3,1,4.0;"
            "4,2,3.0;"
            "5,1,2.9;"
            "6,1,2.9;"
            "7,2,2.8;"
            "8,1,2.8;"
            "9,1,2.8;"
            "10,2,2.5;"
            "11,3,2.5;");
    Hits *hits = new Hits;
    for (size_t i = 0; i < hitVec.size(); ++i)
    {
        hits->addHit(hitVec[i]);
    }
    return ResultPtr(new Result(hits));
}

RequestPtr PageDistinctProcessorTest::createRequest() {
    RequestPtr requestPtr(new Request);

    ConfigClause *configClause = createConfigClause();
    requestPtr->setConfigClause(configClause);

    SortClause *sortClause = new  SortClause;
    sortClause->setOriginalString("type");
    SortDescription *sortDescription = new SortDescription("type");
    sortDescription->setExpressionType(SortDescription::RS_NORMAL);
    sortClause->addSortDescription(sortDescription);
    requestPtr->setSortClause(sortClause);
    return requestPtr;
}

END_HA3_NAMESPACE(qrs);

