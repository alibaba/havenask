#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Hit.h>
#include <ha3/test/test.h>
#include <string>
#include <ha3/common/Result.h>
#include <ha3/common/AggregateResult.h>
#include <ha3/common/test/ResultConstructor.h>

using namespace std;

USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(common);

class ResultTest : public TESTBASE {
public:
    ResultTest();
    ~ResultTest();
public:
    void setUp();
    void tearDown();
protected:
    void checkHit(common::HitPtr hitPtr, docid_t docid,
                  hashid_t hashid, clusterid_t clusterid);
protected:
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, ResultTest);


ResultTest::ResultTest() {
}

ResultTest::~ResultTest() {
}

void ResultTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _pool = new autil::mem_pool::Pool(10 * 1024 * 1024);
}

void ResultTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    delete _pool;
    _pool = NULL;
}

TEST_F(ResultTest, testLackResult) {
    Result result;
    EXPECT_FALSE(result._lackResult);
    EXPECT_FALSE(result.lackResult());
    result.setLackResult(false);
    EXPECT_FALSE(result.lackResult());
    result.setLackResult(true);
    EXPECT_TRUE(result.lackResult());
}

TEST_F(ResultTest, testGetAggregateResult) {
    HA3_LOG(DEBUG, "Begin Test!");
    Result result;
    AggregateResultPtr aggResultPtr(new AggregateResult());
    result.addAggregateResult(aggResultPtr);

    ASSERT_EQ((uint32_t)1, result.getAggregateResultCount());
    AggregateResultPtr aggResultRetPtr = result.getAggregateResult(0);
    ASSERT_TRUE(aggResultRetPtr.get());
}

TEST_F(ResultTest, testSerializeTracer) {
    HA3_LOG(DEBUG, "Begin Test!");

    Result result;
    common::TracerPtr tracer(new common::Tracer);
    tracer->trace("test");
    result.setTracer(tracer);

    Result expectedResult;

    autil::DataBuffer buffer;
    buffer.write(result);
    expectedResult.deserialize(buffer, _pool);

    common::Tracer* expectedTracer = expectedResult.getTracer();
    ASSERT_TRUE(expectedTracer != NULL);
    ASSERT_EQ(tracer->getTraceInfo(),
                         expectedTracer->getTraceInfo());
}

TEST_F(ResultTest, testSerializePhaseOneSearchInfo) {
    Result result;
    PhaseOneSearchInfo *searchInfo = new PhaseOneSearchInfo(1,2,3,4,5,6,7,8,9);
    result.setPhaseOneSearchInfo(searchInfo);
    result.setUseTruncateOptimizer(true);

    Result expectedResult;

    autil::DataBuffer buffer;
    buffer.write(result);
    expectedResult.deserialize(buffer, _pool);

    PhaseOneSearchInfo *expectedSearchInfo = expectedResult.getPhaseOneSearchInfo();
    ASSERT_TRUE(*searchInfo == *expectedSearchInfo);
    ASSERT_TRUE(result.useTruncateOptimizer() ==
                   expectedResult.useTruncateOptimizer());
}

TEST_F(ResultTest, testSerializeOtherInfoStr) {
    Result result;
    PhaseOneSearchInfo *phaseOneSearchInfo = new PhaseOneSearchInfo;
    phaseOneSearchInfo->seekDocCount = 2;
    result.setPhaseOneSearchInfo(phaseOneSearchInfo);

    PhaseTwoSearchInfo *phaseTwoSearchInfo = new PhaseTwoSearchInfo;
    phaseTwoSearchInfo->summaryLatency = 1;
    result.setPhaseTwoSearchInfo(phaseTwoSearchInfo);

    autil::DataBuffer dataBuffer;
    result.serialize(dataBuffer);
    Result expectedResult;
    expectedResult.deserialize(dataBuffer, _pool);

    auto expectedPhaseOneSearchInfo = expectedResult.getPhaseOneSearchInfo();
    auto expectedPhaseTwoSearchInfo = expectedResult.getPhaseTwoSearchInfo();
    ASSERT_TRUE(*phaseOneSearchInfo == *expectedPhaseOneSearchInfo);
    ASSERT_EQ(phaseTwoSearchInfo->summaryLatency, expectedPhaseTwoSearchInfo->summaryLatency);
}

TEST_F(ResultTest, testSerializeOriginOtherInfoStr) {
    Result result;
    PhaseOneSearchInfo *phaseOneSearchInfo = new PhaseOneSearchInfo;
    phaseOneSearchInfo->otherInfoStr = "123";
    phaseOneSearchInfo->seekDocCount = 2;
    result.setPhaseOneSearchInfo(phaseOneSearchInfo);

    PhaseTwoSearchInfo *phaseTwoSearchInfo = new PhaseTwoSearchInfo;
    phaseTwoSearchInfo->summaryLatency = 1;
    result.setPhaseTwoSearchInfo(phaseTwoSearchInfo);

    autil::DataBuffer dataBuffer;
    result.serialize(dataBuffer);
    Result expectedResult;
    expectedResult.deserialize(dataBuffer, _pool);
    auto expectedPhaseOneSearchInfo = expectedResult.getPhaseOneSearchInfo();
    auto expectedPhaseTwoSearchInfo = expectedResult.getPhaseTwoSearchInfo();
    ASSERT_TRUE(*phaseOneSearchInfo == *expectedPhaseOneSearchInfo);
    ASSERT_EQ(phaseTwoSearchInfo->summaryLatency, expectedPhaseTwoSearchInfo->summaryLatency);
}

TEST_F(ResultTest, testSerializeAndDeserializeEmptyResultWithoutJson) {
    HA3_LOG(DEBUG, "Begin Test!");
    autil::DataBuffer dataBuffer;
    Result result;

    result.serialize(dataBuffer);

    Result result2;
    result2.deserialize(dataBuffer, _pool);

    ASSERT_TRUE(!result2.getHits());
    ASSERT_TRUE(!result2.getMatchDocs());
    ASSERT_EQ((uint32_t)0, result2.getAggregateResultCount());
    ASSERT_EQ((size_t)0, result.getCoveredRanges().size());
}

TEST_F(ResultTest, testSerializeAndDeserializeWithoutJson) {
    HA3_LOG(DEBUG, "Begin Test!");
    Result result;

    Hits *hits = new Hits;
    hits->addHit(HitPtr(new Hit(100)));
    result.setHits(hits);

    ErrorResult mError;
    mError.setErrorCode(ERROR_ATTRIBUTE_NOT_EXIST);
    mError.setErrorMsg(haErrorCodeToString(ERROR_ATTRIBUTE_NOT_EXIST));
    result.addErrorResult(mError);

    MatchDocs* matchDocs = new MatchDocs();
    ResultConstructor resultConstructor;
    resultConstructor.fillMatchDocs(matchDocs, 1, 0, 10, 5, _pool,
                                    "1, 11, 777 # 2, 22, 888");
    result.setMatchDocs(matchDocs);

    AggregateResultPtr aggResultPtr(new AggregateResult());
    result.addAggregateResult(aggResultPtr);

    result.addCoveredRange(string("clustername"), (uint32_t)1, (uint32_t)3);
    result.addCoveredRange(string("clustername"), (uint32_t)5, (uint32_t)6);

    AttributeItemTyped<int32_t> *p1 = new AttributeItemTyped<int32_t>;
    *(p1->getPointer()) = 1;

    AttributeItemTyped<string> *p2 = new AttributeItemTyped<string>;
    *(p2->getPointer()) = "abc";

    AttributeItemTyped<vector<int32_t> > *p3 = new AttributeItemTyped<vector<int32_t> >;
    p3->getPointer()->push_back(10);

    AttributeItemMapPtr am(new AttributeItemMap);
    (*am)["int32_t"] = AttributeItemPtr(p1);
    (*am)["string"] = AttributeItemPtr(p2);
    (*am)["vector"] = AttributeItemPtr(p3);
    result.addGlobalVariableMap(am);

    autil::DataBuffer dataBuffer;
    result.serialize(dataBuffer);
    HA3_LOG(DEBUG, "serialize success!");
    Result result2;
    result2.deserialize(dataBuffer, _pool);
    HA3_LOG(DEBUG, "deserialize success!");

    Hits *hits2 = result2.getHits();
    ASSERT_TRUE(hits2);
    ASSERT_EQ((uint32_t)1, hits2->size());
    HitPtr hitPtr = hits2->getHit(0);
    ASSERT_TRUE(hitPtr.get());
    ASSERT_EQ((docid_t)100, hitPtr->getDocId());

    ASSERT_EQ((uint32_t)1, result2.getAggregateResultCount());
    AggregateResultPtr aggResultRetPtr = result2.getAggregateResult(0);
    ASSERT_TRUE(aggResultRetPtr.get());

    MatchDocs* matchDocsRet = result2.getMatchDocs();
    auto hashidRef = matchDocs->getHashIdRef();
    ASSERT_TRUE(matchDocsRet);
    ASSERT_EQ((uint32_t)10, matchDocsRet->totalMatchDocs());
    ASSERT_EQ((uint32_t)5, matchDocsRet->actualMatchDocs());
    ASSERT_EQ((uint32_t)2, matchDocsRet->size());
    ASSERT_EQ((docid_t)1, matchDocsRet->getDocId(0));
    ASSERT_EQ((hashid_t)11, hashidRef->get(matchDocsRet->getMatchDoc(0)));
    ASSERT_EQ((docid_t)2, matchDocsRet->getDocId(1));
    ASSERT_EQ((hashid_t)22, hashidRef->get(matchDocsRet->getMatchDoc(1)));
    ASSERT_TRUE(result2.hasError());
    ErrorResults errors = result2.getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)1, errors.size());
    ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST, errors[0].getErrorCode());
    ASSERT_EQ(haErrorCodeToString(ERROR_ATTRIBUTE_NOT_EXIST), errors[0].getErrorMsg());

    Result::ClusterPartitionRanges coveredRanges = result2.getCoveredRanges();
    ASSERT_EQ((size_t)1, coveredRanges.size());
    ASSERT_TRUE(coveredRanges.find("clustername") != coveredRanges.end());
    ASSERT_EQ((size_t)2, coveredRanges["clustername"].size());
    ASSERT_EQ((uint32_t)1, coveredRanges["clustername"][0].first);
    ASSERT_EQ((uint32_t)3, coveredRanges["clustername"][0].second);
    ASSERT_EQ((uint32_t)5, coveredRanges["clustername"][1].first);
    ASSERT_EQ((uint32_t)6, coveredRanges["clustername"][1].second);

    vector<AttributeItemMapPtr> globalVariables = result2.getGlobalVarialbes();
    ASSERT_EQ((size_t)1, globalVariables.size());
    AttributeItemMapPtr am2 = globalVariables[0];
    p1 = dynamic_cast<AttributeItemTyped<int32_t> *>(((*am2)["int32_t"]).get());
    ASSERT_EQ((int32_t)1, p1->getItem());

    p2 = dynamic_cast<AttributeItemTyped<string> *>(((*am2)["string"]).get());
    ASSERT_EQ((string)"abc", p2->getItem());

    p3 = dynamic_cast<AttributeItemTyped<vector<int32_t> > *>(((*am2)["vector"]).get());
    ASSERT_EQ((int32_t)10, (p3->getItem())[0]);
}

TEST_F(ResultTest, testMergeByAppend) {
    Hits *hits = new Hits;
    hits->addHit(HitPtr(new Hit(100)));
    hits->addHit(HitPtr(new Hit(200)));
    ResultPtr resultPtr1(new Result(hits));

    hits = new Hits;
    hits->addHit(HitPtr(new Hit(300)));
    ResultPtr resultPtr2(new Result(hits));

    AggregateResultPtr aggResultPtr1(new AggregateResult());
    resultPtr1->addAggregateResult(aggResultPtr1);

    AggregateResultPtr aggResultPtr2(new AggregateResult());
    resultPtr2->addAggregateResult(aggResultPtr2);

    resultPtr1->mergeByAppend(resultPtr2);
    Hits *newHits = resultPtr1->getHits();
    ASSERT_EQ((uint32_t)3, newHits->size());
    ASSERT_EQ((uint32_t)0, resultPtr2->getHits()->size());
    ASSERT_EQ((docid_t)100, newHits->getHit(0)->getDocId());
    ASSERT_EQ((docid_t)200, newHits->getHit(1)->getDocId());
    ASSERT_EQ((docid_t)300, newHits->getHit(2)->getDocId());

    ASSERT_EQ((uint32_t)2, resultPtr1->getAggregateResultCount());
    AggregateResultPtr aggResultRetPtr = resultPtr1->getAggregateResult(0);
    ASSERT_TRUE(aggResultRetPtr.get());
    aggResultRetPtr = resultPtr1->getAggregateResult(1);
    ASSERT_TRUE(aggResultRetPtr.get());

}

TEST_F(ResultTest, testMergeByAppendWithEmptyResult1) {
    Hits *hits = new Hits;
    ResultPtr resultPtr1(new Result(hits));

    hits = new Hits;
    hits->addHit(HitPtr(new Hit(100)));
    ResultPtr resultPtr2(new Result(hits));

    resultPtr1->mergeByAppend(resultPtr2);
    Hits *newHits = resultPtr1->getHits();
    ASSERT_EQ((uint32_t)1, newHits->size());
    ASSERT_EQ((uint32_t)0, resultPtr2->getHits()->size());
    ASSERT_EQ((docid_t)100, newHits->getHit(0)->getDocId());
}

TEST_F(ResultTest, testMergeByAppendWithEmptyResult2) {
    Hits *hits = new Hits;
    hits->addHit(HitPtr(new Hit(100)));
    hits->addHit(HitPtr(new Hit(200)));
    ResultPtr resultPtr1(new Result(hits));

    hits = new Hits;
    ResultPtr resultPtr2(new Result(hits));

    resultPtr1->mergeByAppend(resultPtr2);
    Hits *newHits = resultPtr1->getHits();
    ASSERT_EQ((uint32_t)2, newHits->size());
    ASSERT_EQ((uint32_t)0, resultPtr2->getHits()->size());
    ASSERT_EQ((docid_t)100, newHits->getHit(0)->getDocId());
    ASSERT_EQ((docid_t)200, newHits->getHit(1)->getDocId());
}

TEST_F(ResultTest, testMergeByAppendWithEmptyBoth) {
    Hits *hits = new Hits;
    ResultPtr resultPtr1(new Result(hits));

    hits = new Hits;
    ResultPtr resultPtr2(new Result(hits));

    resultPtr1->mergeByAppend(resultPtr2);
    Hits *newHits = resultPtr1->getHits();
    ASSERT_EQ((uint32_t)0, newHits->size());
    ASSERT_EQ((uint32_t)0, resultPtr2->getHits()->size());
}



TEST_F(ResultTest, testMergeByDefaultSort) {
    Hits *hits = new Hits;

    hits->addHit(HitPtr(new Hit(100, 0, "c1")));
    hits->getHit(0)->addSortExprValue("1.0", vt_double);
    hits->addHit(HitPtr(new Hit(200, 1, "c1")));
    hits->getHit(1)->addSortExprValue("2.0", vt_double);
    ResultPtr resultPtr1(new Result(hits));

    hits = new Hits;
    hits->addHit(HitPtr(new Hit(300, 0, "c2")));
    hits->getHit(0)->addSortExprValue("3.0", vt_double);
    ResultPtr resultPtr2(new Result(hits));

    resultPtr1->mergeByDefaultSort(resultPtr2);
    Hits *newHits = resultPtr1->getHits();
    ASSERT_EQ((uint32_t)3, newHits->size());
    ASSERT_EQ((uint32_t)0, resultPtr2->getHits()->size());
    ASSERT_EQ((docid_t)300, newHits->getHit(0)->getDocId());
    ASSERT_EQ((docid_t)200, newHits->getHit(1)->getDocId());
    ASSERT_EQ((docid_t)100, newHits->getHit(2)->getDocId());
}

TEST_F(ResultTest, testMergeByDefaultSortWithEmptyResult1) {
    Hits *hits = new Hits;
    ResultPtr resultPtr1(new Result(hits));

    hits = new Hits;
    hits->addHit(HitPtr(new Hit(100, 0, "c2")));
    hits->getHit(0)->addSortExprValue("1.0", vt_double);
    ResultPtr resultPtr2(new Result(hits));

    resultPtr1->mergeByDefaultSort(resultPtr2);
    Hits *newHits = resultPtr1->getHits();
    ASSERT_EQ((uint32_t)1, newHits->size());
    ASSERT_EQ((uint32_t)0, resultPtr2->getHits()->size());
    ASSERT_EQ((docid_t)100, newHits->getHit(0)->getDocId());
}

TEST_F(ResultTest, testMergeByDefaultSortWithEmptyResult2) {
    Hits *hits = new Hits;
    hits->addHit(HitPtr(new Hit(100, 0, "c1")));
    hits->getHit(0)->addSortExprValue("1.0", vt_double);
    ResultPtr resultPtr1(new Result(hits));

    hits = new Hits;
    ResultPtr resultPtr2(new Result(hits));

    resultPtr1->mergeByDefaultSort(resultPtr2);
    Hits *newHits = resultPtr1->getHits();
    ASSERT_EQ((uint32_t)1, newHits->size());
    ASSERT_EQ((uint32_t)0, resultPtr2->getHits()->size());
    ASSERT_EQ((docid_t)100, newHits->getHit(0)->getDocId());
}

TEST_F(ResultTest, testMergeByDefaultSortWithEmptyBoth) {
    Hits *hits = new Hits;
    ResultPtr resultPtr1(new Result(hits));

    hits = new Hits;
    ResultPtr resultPtr2(new Result(hits));

    resultPtr1->mergeByDefaultSort(resultPtr2);
    Hits *newHits = resultPtr1->getHits();
    ASSERT_EQ((uint32_t)0, newHits->size());
    ASSERT_EQ((uint32_t)0, resultPtr2->getHits()->size());
}

TEST_F(ResultTest, testMergeByDefaultSortMulti) {
    Hits *hits = new Hits;
    HitPtr hitPtr;
    hitPtr.reset(new Hit(1, 0, "c1", 1.0));
    hitPtr->setClusterId(0);
    hits->addHit(hitPtr);
    hitPtr.reset(new Hit(2, 0, "c1", 2.0));
    hitPtr->setClusterId(0);
    hits->addHit(hitPtr);
    hitPtr.reset(new Hit(1, 1, "c1", 1.0));
    hitPtr->setClusterId(0);
    hits->addHit(hitPtr);
    hitPtr.reset(new Hit(3, 1, "c1", 4.0));
    hitPtr->setClusterId(0);
    hits->addHit(hitPtr);
    ResultPtr resultPtr1(new Result(hits));

    hits = new Hits;
    hitPtr.reset(new Hit(1, 1, "c2", 1.0));
    hitPtr->setClusterId(1);
    hits->addHit(hitPtr);
    hitPtr.reset(new Hit(2, 1, "c2", 2.0));
    hitPtr->setClusterId(1);
    hits->addHit(hitPtr);
    hitPtr.reset(new Hit(2, 4, "c2", 0.1));
    hitPtr->setClusterId(1);
    hits->addHit(hitPtr);
    hitPtr.reset(new Hit(2, 3, "c2", 8.0));
    hitPtr->setClusterId(1);
    hits->addHit(hitPtr);
    ResultPtr resultPtr2(new Result(hits));

    resultPtr1->mergeByDefaultSort(resultPtr2);
    Hits *newHits = resultPtr1->getHits();
    ASSERT_EQ((uint32_t)8, newHits->size());
    ASSERT_EQ((uint32_t)0, resultPtr2->getHits()->size());

    checkHit(newHits->getHit(0), 2, 3, 1);
    checkHit(newHits->getHit(1), 3, 1, 0);
    checkHit(newHits->getHit(2), 2, 1, 1);
    checkHit(newHits->getHit(3), 2, 0, 0);
    checkHit(newHits->getHit(4), 1, 1, 1);
    checkHit(newHits->getHit(5), 1, 1, 0);
    checkHit(newHits->getHit(6), 1, 0, 0);
    checkHit(newHits->getHit(7), 2, 4, 1);
}

TEST_F(ResultTest, testGetGlobalVarialbes) {
    Result result;
    AttributeItemTyped<int32_t> *p1 = new AttributeItemTyped<int32_t>;
    *(p1->getPointer()) = 1;

    AttributeItemTyped<string> *p2 = new AttributeItemTyped<string>;
    *(p2->getPointer()) = "abc";

    AttributeItemTyped<vector<int32_t> > *p3 = new AttributeItemTyped<vector<int32_t> >;
    p3->getPointer()->push_back(10);

    AttributeItemMapPtr am(new AttributeItemMap);
    (*am)["int32_t"] = AttributeItemPtr(p1);
    (*am)["string"] = AttributeItemPtr(p2);
    (*am)["vector"] = AttributeItemPtr(p3);

    result.addGlobalVariableMap(am);
    result.addGlobalVariableMap(am);

    vector<int32_t *> v1 = result.getGlobalVarialbes<int32_t>("int32_t");
    ASSERT_EQ((size_t)2, v1.size());
    ASSERT_EQ((int32_t)1, *v1[0]);
    ASSERT_EQ((int32_t)1, *v1[1]);

    vector<string *> v2 = result.getGlobalVarialbes<string>("string");
    ASSERT_EQ((size_t)2, v2.size());
    ASSERT_EQ((string)"abc", *v2[0]);
    ASSERT_EQ((string)"abc", *v2[1]);

    vector<vector<int32_t> *> v3 = result.getGlobalVarialbes<vector<int32_t> >("vector");
    ASSERT_EQ((size_t)2, v3.size());
    ASSERT_EQ((size_t)1, v3[0]->size());
    ASSERT_EQ((int32_t)10, (*v3[0])[0]);
    ASSERT_EQ((size_t)1, v3[1]->size());
    ASSERT_EQ((int32_t)10, (*v3[1])[0]);
}

void ResultTest::checkHit(HitPtr hitPtr, docid_t docid,
                          hashid_t hashid, clusterid_t clusterid)
{
    ASSERT_EQ(docid, hitPtr->getDocId());
    ASSERT_EQ(hashid, hitPtr->getHashId());
    ASSERT_EQ(clusterid, hitPtr->getClusterId());
}

TEST_F(ResultTest, testMetaMap) {
    ResultPtr resultPtr(new Result());

    ASSERT_EQ((size_t)0, resultPtr->getMetaMap().size());
    Meta meta = {{"matchCount", "100"}};
    resultPtr->addMeta("searchInfo", meta);
    ASSERT_EQ((size_t)1, resultPtr->getMetaMap().size());
    ASSERT_EQ(string("100"), resultPtr->getMeta("searchInfo")["matchCount"]);
    Meta meta2 = {{"seekCount", "1000"}};
    resultPtr->addMeta("searchInfo", meta2);
    ASSERT_EQ((size_t)1, resultPtr->getMetaMap().size());
    ASSERT_EQ(string("1000"), resultPtr->getMeta("searchInfo")["seekCount"]);
    Meta meta3 = {{"time", "1"}};
    resultPtr->addMeta("qrsInfo", meta3);
    ASSERT_EQ((size_t)2, resultPtr->getMetaMap().size());
    ASSERT_EQ(string("1"), resultPtr->getMeta("qrsInfo")["time"]);
}

END_HA3_NAMESPACE(common);
