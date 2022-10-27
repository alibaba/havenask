#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/test/FakeRankExpression.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/framework/AttributeExpressionPool.h>
#include <ha3/search/SortExpressionCreator.h>
#include <ha3/rank/ReferenceComparator.h>
#include <matchdoc/MatchDoc.h>
#include <memory>
#include <ha3/rank/test/FakeRankExpression.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/framework/ComboAttributeExpression.h>
#include <matchdoc/Reference.h>
#include <ha3/rank/test/HitCollectorCaseHelper.h>
#include <ha3/rank/ComparatorCreator.h>
#include <ha3/rank/HitCollector.h>
#include <ha3/rank/NthElementCollector.h>
#include <sstream>
#include <cstdlib>
using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(rank);

class HitCollectorTest : public TESTBASE {
public:
    HitCollectorTest();
    ~HitCollectorTest();
public:
    void setUp();
    void tearDown();
protected:
    void internalTestPushAndPop(uint32_t queueSize, bool forceScore,
                                const std::string &pushOpteratorStr,
                                const std::string &popResultStr,
                                uint32_t evaluateCount, uint32_t deletedCount);
    template <typename T>
    void innerTestHitCollector(uint32_t queueSize, bool forceScore, 
                               const std::string &pushOpteratorStr,        
                               const std::string &popResultStr,
                               uint32_t evaluateCount, uint32_t deletedCount);

    matchdoc::MatchDoc createMatchDoc(docid_t docid, float score);
protected:
    FakeRankExpression *_expr;
    common::Ha3MatchDocAllocatorPtr _allocatorPtr;
    std::vector<FakeRankExpression*> _exprs;
    autil::mem_pool::Pool *_pool;
    search::SortExpressionCreator *_sortExpressionCreator;
    search::SortExpression *_sortExpr;
    common::MultiErrorResultPtr _errorResult;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, HitCollectorTest);


HitCollectorTest::HitCollectorTest() {
    _pool = NULL;
    _sortExpressionCreator = NULL;
}

HitCollectorTest::~HitCollectorTest() { 
}

void HitCollectorTest::setUp() { 
    _pool = new mem_pool::Pool(1024);
    _sortExpressionCreator = new SortExpressionCreator(NULL, NULL, NULL, 
            _errorResult, _pool);
    common::Ha3MatchDocAllocatorPtr allocatorPtr(new common::Ha3MatchDocAllocator(_pool));
    _allocatorPtr = allocatorPtr;
    _expr = new FakeRankExpression("fake_ref");
    _expr->allocate(_allocatorPtr.get());
    _sortExpr = _sortExpressionCreator->createSortExpression(_expr, false);
    _exprs.push_back(_expr);
}

void HitCollectorTest::tearDown() { 
    _exprs.clear();
    DELETE_AND_SET_NULL(_expr);
    _allocatorPtr.reset();
    DELETE_AND_SET_NULL(_sortExpressionCreator);
    DELETE_AND_SET_NULL(_pool);
}

TEST_F(HitCollectorTest, testLargeDocs) { 
    for(int k = 0; k < 20 ; k++){
        const int DOC_COUNT = 10000;
        const int DEL_COUNT = 8000;
        vector<int> docIdVec(DOC_COUNT, 0);
        for(int i = 0; i< DOC_COUNT; i++ ){
            docIdVec[i] = i;
        }
        srand(time(NULL));
        set<int> delDocSet;
        for(int i = 0; i < DEL_COUNT; i++ ){
            delDocSet.insert(std::rand() % DOC_COUNT);
        }
        random_shuffle(docIdVec.begin(), docIdVec.end());
        ostringstream oss;
        for(int i = 0; i< DOC_COUNT; i++ ){
            if(delDocSet.count(docIdVec[i]) > 0){
                oss << docIdVec[i] << "," << docIdVec[i] <<",d"<< ";";
            }else{
                oss << docIdVec[i] << "," << docIdVec[i] << ";";
            }
         }
        string pushStr = oss.str();
        oss.str("");
        int requestDoc = 128;
        int cnt = 0, cur = DOC_COUNT - 1;
        while (cnt < requestDoc && cur >= 0){
            if(delDocSet.count(cur) > 0 ){
                cur -- ;
                continue;
            }
            oss << cur <<", ";
            cnt ++;
            cur --;
        }
        string popStr = oss.str();
        if(k % 2 != 0){
            internalTestPushAndPop(requestDoc, true, pushStr, popStr, DOC_COUNT, delDocSet.size());
        }else {
            internalTestPushAndPop(requestDoc, false, pushStr, popStr, DOC_COUNT, delDocSet.size());
        }
    }
}

TEST_F(HitCollectorTest, testIsRealMinDocInBuffer) {
    string pushOperatorStr = "3,3; 4,3,d; 5,2; 6,2.5f ";
    string popResultStr = "3,6";
    internalTestPushAndPop(2, false, pushOperatorStr, popResultStr, 4, 1);
}



TEST_F(HitCollectorTest, testHitCollector) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string pushOperatorStr = "3,1f;"
                             "6,4.3f;";
    string popResultStr = "3,6";
    internalTestPushAndPop(100, true, pushOperatorStr, popResultStr, 2, 0);
}

TEST_F(HitCollectorTest, testNoSortHitCollector) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string pushOperatorStr = "3,1f;"
                             "6,4.3f;";
    string popResultStr = "6,3";
    internalTestPushAndPop(100, false, pushOperatorStr, popResultStr, 0, 0);
}

TEST_F(HitCollectorTest, testHitColletorKeepCount) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    stringstream ss;
    for (size_t i = 0; i < 100; ++i) {
        ss << i << "," << i << ";";
    }
    string pushOperatorStr = ss.str() + "101,0.5;102,33.0f";
    ss.str("");
    for (size_t i = 1; i <= 32; ++i) {
        ss << i << ",";
    }
    ss << "102,";
    for (size_t i = 33; i <= 99; ++i) {
        ss << i << ",";
    }
    string popResultStr = ss.str();
    internalTestPushAndPop(100, true, pushOperatorStr, popResultStr, 102, 0);
}

TEST_F(HitCollectorTest, testHitColletorAutoSort) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    stringstream ss;
    for (size_t i = 0; i < 100; ++i) {
        ss << i << "," << i << ";";
    }
    string pushOperatorStr = ss.str() + "101,0.5;102,33.0f";
    ss.str("");
    for (size_t i = 1; i <= 32; ++i) {
        ss << i << ",";
    }
    ss << "102,";
    for (size_t i = 33; i <= 99; ++i) {
        ss << i << ",";
    }
    string popResultStr = ss.str();
    internalTestPushAndPop(100, false, pushOperatorStr, popResultStr, 102, 0);
}

TEST_F(HitCollectorTest, testHitColletorDeleteMatchDocAutoSort) {
    ComparatorCreator creator(_pool, false);
    SortExpressionVector sortExprVector;
    sortExprVector.push_back(_sortExpr);
    ComboComparator *comboComp = creator.createSortComparator(sortExprVector);
    HitCollector hitCollector(10, _pool, _allocatorPtr, _expr, comboComp);

    stringstream ss;
    for (size_t i = 0; i < 100; ++i) {
        ss << i << "," << i << (i % 2 ? "" : ",d") << ";";
    }
    string pushOperatorStr = ss.str();
    ss.str("");
    for (size_t i = 40; i < 50; ++i) {
        ss << i * 2 + 1 << ",";
    }
    string popResultStr = ss.str();
    internalTestPushAndPop(10, true, pushOperatorStr, popResultStr, 100, 50);
}

TEST_F(HitCollectorTest, testHitColletorDeleteMatchDoc) {
    ComparatorCreator creator(_pool, false);
    SortExpressionVector sortExprVector;
    sortExprVector.push_back(_sortExpr);
    ComboComparator *comboComp = creator.createSortComparator(sortExprVector);
    HitCollector hitCollector(10, _pool, _allocatorPtr, _expr, comboComp);

    stringstream ss;
    for (size_t i = 0; i < 100; ++i) {
        ss << i << "," << i << (i % 2 ? "" : ",d") << ";";
    }
    string pushOperatorStr = ss.str();
    ss.str("");
    for (size_t i = 40; i < 50; ++i) {
        ss << i * 2 + 1 << ",";
    }
    string popResultStr = ss.str();
    internalTestPushAndPop(10, false, pushOperatorStr, popResultStr, 100, 50);
}

TEST_F(HitCollectorTest, testBatchEvaluateEmpty) {
    ComparatorCreator creator(_pool, false);
    SortExpressionVector sortExprVector;
    sortExprVector.push_back(_sortExpr);
    ComboComparator *comboComp = creator.createSortComparator(sortExprVector);
    HitCollector hitCollector(100, _pool, _allocatorPtr, _expr, comboComp);
    hitCollector.flush();
}

TEST_F(HitCollectorTest, testDisableBatchEvaluate) {
    ComparatorCreator creator(_pool, false);
    SortExpressionVector sortExprVector;
    sortExprVector.push_back(_sortExpr);
    ComboComparator *comboComp = creator.createSortComparator(sortExprVector);
    HitCollector hitCollector(10, _pool, _allocatorPtr, _expr, comboComp);
    hitCollector.disableBatchScore();
    string pushOperStr = "0,1;"
                         "1,2;"
                         "2,3,d;"
                         "3,0";
    HitCollectorCaseHelper::makeData(&hitCollector, pushOperStr, _exprs, _allocatorPtr);
    ASSERT_EQ(uint32_t(0), hitCollector.getItemCount());
    hitCollector.flush();
    ASSERT_EQ(uint32_t(3), hitCollector.getItemCount());
    string popOperStr = "3,0,1";
    HitCollectorCaseHelper::checkLeftMatchDoc(&hitCollector, popOperStr, _allocatorPtr);
}

TEST_F(HitCollectorTest, testBatchEvaluateWithDel) {
    ComparatorCreator creator(_pool, false);
    SortExpressionVector sortExprVector;
    sortExprVector.push_back(_sortExpr);
    ComboComparator *comboComp = creator.createSortComparator(sortExprVector);
    HitCollector hitCollector(10, _pool, _allocatorPtr, _expr, comboComp);
    string pushOperStr = "0,1;"
                         "1,2;"
                         "2,3,d;"
                         "3,0";
    HitCollectorCaseHelper::makeData(&hitCollector, pushOperStr, _exprs, _allocatorPtr);
    ASSERT_EQ(uint32_t(0), hitCollector.getItemCount());
    hitCollector.flush();
    ASSERT_EQ(uint32_t(3), hitCollector.getItemCount());
    string popOperStr = "3,0,1";
    HitCollectorCaseHelper::checkLeftMatchDoc(&hitCollector, popOperStr, _allocatorPtr);
}

TEST_F(HitCollectorTest, testBatchEvaluateWithDelAll) {
    ComparatorCreator creator(_pool, false);
    SortExpressionVector sortExprVector;
    sortExprVector.push_back(_sortExpr);
    ComboComparator *comboComp = creator.createSortComparator(sortExprVector);
    HitCollector hitCollector(10, _pool, _allocatorPtr, _expr, comboComp);
    string pushOperStr = "0,1,d;"
                         "1,2,d;"
                         "2,3,d;"
                         "3,0,d";
    HitCollectorCaseHelper::makeData(&hitCollector, pushOperStr, _exprs, _allocatorPtr);
    ASSERT_EQ(uint32_t(0), hitCollector.getItemCount());
    hitCollector.flush();
    ASSERT_EQ(uint32_t(0), hitCollector.getItemCount());
    string popOperStr = "";
    HitCollectorCaseHelper::checkLeftMatchDoc(&hitCollector, popOperStr, _allocatorPtr);
}
 
void HitCollectorTest::internalTestPushAndPop(
        uint32_t queueSize, bool forceScore, const string &pushOpteratorStr,
        const string &popResultStr, uint32_t evaluateCount, uint32_t deletedCount)
{
    innerTestHitCollector<HitCollector>(queueSize, forceScore, pushOpteratorStr,
            popResultStr, evaluateCount, deletedCount);
    innerTestHitCollector<NthElementCollector>(queueSize, forceScore, pushOpteratorStr,
            popResultStr, evaluateCount, deletedCount);
}
template <typename T>
void HitCollectorTest::innerTestHitCollector(
        uint32_t queueSize, bool forceScore, const string &pushOpteratorStr,        
        const string &popResultStr, uint32_t evaluateCount, uint32_t deletedCount)
{
    _expr->resetEvaluateTimes();
    ComparatorCreator creator(_pool, false);
    SortExpressionVector sortExprVector;
    sortExprVector.push_back(_sortExpr);
    ComboComparator *comboComp = creator.createSortComparator(sortExprVector);
    T hitCollector(queueSize, _pool, _allocatorPtr, _expr, comboComp);

    if (!forceScore) {
        hitCollector.enableLazyScore(queueSize);
    }
    HitCollectorCaseHelper::makeData(&hitCollector, pushOpteratorStr, _exprs, _allocatorPtr);
    HitCollectorCaseHelper::checkLeftMatchDoc(&hitCollector, popResultStr, _allocatorPtr);
    ASSERT_EQ(evaluateCount, _expr->getEvaluateCount());
    ASSERT_EQ(deletedCount, hitCollector.getDeletedDocCount());
}

END_HA3_NAMESPACE(rank);

