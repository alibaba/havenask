#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/MultiHitCollector.h>
#include <ha3/rank/test/HitCollectorCaseHelper.h>
#include <suez/turing/expression/framework/AttributeExpressionPool.h>
#include <ha3/search/SortExpressionCreator.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <ha3/rank/test/FakeRankExpression.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/framework/ComboAttributeExpression.h>
#include <matchdoc/Reference.h>
#include <ha3/rank/ComparatorCreator.h>
#include <ha3/rank/HitCollector.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(rank);

class MultiHitCollectorTest : public TESTBASE {
public:
    MultiHitCollectorTest();
    ~MultiHitCollectorTest();
public:
    void setUp();
    void tearDown();
protected:
    void innerTestMultiHitCollector(const std::string &queueSize,
                                    bool lazyScore,
                                    const std::string &pushOpteratorStr,
                                    const std::string &popResultStr,
                                    uint32_t deletedCount);
protected:
    common::Ha3MatchDocAllocatorPtr _allocatorPtr;
    autil::mem_pool::Pool *_pool;
    search::SortExpressionCreator *_sortExpressionCreator;
    common::MultiErrorResultPtr _errorResult;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, MultiHitCollectorTest);


MultiHitCollectorTest::MultiHitCollectorTest() {
    _pool = NULL;
    _sortExpressionCreator = NULL;
}

MultiHitCollectorTest::~MultiHitCollectorTest() { 
}

void MultiHitCollectorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _pool = new mem_pool::Pool(1024);    
    _sortExpressionCreator = new SortExpressionCreator(NULL, NULL, NULL,
            _errorResult, _pool);
}

void MultiHitCollectorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    _allocatorPtr.reset();
    DELETE_AND_SET_NULL(_sortExpressionCreator);
    DELETE_AND_SET_NULL(_pool);    
}

TEST_F(MultiHitCollectorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string pushOpteratorStr = "0,10,20;"
                              "1,5,30;"
                              "2,7,40;"
                              "3,20,1;"
                              "4,13,1000;"
                              "5,1,100;";
    string popResultStr = "1,2,5,0,4,3";
    innerTestMultiHitCollector("3,3", false, pushOpteratorStr, popResultStr, 0);
}

TEST_F(MultiHitCollectorTest, testOneHitCollector) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string pushOpteratorStr = "0,10;"
                              "1,30;"
                              "2,7;"
                              "3,1;"
                              "4,13;"
                              "5,2;";
    string popResultStr = "5,2,0,4,1";
    innerTestMultiHitCollector("5", false, pushOpteratorStr, popResultStr, 0);
}

TEST_F(MultiHitCollectorTest, testThreeHitCollector) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string pushOpteratorStr = "0,10,20,1;"
                              "1,5,30,2;"
                              "2,7,40,3;"
                              "3,20,1,4;"
                              "4,13,1000,5;"
                              "5,1,105,6;"
                              "6,2,104,7;"
                              "7,1,103,8;"
                              "8,4,102,9;"
                              "9,5,100,10;";
    string popResultStr = "2,8,9,7,6,5,0,4,3";
    innerTestMultiHitCollector("3,3,3", false, pushOpteratorStr, popResultStr, 0);
}

TEST_F(MultiHitCollectorTest, testThreeHitCollectorNotFull) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string pushOpteratorStr = "0,10,20,1;"
                              "1,5,30,2;"
                              "2,7,40,3;"
                              "3,20,1,4;";
    string popResultStr = "1,2,0,3";
    innerTestMultiHitCollector("3,3,3", false, pushOpteratorStr, popResultStr, 0);
}

TEST_F(MultiHitCollectorTest, testThreeHitCollectorWithDel) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string pushOpteratorStr = "0,10,20,1;"
                              "1,5,30,2;"
                              "2,7,40,3;"
                              "3,20,1,4,d;"
                              "4,13,1000,5;"
                              "5,1,105,6;"
                              "6,2,104,7,d;"
                              "7,1,103,8;"
                              "8,4,102,9,d;"
                              "9,5,100,10;";
    string popResultStr = "2,9,7,5,0,4";
    innerTestMultiHitCollector("2,2,2", false, pushOpteratorStr, popResultStr, 3);
}

TEST_F(MultiHitCollectorTest, testThreeHitCollectorLazyScore) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string pushOpteratorStr = "0,10,20,1;"
                              "1,5,30,2;"
                              "2,7,40,3;"
                              "3,20,1,4;"
                              "4,13,1000,5;"
                              "5,1,105,6;"
                              "6,2,104,7;"
                              "7,1,103,8;"
                              "8,4,102,9;"
                              "9,5,100,10;";
    string popResultStr = "2,8,9,7,6,5,0,4,3";
    innerTestMultiHitCollector("3,3,3", true, pushOpteratorStr, popResultStr, 0);
}

TEST_F(MultiHitCollectorTest, testThreeHitCollectorWithDelLazyScore) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string pushOpteratorStr = "0,10,20,1;"
                              "1,5,30,2;"
                              "2,7,40,3;"
                              "3,20,1,4,d;"
                              "4,13,1000,5;"
                              "5,1,105,6;"
                              "6,2,104,7,d;"
                              "7,1,103,8;"
                              "8,4,102,9,d;"
                              "9,5,100,10;";
    string popResultStr = "2,9,7,5,0,4";
    innerTestMultiHitCollector("2,2,2", true, pushOpteratorStr, popResultStr, 3);
}

TEST_F(MultiHitCollectorTest, testThreeHitCollectorNotFullLazyScore) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string pushOpteratorStr = "0,10,20,1;"
                              "1,5,30,2;"
                              "2,7,40,3;"
                              "3,20,1,4;";
    string popResultStr = "3,2,1,0";
    innerTestMultiHitCollector("3,3,3", true, pushOpteratorStr, popResultStr, 0);
}

void MultiHitCollectorTest::innerTestMultiHitCollector(
        const string &queueSize, bool lazyScore, const string &pushOpteratorStr,
        const string &popResultStr, uint32_t deletedCount)
{
    _allocatorPtr.reset(new common::Ha3MatchDocAllocator(_pool));
    StringTokenizer st(queueSize, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    vector<uint32_t> queueSizes;
    uint32_t totalSize = 0;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        uint32_t size = StringUtil::fromString<uint32_t>(st[i].c_str());
        totalSize += size;
        queueSizes.push_back(size);
    }

    vector<SortExpressionVector> rankSortExpressions;
    vector<FakeRankExpression*> exprs;
    for (size_t i = 0; i < queueSizes.size(); ++i) {
        FakeRankExpression *expr = new FakeRankExpression("ref_" + StringUtil::toString(i));
        exprs.push_back(expr);
        expr->allocate(_allocatorPtr.get());
        SortExpression* sortExpr = 
            _sortExpressionCreator->createSortExpression(expr, false);
        SortExpressionVector sortExprVec;
        sortExprVec.push_back(sortExpr);
        rankSortExpressions.push_back(sortExprVec);
    }

    ComparatorCreator creator(_pool, false);
    vector<ComboComparator*> rankComps =
        creator.createRankSortComparators(rankSortExpressions);
    const vector<AttributeExpression*> &immEvaluateExprs =
        creator.getImmEvaluateExpressions();

    ComboAttributeExpression attrForHitCollector;
    attrForHitCollector.setExpressionVector(ExpressionVector(
                    immEvaluateExprs.begin(), immEvaluateExprs.end()));
    
    MultiHitCollector multiCollector(_pool, _allocatorPtr, &attrForHitCollector);
    if (lazyScore) {
        multiCollector.enableLazyScore(totalSize);
    }
    for (size_t i = 0; i < queueSizes.size(); ++i) {
        HitCollector *hitCollector = POOL_NEW_CLASS(_pool, HitCollector,
                queueSizes[i], _pool, _allocatorPtr, NULL, rankComps[i]);
        multiCollector.addHitCollector(hitCollector);
    }
    HitCollectorCaseHelper::makeData(&multiCollector, pushOpteratorStr, exprs,
            _allocatorPtr);
    multiCollector.flush();
    HitCollectorCaseHelper::checkLeftMatchDoc(&multiCollector, popResultStr,
            _allocatorPtr);
    ASSERT_EQ(deletedCount, multiCollector.getDeletedDocCount());
    for (size_t i = 0; i < exprs.size(); ++i) {
        delete exprs[i];
    }
}

END_HA3_NAMESPACE(rank);

