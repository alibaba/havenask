#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/ComparatorCreator.h>
#include <suez/turing/expression/framework/ComboAttributeExpression.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/rank/ReferenceComparator.h>
#include <ha3/rank/ExpressionComparator.h>
#include <ha3/rank/test/FakeExpression.h>
#include <ha3/rank/test/FakeRankExpression.h>
#include <autil/mem_pool/Pool.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(rank);

class ComparatorCreatorTest : public TESTBASE {
public:
    ComparatorCreatorTest();
    ~ComparatorCreatorTest();
public:
    void setUp();
    void tearDown();
protected:
    void innerTestCreateSortComparator(bool allowLazyEvaluate, uint32_t count);
    void innerTestCreateSortComparator(ComparatorCreator &creator,
            bool allowLazyEvaluate, uint32_t count);
    void innerTestCreateRankSortExpression(
            ComparatorCreator &creator,
            uint32_t attrCount,
            const std::string &rankSortStr,
            const std::string &expectCompStr,
            const std::string &expectImmExprStr);
    void innerTestComparatorType(
            const Comparator *comp,
            ComparatorCreator::ComparatorType type = ComparatorCreator::CT_REFERENCE,
            bool sortFlag = false);
protected:
    autil::mem_pool::Pool *_pool;
    search::SortExpressionCreator *_sortExprCreator;
    common::MultiErrorResultPtr _errorResult;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, ComparatorCreatorTest);


ComparatorCreatorTest::ComparatorCreatorTest() {
    _pool = new autil::mem_pool::Pool(1024);
    _sortExprCreator = new SortExpressionCreator(NULL, NULL, 
            NULL, _errorResult, _pool);
}

ComparatorCreatorTest::~ComparatorCreatorTest() {
    delete _pool;
    delete _sortExprCreator;
}

void ComparatorCreatorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void ComparatorCreatorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ComparatorCreatorTest, testAllowLazyEvaluate) {
    for (uint32_t exprCount = 1; exprCount <= 40; ++exprCount) {
        innerTestCreateSortComparator(true, exprCount);
    }
}

TEST_F(ComparatorCreatorTest, testNotAllowLazyEvaluate) {
    for (uint32_t exprCount = 1; exprCount <= 40; ++exprCount) {
        innerTestCreateSortComparator(false, exprCount);
    }
}

TEST_F(ComparatorCreatorTest, testCreateComparator) {
    FakeExpression *fakeExpr = new FakeExpression;
    fakeExpr->setExpressionId(1);
    unique_ptr<AttributeExpression> expr(fakeExpr);
    SortExpression *sortExpr =
        _sortExprCreator->createSortExpression(expr.get(), false);
    sortExpr->setSortFlag(true);
    Comparator *comp1 = ComparatorCreator::createComparator(_pool, sortExpr);
    innerTestComparatorType(comp1, ComparatorCreator::CT_REFERENCE, true);
    POOL_DELETE_CLASS(comp1);

    sortExpr->setSortFlag(false);
    Comparator *comp2 = ComparatorCreator::createComparator(_pool, sortExpr);
    innerTestComparatorType(comp2, ComparatorCreator::CT_REFERENCE, false);
    POOL_DELETE_CLASS(comp2);

    Comparator *comp3 = ComparatorCreator::createComparator(_pool, sortExpr,
            ComparatorCreator::CT_EXPRESSION);
    innerTestComparatorType(comp3, ComparatorCreator::CT_EXPRESSION, false);
    POOL_DELETE_CLASS(comp3);
}

TEST_F(ComparatorCreatorTest, testCreateOptimizedComparatorOneRef) {
    {
        suez::turing::AttributeExpressionTyped<int32_t> exprInt32;
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(&exprInt32, false);
        sortExpr->setSortFlag(true);
        Comparator *comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr,
                ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp != NULL);
        ASSERT_EQ("one_ref", comp->getType());
        auto oneRef = dynamic_cast<OneRefComparatorTyped<int32_t> *>(comp); 
        ASSERT_TRUE(oneRef->_sortFlag);
    }   
    {
        suez::turing::AttributeExpressionTyped<int64_t> exprInt64;
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(&exprInt64, false);
        sortExpr->setSortFlag(false);
        Comparator *comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr,
                ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp != NULL);
        ASSERT_EQ("one_ref", comp->getType());
        auto oneRef = dynamic_cast<OneRefComparatorTyped<int64_t> *>(comp); 
        ASSERT_FALSE(oneRef->_sortFlag);
    }   
    {
        suez::turing::AttributeExpressionTyped<double> exprDouble;
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(&exprDouble, false);
        sortExpr->setSortFlag(true);
        Comparator *comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, 
                ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp != NULL);
        ASSERT_EQ("one_ref", comp->getType());
        auto oneRef = dynamic_cast<OneRefComparatorTyped<double> *>(comp); 
        ASSERT_TRUE(oneRef->_sortFlag);
    }   
    {
        suez::turing::AttributeExpressionTyped<uint32_t> exprUint32;
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(&exprUint32, false);
        sortExpr->setSortFlag(true);
        Comparator *comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, 
                ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp == NULL);
    }   
    {
        suez::turing::AttributeExpressionTyped<int32_t> exprInt32;
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(&exprInt32, false);
        sortExpr->setSortFlag(true);
        Comparator *comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr,
                ComparatorCreator::CT_EXPRESSION);
        ASSERT_TRUE(comp == NULL);
    }   
}

TEST_F(ComparatorCreatorTest, testCreateOptimizedComparatorTwo) {
    {
        suez::turing::AttributeExpressionTyped<int32_t> exprInt32;
        suez::turing::AttributeExpressionTyped<int32_t> exprInt32s;
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(&exprInt32, false);
        sortExpr->setSortFlag(true);
        SortExpression *sortExpr2 =
            _sortExprCreator->createSortExpression(&exprInt32s, false);
        sortExpr2->setSortFlag(true);
        Comparator *comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2,
                ComparatorCreator::CT_REFERENCE, ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp != NULL);
        ASSERT_EQ("two_ref", comp->getType());
        auto twoRef = dynamic_cast<TwoRefComparatorTyped<int32_t, int32_t> *>(comp); 
        ASSERT_TRUE(twoRef->_sortFlag1);
        ASSERT_TRUE(twoRef->_sortFlag2);

        comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2,
                ComparatorCreator::CT_REFERENCE, ComparatorCreator::CT_EXPRESSION);
        ASSERT_TRUE(comp != NULL);
        ASSERT_EQ("ref_expr", comp->getType());
        auto refAndExpr = dynamic_cast<RefAndExprComparatorTyped<int32_t, int32_t> *>(comp); 
        ASSERT_TRUE(refAndExpr->_sortFlag1);
        ASSERT_TRUE(refAndExpr->_sortFlag2);

        comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2,
                ComparatorCreator::CT_EXPRESSION, ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp == NULL);
    }   
    {
        suez::turing::AttributeExpressionTyped<int64_t> exprInt64;
        suez::turing::AttributeExpressionTyped<double> exprDouble;
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(&exprInt64, false);
        sortExpr->setSortFlag(false);
        SortExpression *sortExpr2 =
            _sortExprCreator->createSortExpression(&exprDouble, false);
        sortExpr2->setSortFlag(false);
        Comparator *comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2,
                ComparatorCreator::CT_REFERENCE, ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp != NULL);
        ASSERT_EQ("two_ref", comp->getType());
        auto twoRef = dynamic_cast<TwoRefComparatorTyped<int64_t, double> *>(comp); 
        ASSERT_FALSE(twoRef->_sortFlag1);
        ASSERT_FALSE(twoRef->_sortFlag2);

        comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2,
                ComparatorCreator::CT_REFERENCE, ComparatorCreator::CT_EXPRESSION);
        ASSERT_TRUE(comp != NULL);
        ASSERT_EQ("ref_expr", comp->getType());
        auto refAndExpr = dynamic_cast<RefAndExprComparatorTyped<int64_t, double> *>(comp); 
        ASSERT_FALSE(refAndExpr->_sortFlag1);
        ASSERT_FALSE(refAndExpr->_sortFlag2);

        comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2,
                ComparatorCreator::CT_EXPRESSION, ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp == NULL);
    }   
    {
        suez::turing::AttributeExpressionTyped<int64_t> exprInt64;
        suez::turing::AttributeExpressionTyped<uint32_t> exprUint32;
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(&exprInt64, false);
        sortExpr->setSortFlag(false);
        SortExpression *sortExpr2 =
            _sortExprCreator->createSortExpression(&exprUint32, false);
        sortExpr2->setSortFlag(false);
        Comparator *comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2,
                ComparatorCreator::CT_REFERENCE, ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp == NULL);
    }
}

TEST_F(ComparatorCreatorTest, testCreateOptimizedComparatorThree) {
    {
        suez::turing::AttributeExpressionTyped<int64_t> exprInt64;
        suez::turing::AttributeExpressionTyped<double> exprDouble;
        suez::turing::AttributeExpressionTyped<int32_t> exprInt32;
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(&exprInt64, false);
        sortExpr->setSortFlag(true);
        SortExpression *sortExpr2 =
            _sortExprCreator->createSortExpression(&exprDouble, false);
        sortExpr2->setSortFlag(true);
        SortExpression *sortExpr3 =
            _sortExprCreator->createSortExpression(&exprInt32, false);
        sortExpr3->setSortFlag(true);

        Comparator *comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2,
                sortExpr3, ComparatorCreator::CT_REFERENCE,
                ComparatorCreator::CT_REFERENCE, ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp != NULL);
        ASSERT_EQ("three_ref", comp->getType());
        auto threeRef = dynamic_cast<ThreeRefComparatorTyped<int64_t, double, int32_t> *>(comp); 
        ASSERT_TRUE(threeRef->_sortFlag1);
        ASSERT_TRUE(threeRef->_sortFlag2);
        ASSERT_TRUE(threeRef->_sortFlag3);

        comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2,
                sortExpr3, ComparatorCreator::CT_REFERENCE,
                ComparatorCreator::CT_REFERENCE, ComparatorCreator::CT_EXPRESSION);
        ASSERT_TRUE(comp != NULL);
        ASSERT_EQ("two_ref_expr", comp->getType());
        auto refAndExpr = dynamic_cast<TwoRefAndExprComparatorTyped<int64_t, double, int32_t> *>(comp); 
        ASSERT_TRUE(refAndExpr->_sortFlag1);
        ASSERT_TRUE(refAndExpr->_sortFlag2);
        ASSERT_TRUE(refAndExpr->_sortFlag3);

        sortExpr->setSortFlag(false);
        sortExpr2->setSortFlag(false);
        sortExpr3->setSortFlag(false);

        comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2,
                sortExpr3, ComparatorCreator::CT_REFERENCE,
                ComparatorCreator::CT_REFERENCE, ComparatorCreator::CT_EXPRESSION);
        ASSERT_TRUE(comp != NULL);
        ASSERT_EQ("two_ref_expr", comp->getType());
        refAndExpr = dynamic_cast<TwoRefAndExprComparatorTyped<int64_t, double, int32_t> *>(comp); 
        ASSERT_FALSE(refAndExpr->_sortFlag1);
        ASSERT_FALSE(refAndExpr->_sortFlag2);
        ASSERT_FALSE(refAndExpr->_sortFlag3);
        
        comp = ComparatorCreator::createOptimizedComparator(_pool, sortExpr, sortExpr2, sortExpr3,
                ComparatorCreator::CT_REFERENCE,
                ComparatorCreator::CT_EXPRESSION, ComparatorCreator::CT_REFERENCE);
        ASSERT_TRUE(comp == NULL);
    }   
}

TEST_F(ComparatorCreatorTest, testReentry) {
    ComparatorCreator creator(_pool, true);
    innerTestCreateSortComparator(creator, true, 5);
    innerTestCreateSortComparator(creator, true, 5);
    innerTestCreateSortComparator(creator, true, 5);
}

TEST_F(ComparatorCreatorTest, testCreateRankSortComparators) {
    ComparatorCreator creator(_pool, true);
    // attribute 0 is rank atttribute expression
    innerTestCreateRankSortExpression(creator, 20,
            "0,1,2,1,0,3", "REEERE", "0");
    innerTestCreateRankSortExpression(creator, 20,
            "1,2,1,0,3", "RERRE", "1,0");
    innerTestCreateRankSortExpression(creator, 20,
            "1,2,1,3,2", "REREE", "1");
    innerTestCreateRankSortExpression(creator, 40,
            "0,1,2,1,0,3,4,5,6,31,33,34,35", "REEEREEEEEEEE", "0");
    innerTestCreateRankSortExpression(creator, 40,
            "1,2,1,3,4,5,6,7,8,32,33", "REREEEEEEEE", "1");
    innerTestCreateRankSortExpression(creator, 20,
            "0,1,2#2,3,4", "RER#REE", "0,2");
    innerTestCreateRankSortExpression(creator, 20,
            "1,2#2,0,3", "RR#RRE", "0,1,2");    
    innerTestCreateRankSortExpression(creator, 20,
            "0,1,2#2#3,4", "RER#R#RE", "0,2,3");
    innerTestCreateRankSortExpression(creator, 20,
            "0,1,2,3,4#2#3,4", "RERRE#R#RE", "0,2,3");
    innerTestCreateRankSortExpression(creator, 40,
            "0,1,2,3,4#5,6,7,8,9,#10,11,7,12,33,38", 
            "REEEE#REEEE#REEEEE", "0,5,10");
}

void ComparatorCreatorTest::innerTestCreateSortComparator(
        bool allowLazyEvaluate, uint32_t count)
{
    ComparatorCreator creator(_pool, allowLazyEvaluate);
    innerTestCreateSortComparator(creator, allowLazyEvaluate, count);
}

void ComparatorCreatorTest::innerTestCreateSortComparator(
        ComparatorCreator &creator, bool allowLazyEvaluate, uint32_t count)
{
    vector<AttributeExpression*> origAttrExprs;
    SortExpressionVector origSortExprs;
    for (uint32_t i = 0; i < count; ++i) {
        FakeExpression *fakeExpr = new FakeExpression;
        if (i > 0) {
            uint32_t exprIdx = i <= 31 ? i : (uint32_t)-1;
            fakeExpr->setExpressionId(exprIdx);
        }
        origAttrExprs.push_back(fakeExpr);
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(origAttrExprs[i], false);
        origSortExprs.push_back(sortExpr);
    }
    ComboComparator *comboComp = creator.createSortComparator(origSortExprs);
    const ComboComparator::ComparatorVector &vec = comboComp->getComparators();
    if (comboComp->getType() == "combo") {
        ASSERT_EQ((size_t)count, vec.size());
        innerTestComparatorType(vec[0], ComparatorCreator::CT_REFERENCE);
        for (uint32_t i = 1; i < count; ++i) {
            ComparatorCreator::ComparatorType type =
                allowLazyEvaluate && i <= count
                ? ComparatorCreator::CT_EXPRESSION
                : ComparatorCreator::CT_REFERENCE;
            innerTestComparatorType(vec[i], type, false);
        }
    }
    if (count == 1) {
        ASSERT_EQ("one_ref", comboComp->getType());
    }
    const vector<AttributeExpression*> &immEvaluateExprs = creator.getImmEvaluateExpressions();
    const ExpressionSet &lazyEvaluateExprs = creator.getLazyEvaluateExpressions();
    size_t expectImmCount;
    if (allowLazyEvaluate) {
        expectImmCount = 1;
    } else {
        expectImmCount = count;
    }
    ASSERT_EQ(expectImmCount, immEvaluateExprs.size());
    
    size_t expectLazyCount = allowLazyEvaluate ? count - immEvaluateExprs.size() : 0;
    ASSERT_EQ(expectLazyCount, lazyEvaluateExprs.size());
    
    for (uint32_t i = 0; i < origAttrExprs.size(); ++i) {
        delete origAttrExprs[i];
    }
    POOL_DELETE_CLASS(comboComp);
}

void ComparatorCreatorTest::innerTestCreateRankSortExpression(
        ComparatorCreator &creator,
        uint32_t attrCount, const string &rankSortStr,
        const string &expectCompStr, const string &expectImmExprStr)
{
    vector<AttributeExpression*> origAttrExprs;
    SortExpressionVector origSortExprs;
    for (uint32_t i = 0; i < attrCount; ++i) {
        if (i == 0) {
            origAttrExprs.push_back(new FakeRankExpression("ref_" + StringUtil::toString(i)));
        } else {
            FakeExpression *fakeExpr = new FakeExpression();
            fakeExpr->setExpressionId(i <= 31 ? i : -1);
            origAttrExprs.push_back(fakeExpr);
        }
        SortExpression *sortExpr =
            _sortExprCreator->createSortExpression(origAttrExprs[i], false);
        origSortExprs.push_back(sortExpr);
    }

    // create rankSortExprs from string
    vector<SortExpressionVector> rankSortExprs;
    const vector<string> &rankSortStrs = StringUtil::split(rankSortStr, "#");
    for (uint32_t i = 0; i < rankSortStrs.size(); ++i) {
        SortExpressionVector rankSortExpr;
        const vector<string> &indexStrs = StringUtil::split(rankSortStrs[i], ",");
        for (uint32_t j = 0; j < indexStrs.size(); ++j) {
            uint32_t index = 0;
            ASSERT_TRUE(StringUtil::strToUInt32(indexStrs[j].c_str(), index));
            ASSERT_TRUE(index < attrCount);
            rankSortExpr.push_back(origSortExprs[index]);
        }
        rankSortExprs.push_back(rankSortExpr);
    }

    // check all comparators' type
    vector<ComboComparator *> comboComps = creator.createRankSortComparators(
            rankSortExprs);
    vector<string> expectComps = StringUtil::split(expectCompStr, "#");
    ASSERT_EQ(rankSortExprs.size(), comboComps.size());
    ASSERT_EQ(expectComps.size(), comboComps.size());
    for (uint32_t i = 0; i < expectComps.size(); ++i) {
        const ComboComparator::ComparatorVector comps =
            comboComps[i]->getComparators();
        if (comboComps[i]->getType() =="combo") {
            ASSERT_EQ(expectComps[i].size(), comps.size());
            for (uint32_t j = 0; j < expectComps[i].size(); ++j) {
                if (expectComps[i][j] == 'R') {
                    innerTestComparatorType(comps[j],
                            ComparatorCreator::CT_REFERENCE, false);
                } else {
                    innerTestComparatorType(comps[j],
                            ComparatorCreator::CT_EXPRESSION, false);
                }
            }
        } else if (comboComps[i]->getType() =="one_ref") {
            ASSERT_TRUE (expectComps[i][0] == 'R');
        } else if (comboComps[i]->getType() =="two_ref") {
            ASSERT_TRUE (expectComps[i][0] == 'R');
            ASSERT_TRUE (expectComps[i][1] == 'R');
        } else if (comboComps[i]->getType() =="ref_expr") {
            ASSERT_TRUE (expectComps[i][0] == 'R');
            ASSERT_TRUE (expectComps[i][1] == 'E');
        } else {
            ASSERT_FALSE(true);
        }
    }

    // check imm & lazy exprs
    const vector<AttributeExpression*> &immEvaluateExprs = creator.getImmEvaluateExpressions();
    const ExpressionSet &lazyEvaluateExprs = creator.getLazyEvaluateExpressions();
    const vector<string> &expectImmExprs = StringUtil::split(expectImmExprStr, ",");
    ASSERT_EQ(immEvaluateExprs.size(), expectImmExprs.size());
    
    for (uint32_t i = 0; i < expectImmExprs.size(); ++i) {
        uint32_t index = 0;
        ASSERT_TRUE(StringUtil::strToUInt32(expectImmExprs[i].c_str(),
                        index));
        ASSERT_TRUE(index < attrCount);
        ASSERT_TRUE(find(immEvaluateExprs.begin(), immEvaluateExprs.end(), origAttrExprs[index])
                       != immEvaluateExprs.end());
        ASSERT_TRUE(lazyEvaluateExprs.find(origAttrExprs[index])
                       == lazyEvaluateExprs.end());
    }

    for (uint32_t i = 0; i < origAttrExprs.size(); ++i) {
        delete origAttrExprs[i];
    }
    for (uint32_t i = 0; i < comboComps.size(); ++i) {
        POOL_DELETE_CLASS(comboComps[i]);
    }
}

void ComparatorCreatorTest::innerTestComparatorType(const Comparator *comp,
        ComparatorCreator::ComparatorType type, bool sortFlag)
{
    ASSERT_TRUE(comp);
    if (type == ComparatorCreator::CT_REFERENCE) {
        // we may have two type of ReferenceComparator, int32_t for fake
        // expression, score_t for fake rank expression
        const ReferenceComparator<int32_t> *refComp1 =
            dynamic_cast<const ReferenceComparator<int32_t> *>(comp);
        const ReferenceComparator<score_t> *refComp2 =
            dynamic_cast<const ReferenceComparator<score_t> *>(comp);
        assert(refComp1 || refComp2);
        ASSERT_TRUE(refComp1 || refComp2);
        if (refComp1) {
            ASSERT_TRUE(sortFlag == refComp1->getReverseFlag());
        }
        if (refComp2) {
            ASSERT_TRUE(sortFlag == refComp2->getReverseFlag());
        }
    } else {
        const ExpressionComparator<int32_t> *exprComp =
            dynamic_cast<const ExpressionComparator<int32_t> *>(comp);
        ASSERT_TRUE(exprComp);
        ASSERT_TRUE(sortFlag == exprComp->getReverseFlag());
    }
}

END_HA3_NAMESPACE(rank);

