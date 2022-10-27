#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/Comparator.h>
#include <ha3/rank/ComparatorCreator.h>

using namespace suez::turing;

BEGIN_HA3_NAMESPACE(search);

class AttributeExpressionTest : public TESTBASE {
public:
    AttributeExpressionTest();
    ~AttributeExpressionTest();
public:
    void setUp();
    void tearDown();

protected:
    matchdoc::MatchDoc createMatchDoc(docid_t docid, int32_t score);

protected:
    matchdoc::MatchDoc _a;
    matchdoc::MatchDoc _b;
    autil::mem_pool::Pool _pool;
    common::Ha3MatchDocAllocator *_allocator;
    matchdoc::Reference<int32_t> *_scoreRef;

protected:
    HA3_LOG_DECLARE();
};

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
HA3_LOG_SETUP(search, AttributeExpressionTest);


AttributeExpressionTest::AttributeExpressionTest() {    
    _allocator = new common::Ha3MatchDocAllocator(&_pool);
    _scoreRef = _allocator->declare<int32_t>("score");
    _a = createMatchDoc(0, 5);
    _b = createMatchDoc(1, 6);
}

AttributeExpressionTest::~AttributeExpressionTest() { 
    _allocator->deallocate(_a);
    _allocator->deallocate(_b);
    delete _allocator;
}

void AttributeExpressionTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AttributeExpressionTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AttributeExpressionTest, testBug42650) { 
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<int32_t> ae;
    ae.setReference(_scoreRef);    
    SortExpression sortExpression(&ae);
    sortExpression.setSortFlag(true);
    ASSERT_EQ(true, sortExpression.getSortFlag());

    autil::mem_pool::Pool pool(128);
    Comparator* c1 = ComparatorCreator::createComparator(&pool, &sortExpression);
    ASSERT_TRUE(!c1->compare(_a, _b));
    ASSERT_TRUE(c1->compare(_b, _a));

    sortExpression.setSortFlag(false);
    ASSERT_EQ(false, sortExpression.getSortFlag());
    Comparator* c2 = ComparatorCreator::createComparator(&pool, &sortExpression);
    ASSERT_TRUE(c2->compare(_a, _b));
    ASSERT_TRUE(!c2->compare(_b, _a));

    POOL_DELETE_CLASS(c1);
    POOL_DELETE_CLASS(c2);
}

matchdoc::MatchDoc AttributeExpressionTest::createMatchDoc(docid_t docid, 
        int32_t score)
{
    matchdoc::MatchDoc matchDoc = _allocator->allocate(docid);
    _scoreRef->set(matchDoc, score);
    return matchDoc;
}

END_HA3_NAMESPACE(search);

