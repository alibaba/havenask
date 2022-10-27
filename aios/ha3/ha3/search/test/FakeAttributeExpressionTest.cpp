#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <vector>
#include <ha3/search/test/FakeAttributeExpression.h>

using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);

class FakeAttributeExpressionTest : public TESTBASE {
public:
    FakeAttributeExpressionTest();
    ~FakeAttributeExpressionTest();
public:
    void setUp();
    void tearDown();
protected:
    common::Ha3MatchDocAllocator *_matchDocAllocator;
    std::vector<int32_t> _int32Values;
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, FakeAttributeExpressionTest);


FakeAttributeExpressionTest::FakeAttributeExpressionTest() { 
    _matchDocAllocator = NULL;
    _int32Values.push_back(0);
    _int32Values.push_back(10);
}

FakeAttributeExpressionTest::~FakeAttributeExpressionTest() { 
}

void FakeAttributeExpressionTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _matchDocAllocator =  new common::Ha3MatchDocAllocator(&_pool);
}

void FakeAttributeExpressionTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _matchDocAllocator;
    _matchDocAllocator = NULL;
}

TEST_F(FakeAttributeExpressionTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    FakeAttributeExpression<int32_t> expr("dummy", &_int32Values);

    matchdoc::MatchDoc matchDoc1 = _matchDocAllocator->allocate(1);
    ASSERT_TRUE(expr.evaluate(matchDoc1));
    ASSERT_EQ(10, expr.getValue(matchDoc1));
    
    matchdoc::MatchDoc matchDoc2 = _matchDocAllocator->allocate(2);    
    ASSERT_TRUE(expr.evaluate(matchDoc2));
    ASSERT_EQ(0, expr.getValue(matchDoc1));

    _matchDocAllocator->deallocate(matchDoc1);
    _matchDocAllocator->deallocate(matchDoc2);    
}

END_HA3_NAMESPACE(search);

