#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDocAllocator.h>
#include <ha3/search/test/FakeAttributeExpressionFactory.h>
#include <autil/StringTokenizer.h>

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);

class FakeAttributeExpressionFactoryTest : public TESTBASE {
public:
    FakeAttributeExpressionFactoryTest();
    ~FakeAttributeExpressionFactoryTest();
public:
    void setUp();
    void tearDown();
protected:
    MatchDocAllocator *_allocator;
    FakeAttributeExpressionFactory *_fakeAttrExprfactory;
    AttributeExpression *_expr;
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, FakeAttributeExpressionFactoryTest);


FakeAttributeExpressionFactoryTest::FakeAttributeExpressionFactoryTest() { 
    _allocator = NULL;
    _fakeAttrExprfactory = NULL;
    _expr = NULL;
    _pool = new autil::mem_pool::Pool(1024);
}

FakeAttributeExpressionFactoryTest::~FakeAttributeExpressionFactoryTest() { 
}

void FakeAttributeExpressionFactoryTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _allocator = new MatchDocAllocator;
    _fakeAttrExprfactory = new FakeAttributeExpressionFactory(_pool);
    _expr = NULL;
}

void FakeAttributeExpressionFactoryTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_allocator);
    DELETE_AND_SET_NULL(_fakeAttrExprfactory);
    POOL_DELETE_CLASS(_expr);
}

TEST_F(FakeAttributeExpressionFactoryTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    _fakeAttrExprfactory->setInt32Attribute("price", "15,0,35");

    _expr = _fakeAttrExprfactory->createAtomicExpr("price");
    ASSERT_TRUE(_expr);

    AttributeExpressionTyped<int32_t> *exprTyped
        = dynamic_cast<AttributeExpressionTyped<int32_t>* > (_expr);
    ASSERT_TRUE(exprTyped);

    MatchDoc *matchDoc = _allocator->allocateMatchDoc(0);
    exprTyped->evaluate(matchDoc);
    int32_t value = exprTyped->getValue();
    _allocator->deallocateMatchDoc(matchDoc);
    
    ASSERT_EQ(15, value);
}

END_HA3_NAMESPACE(search);

