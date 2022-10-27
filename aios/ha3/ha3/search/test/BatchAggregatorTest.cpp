#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/search/test/AggregateBase.h"
#include "ha3/search/NormalAggregator.h"
#include "ha3/search/BatchAggregator.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(search);

template <typename T>
class BatchAggregatorTest : public TESTBASE, public AggregateBase {
public:
    BatchAggregatorTest() {
    }
    ~BatchAggregatorTest() {
    }
public:
    void setUp() {
        baseSetUp();
        _keyAttributeExpr = initKeyExpression<int32_t>("userid");
        _agg = new T((AttributeExpressionTyped<int32_t>*)_keyAttributeExpr,
                     1, _pool, 5, 5);

    }
    void tearDown() {
//        DELETE_AND_SET_NULL(_keyAttributeExpr);
        DELETE_AND_SET_NULL(_agg);
        baseTearDown();
    }

private:
    T *_agg;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE(search, BatchAggregatorTest, T);

typedef Types<BatchAggregator<int, int, DenseMapTraits<int>::GroupMapType>,
              BatchAggregator<int, int, UnorderedMapTraits<int>::GroupMapType>
              > Implementations;
// use macro TYPED_TEST_SUITE when gtest(version) > 1.8.0
TYPED_TEST_CASE(BatchAggregatorTest, Implementations);

TYPED_TEST(BatchAggregatorTest, testDefaultAgg) {

    AttributeExpressionTyped<int32_t> *expr1 =
        this->template initFunExpression<int32_t>("sum");
    SumAggregateFunction<int32_t> *afun1
        = POOL_NEW_CLASS(this->_pool, SumAggregateFunction<int32_t>,
                         "price", vt_int32, expr1);
    CountAggregateFunction *afun2 =
        POOL_NEW_CLASS(this->_pool, CountAggregateFunction);
    auto agg = this->_agg;
    agg->addAggregateFunction(afun1);
    agg->addAggregateFunction(afun2);
    this->aggMatchDoc(this->DOC_NUM, agg);

    matchdoc::MatchDoc resultSlab;
    const matchdoc::Reference<int32_t> *funResultRef1
        = agg->template getFunResultReference<int32_t>("sum(price)");
    ASSERT_TRUE(funResultRef1);
    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ((int32_t)40, *(funResultRef1->getPointer(resultSlab)));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);
    // ASSERT_EQ((int32_t)25, *(funResultRef1->getPointer(resultSlab)));

    const matchdoc::Reference<int64_t> *funResultRef2
        = agg->template getFunResultReference<int64_t>("count()");
    ASSERT_TRUE(funResultRef2);
    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ((int64_t)10, *(funResultRef2->getPointer(resultSlab)));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);
}


TYPED_TEST(BatchAggregatorTest, testUpdateSampler) {

    BatchAggregateSampler sampler(22, 21);
    this->_agg->updateSampler(sampler);
    ASSERT_EQ(5u, sampler.getAggThreshold());
    ASSERT_EQ(5u, sampler.getSampleMaxCount());
}

END_HA3_NAMESPACE();
