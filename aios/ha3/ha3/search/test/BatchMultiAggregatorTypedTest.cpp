#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/search/test/MultiAggregatorTypedTest.h"
#include <ha3/search/BatchMultiAggregator.h>
#include <ha3/search/BinaryAttributeExpression.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3_sdk/testlib/search/FakeAttributeExpressionMaker.h>
#include <ha3_sdk/testlib/index/FakeMultiValueMaker.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(index);
// using namespace testing;

BEGIN_HA3_NAMESPACE(search);

typedef BatchAggregator<int64_t, int64_t,
                        DenseMapTraits<int64_t>::GroupMapType> BatchAggregatorType;

class BatchMultiAggregatorTypedTest : public MultiAggregatorTypedTest<BatchAggregatorType>,
                                      public ::testing::WithParamInterface<bool>
{
public:
    void setUp();
    void tearDown();
public:
    MultiAggregator* create() override {
        if (_isBatch) {
            return new BatchMultiAggregator(_pool);
        } else {
            return new MultiAggregator(_pool);
        }
    }
private:
    bool _isBatch;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, BatchMultiAggregatorTypedTest);

void BatchMultiAggregatorTypedTest::setUp() {
    baseSetUp();
    _isBatch = GetParam();
}

void BatchMultiAggregatorTypedTest::tearDown() {
    baseTearDown();
}

TEST_P(BatchMultiAggregatorTypedTest, testSimple) {
    HA3_LOG(DEBUG, "Begin Test!");

    _multiAgg = create();
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.attributes = "key: int64_t: 0,1,1;"
                           "key2: int64_t: 0,1,1;"
                           "sub_key: int64_t: 0,1,1,2,3,2,3;"
                           "sub_id: int64_t : 0,1,1,2,1,2,3;"
                           "sub_price: int64_t: 1,1,3,1,2,1,3;"
                           "price: int64_t: 1,2,3";
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    AttributeExpressionCreator *attrExprCreator = new FakeAttributeExpressionCreator(
            _pool, indexPartReaderPtr, NULL, NULL, NULL, NULL, _mdAllocator->getSubDocRef());
    AttributeExpression *keyExpr = attrExprCreator->createAtomicExpr("key");
    CountAggregateFunction *countFun = POOL_NEW_CLASS(_pool, CountAggregateFunction);
    BatchAggregatorType *agg1 = new BatchAggregatorType(
            (AttributeExpressionTyped<int64_t> *)keyExpr, 100, _pool);
    agg1->addAggregateFunction(countFun);
    _multiAgg->addAggregator(agg1);

    keyExpr = attrExprCreator->createAtomicExpr("key2");
    AttributeExpression *funExpr = attrExprCreator->createAtomicExpr("price");
    AggregateFunction *sumFun = POOL_NEW_CLASS(_pool, SumAggregateFunction<int64_t>,
            "price", vt_int64, (AttributeExpressionTyped<int64_t> *)funExpr);
    BatchAggregatorType *agg2 = new BatchAggregatorType(
            (AttributeExpressionTyped<int64_t> *)keyExpr, 100, _pool);
    agg2->addAggregateFunction(sumFun);
    _multiAgg->addAggregator(agg2);    
    
    AttributeExpression *subKeyExpr = attrExprCreator->createAtomicExpr("sub_key");
    funExpr = attrExprCreator->createAtomicExpr("sub_id");
    AggregateFunction *maxFun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int64_t>,
            "sub_id", vt_int64, (AttributeExpressionTyped<int64_t> *)funExpr);

    // create agg filter: sub_price > 1
    AttributeExpression *subPriceExpr = attrExprCreator->createAtomicExpr("sub_price");
    ConstAttributeExpression<int64_t> *constAttrExpr = new ConstAttributeExpression<int64_t>(1);
    BinaryAttributeExpression<std::greater, int64_t> *filterExpr =
        new BinaryAttributeExpression<std::greater, int64_t>(subPriceExpr, constAttrExpr);
    filterExpr->setIsSubExpression(true);
    Filter *aggFilter = POOL_NEW_CLASS(_pool, Filter, filterExpr);
    BatchAggregatorType *agg3 = new BatchAggregatorType(
            (AttributeExpressionTyped<int64_t> *)subKeyExpr, 100, _pool);
    agg3->addAggregateFunction(maxFun);
    agg3->setFilter(aggFilter);
    _multiAgg->addAggregator(agg3);

    aggSubMatchDoc("3, 2, 2", _multiAgg);

    AggregateResultsPtr aggResultsPtr = _multiAgg->collectAggregateResult();
    ASSERT_EQ((size_t)3, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    ASSERT_TRUE(aggResultPtr);
    aggResultPtr->constructGroupValueMap();

    // group_key:key, agg_fun:count()
    ASSERT_EQ((uint64_t)2, aggResultPtr->getAggExprValueCount());
    ASSERT_EQ((int64_t)1, aggResultPtr->getAggFunResult<int64_t>("0", 0, 0));
    ASSERT_EQ((int64_t)2, aggResultPtr->getAggFunResult<int64_t>("1", 0, 0));
    ASSERT_EQ(string("count"), aggResultPtr->getAggFunName(0));

    // group_key:key, agg_fun:sum(price)
    aggResultPtr = (*aggResultsPtr)[1];
    aggResultPtr->constructGroupValueMap();
    ASSERT_EQ((int64_t)1, aggResultPtr->getAggFunResult<int64_t>("0", 0, 0));
    ASSERT_EQ((int64_t)5, aggResultPtr->getAggFunResult<int64_t>("1", 0, 0));
    ASSERT_EQ(string("sum"), aggResultPtr->getAggFunName(0));

    // group_key:sub_key, agg_fun:max(sub_id), agg_filter:sub_price>1
    aggResultPtr = (*aggResultsPtr)[2];
    aggResultPtr->constructGroupValueMap();
    ASSERT_EQ((int64_t)1, aggResultPtr->getAggFunResult<int64_t>("1", 0, 0));
    ASSERT_EQ((int64_t)3, aggResultPtr->getAggFunResult<int64_t>("3", 0, 0));
    ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));

    DELETE_AND_SET_NULL(attrExprCreator);
    DELETE_AND_SET_NULL(constAttrExpr);
    DELETE_AND_SET_NULL(filterExpr);
    DELETE_AND_SET_NULL(_multiAgg);
}

// use macro INSTANTIATE_TEST_SUITE_P when gtest(version) > 1.8.0
INSTANTIATE_TEST_CASE_P(prefix, BatchMultiAggregatorTypedTest, testing::Bool());

END_HA3_NAMESPACE();

