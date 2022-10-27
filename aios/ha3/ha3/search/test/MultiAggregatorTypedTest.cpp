#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/search/test/AggregateBase.h"
#include "ha3/search/MultiAggregator.h"
#include "ha3/search/test/MultiAggregatorTypedTest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(search);

typedef Types<BatchAggregator<int64_t, int64_t, DenseMapTraits<int64_t>::GroupMapType>,
              BatchAggregator<int64_t, int64_t, UnorderedMapTraits<int64_t>::GroupMapType>,
              NormalAggregator<int64_t, int64_t, DenseMapTraits<int64_t>::GroupMapType>,
              NormalAggregator<int64_t, int64_t, UnorderedMapTraits<int64_t>::GroupMapType>
              > Implementations;
// use macro TYPED_TEST_SUITE when gtest(version) > 1.8.0
TYPED_TEST_CASE(MultiAggregatorTypedTest, Implementations);
//TYPED_TEST_CASE(BatchMultiAggregatorTypedTest, Implementations);

TYPED_TEST(MultiAggregatorTypedTest, testMultiAggregator) {
    
    const auto *agg1 = this->_agg[0];
    ASSERT_TRUE(agg1);

    const auto *agg2 = this->_agg[1];
    ASSERT_TRUE(agg2);

    for (int i = 0; i < this->DOC_NUM; i++) {
        matchdoc::MatchDoc matchDoc = this->_mdAllocator->allocate((docid_t)i);
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
        this->_multiAgg->aggregate(matchDoc);
    }
    this->_multiAgg->endLayer(1);
    this->_multiAgg->estimateResult(1);
    int64_t value = -1;
    ASSERT_TRUE(agg1->template getFunResultOfKey<int64_t>("max(price)", 0, value));
    ASSERT_EQ((int64_t)8, value);

    double value2 = 22.2;
    ASSERT_TRUE(agg2->template getFunResultOfKey<double>("min(price)", 1, value2));
    ASSERT_DOUBLE_EQ(1.0, value2);


    AggregateResultsPtr aggResultsPtr = this->_multiAgg->collectAggregateResult();
    ASSERT_EQ((size_t)4, aggResultsPtr->size());

    //the first AggretateResult
    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    aggResultPtr->constructGroupValueMap();
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)1, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    int64_t aggFunResult = 0;
    ASSERT_EQ((int64_t)8,
                         aggResultPtr->getAggFunResult<int64_t>("0", 0, aggFunResult));
    ASSERT_EQ((int64_t)9,
                         aggResultPtr->getAggFunResult<int64_t>("1", 0, aggFunResult));
    ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));

    //the second AggretateResult
    double aggFunResult2 = 0.0f;
    aggResultPtr = (*aggResultsPtr)[1];
    aggResultPtr->constructGroupValueMap();
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)1, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    ASSERT_EQ((double)0,
                         aggResultPtr->getAggFunResult<double>("0", 0, aggFunResult2));

    ASSERT_DOUBLE_EQ((double)1.0,
                     aggResultPtr->getAggFunResult<double>("1", 0, aggFunResult2));

    ASSERT_EQ(string("min"), aggResultPtr->getAggFunName(0));

}


TYPED_TEST(MultiAggregatorTypedTest, testEstimateResult) {

    for (int i = 0; i < this->DOC_NUM; i++) {
        matchdoc::MatchDoc matchDoc = this->_mdAllocator->allocate((docid_t)i);
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
        this->_multiAgg->aggregate(matchDoc);
    }
    this->_multiAgg->endLayer(1.0f);
    double factor = 2.5;
    this->_multiAgg->estimateResult(factor);
    ASSERT_EQ(uint32_t(10), this->_multiAgg->getAggregateCount());

    AggregateResultsPtr aggResultsPtr = this->_multiAgg->collectAggregateResult();
    ASSERT_EQ((size_t)4, aggResultsPtr->size());

    //the first AggretateResult
    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    aggResultPtr->constructGroupValueMap();
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)1, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    int64_t aggFunResult = 0;
    ASSERT_EQ((int64_t)8,
              aggResultPtr->getAggFunResult<int64_t>("0", 0, aggFunResult));
    ASSERT_EQ((int64_t)9,
              aggResultPtr->getAggFunResult<int64_t>("1", 0, aggFunResult));
    ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));

    //the second AggretateResult
    double aggFunResult2 = 0.0f;
    aggResultPtr = (*aggResultsPtr)[1];
    aggResultPtr->constructGroupValueMap();
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)1, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    ASSERT_EQ((double)0,
              aggResultPtr->getAggFunResult<double>("0", 0, aggFunResult2));

    ASSERT_DOUBLE_EQ((double)1.0,
                     aggResultPtr->getAggFunResult<double>("1", 0, aggFunResult2));

    ASSERT_EQ(string("min"), aggResultPtr->getAggFunName(0));

    //the third AggretateResult
    double aggFunResult3 = 0.0f;
    aggResultPtr = (*aggResultsPtr)[2];
    aggResultPtr->constructGroupValueMap();
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)1, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    ASSERT_EQ((double)50,
              aggResultPtr->getAggFunResult<double>("0", 0, aggFunResult3));

    ASSERT_DOUBLE_EQ((double)62.5,
                     aggResultPtr->getAggFunResult<double>("1", 0, aggFunResult3));

    ASSERT_EQ(string("sum"), aggResultPtr->getAggFunName(0));

    //the forth AggretateResult
    int64_t aggFunResult4 = 0;
    aggResultPtr = (*aggResultsPtr)[3];
    aggResultPtr->constructGroupValueMap();
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)1, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    ASSERT_EQ((int64_t)12,
              aggResultPtr->getAggFunResult<int64_t>("0", 0, aggFunResult4));

    ASSERT_EQ((int64_t)12,
              aggResultPtr->getAggFunResult<int64_t>("1", 0, aggFunResult4));

    ASSERT_EQ(string("count"), aggResultPtr->getAggFunName(0));

}

END_HA3_NAMESPACE();

