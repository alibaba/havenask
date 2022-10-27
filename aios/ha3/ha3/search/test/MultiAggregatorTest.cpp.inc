#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <unittest/unittest.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/search/AggregateFunction.h>
#include <matchdoc/Reference.h>
#include <ha3/search/AttributeExpression.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/MultiAggregator.h>
#include <ha3/search/BatchMultiAggregator.h>
#include <ha3/search/AggregateFunction.h>
#include <ha3/search/MaxAggregateFunction.h>
#include <ha3/search/MinAggregateFunction.h>
#include <ha3/search/SumAggregateFunction.h>
#include <ha3/search/CountAggregateFunction.h>
#include <ha3/search/AvgAggregateFunction.h>
#include <ha3/search/CountAggregateFunction.h>
#include <ha3/search/NormalAggregator.h>
#include <ha3/search/BatchAggregator.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <ha3_sdk/testlib/search/FakeAttributeExpressionMaker.h>
#include <string>
#include <ha3/rank/MatchData.h>
#include <autil/StringUtil.h>
#include <memory>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(search);

class AggregateFunction;

class TEST_CLASS_NAME : public TESTBASE {
public:
    TEST_CLASS_NAME();
    ~TEST_CLASS_NAME();

public:
    void setUp();
    void tearDown();
private:
    template <typename T>
    void makeFakeData_fun(std::vector<T> &fakeData, int count);
    template <typename T>
    void makeFakeData_key(std::vector<T> &fakeData, int count);

    MultiAggregator* createMultiAggregator();
private:
    std::vector<matchdoc::MatchDoc> _mdVector;
    common::Ha3MatchDocAllocator *_mdAllocator;
    const int DOC_NUM;
    autil::mem_pool::Pool *_pool;
    std::shared_ptr<FakeAttributeExpressionMaker> _fakeAttrExprMaker;
    std::vector<AttributeExpression *> _exprs;
protected:
    enum TestMultiMode { MULTI_NORMAL, MULTI_BATCH};
    TestMultiMode _testMode;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, TEST_CLASS_NAME);


TEST_CLASS_NAME::TEST_CLASS_NAME() : DOC_NUM(10){
    _mdAllocator = NULL;
    _testMode = TEST_MODE;
    //_testMode = MULTI_NORMAL;
    //_testMode = MULTI_BATCH;
    _pool = new autil::mem_pool::Pool(1024);
}

TEST_CLASS_NAME::~TEST_CLASS_NAME() {
    delete _pool;
}

void TEST_CLASS_NAME::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _mdAllocator = new common::Ha3MatchDocAllocator(_pool);
    _fakeAttrExprMaker.reset(new FakeAttributeExpressionMaker(_pool));
}

void TEST_CLASS_NAME::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");

    for (size_t i = 0; i < _exprs.size(); ++i) {
        POOL_DELETE_CLASS(_exprs[i]);
    }
    _exprs.clear();

    _fakeAttrExprMaker.reset();

    for (vector<matchdoc::MatchDoc>::iterator it = _mdVector.begin();
         it != _mdVector.end(); it++)
    {
        _mdAllocator->deallocate(*it);
    }
    _mdVector.clear();
    delete _mdAllocator;
}

MultiAggregator* TEST_CLASS_NAME::createMultiAggregator() {
    DefGroupType(int64_t, GroupType);
    typedef BatchAggregator<int64_t, int64_t, GroupType> BatchAggregatorType;
    typedef NormalAggregator<int64_t, int64_t, GroupType> NormalAggregatorType;

    //construct the first aggregator
    vector<int64_t> fakeData_key;
    makeFakeData_key(fakeData_key, DOC_NUM);
    AttributeExpressionTyped<int64_t> *keyExpr1
        = _fakeAttrExprMaker->createAttrExpr<int64_t>("uid", StringUtil::toString(fakeData_key, ","));
    NormalAggregatorType *agg1 = NULL;
    if (_testMode == MULTI_NORMAL) {
        agg1 = new NormalAggregatorType(keyExpr1, 100, _pool);
    } else {
        agg1 = new BatchAggregatorType(keyExpr1, 100, _pool);
    }

    //construct the aggregate function of first aggregator
    vector<int64_t> fakeData_fun1;
    makeFakeData_fun(fakeData_fun1, DOC_NUM);
    AttributeExpressionTyped<int64_t> *expr1
        = _fakeAttrExprMaker->createAttrExpr<int64_t>("uid1", StringUtil::toString(fakeData_fun1, ","));
    MaxAggregateFunction<int64_t> *fun1
        = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int64_t>, "price", vt_int64, expr1);

    _exprs.push_back(keyExpr1);
    _exprs.push_back(expr1);
    agg1->addAggregateFunction(fun1);

    //construct the second aggregator
    vector<int64_t> fakeData_key2;
    makeFakeData_key(fakeData_key2, DOC_NUM);
    AttributeExpressionTyped<int64_t> *keyExpr2
        = _fakeAttrExprMaker->createAttrExpr<int64_t>("type", StringUtil::toString(fakeData_key2, ","));

    NormalAggregatorType *agg2 = NULL;
    if (_testMode == MULTI_NORMAL) {
        agg2 = new NormalAggregatorType(keyExpr2, 100, _pool);
    } else {
        agg2 = new BatchAggregatorType(keyExpr2, 100, _pool);
    }

    //construct the aggregate function of second aggregator
    vector<double> fakeData_fun2;
    makeFakeData_fun(fakeData_fun2, DOC_NUM);
    AttributeExpressionTyped<double> *expr2
        = _fakeAttrExprMaker->createAttrExpr<double>("uid2", StringUtil::toString(fakeData_fun2, ","));

    MinAggregateFunction<double> *fun2
        = POOL_NEW_CLASS(_pool, MinAggregateFunction<double>, "price", vt_double, expr2);

    _exprs.push_back(keyExpr2);
    _exprs.push_back(expr2);
    agg2->addAggregateFunction(fun2);

 //construct the third aggregator
    vector<int64_t> fakeData_key3;
    makeFakeData_key(fakeData_key3, DOC_NUM);
    AttributeExpressionTyped<int64_t> *keyExpr3
        = _fakeAttrExprMaker->createAttrExpr<int64_t>("type", StringUtil::toString(fakeData_key3, ","));

    NormalAggregatorType *agg3 = NULL;
    if (_testMode == MULTI_NORMAL) {
        agg3 = new NormalAggregatorType(keyExpr3, 100, _pool);
    } else {
        agg3 = new BatchAggregatorType(keyExpr3, 100, _pool);
    }

    //construct the aggregate function of third aggregator
    vector<double> fakeData_fun3;
    makeFakeData_fun(fakeData_fun3, DOC_NUM);
    AttributeExpressionTyped<double> *expr3
        = _fakeAttrExprMaker->createAttrExpr<double>("uid3", StringUtil::toString(fakeData_fun3, ","));

    SumAggregateFunction<double> *fun3
        = POOL_NEW_CLASS(_pool, SumAggregateFunction<double>, "price", vt_double, expr3);

    _exprs.push_back(keyExpr3);
    _exprs.push_back(expr3);
    agg3->addAggregateFunction(fun3);

 //construct the forth aggregator
    vector<int64_t> fakeData_key4;
    makeFakeData_key(fakeData_key4, DOC_NUM);
    AttributeExpressionTyped<int64_t> *keyExpr4
        = _fakeAttrExprMaker->createAttrExpr<int64_t>("type", StringUtil::toString(fakeData_key4, ","));

    NormalAggregatorType *agg4 = NULL;
    if (_testMode == MULTI_NORMAL) {
        agg4 = new NormalAggregatorType(keyExpr4, 100, _pool);
    } else {
        agg4 = new BatchAggregatorType(keyExpr4, 100, _pool);
    }

    //construct the aggregate function of forth aggregator
       CountAggregateFunction *fun4
           = POOL_NEW_CLASS(_pool, CountAggregateFunction);

    _exprs.push_back(keyExpr4);
    agg4->addAggregateFunction(fun4);

    //construct MultiAggregator

    MultiAggregator *multiAgg = NULL;
    if (_testMode == MULTI_NORMAL) {
        multiAgg = new MultiAggregator(_pool);
    } else {
        multiAgg = new BatchMultiAggregator(_pool);
    }
    multiAgg->addAggregator(agg1);
    multiAgg->addAggregator(agg2);
    multiAgg->addAggregator(agg3);
    multiAgg->addAggregator(agg4);
    multiAgg->setMatchDocAllocator(_mdAllocator);

    return multiAgg;
}

TEST_F(TEST_CLASS_NAME, testMultiAggregator) {
    DefGroupType(int64_t, GroupType);
    typedef NormalAggregator<int64_t, int64_t, GroupType> NormalAggregatorType;

    HA3_LOG(DEBUG, "Begin Test!");
    MultiAggregator *multiAgg = createMultiAggregator();

    const NormalAggregatorType *agg1
      = multiAgg->getNormalAggregator<int64_t, int64_t, GroupType>(0);
    ASSERT_TRUE(agg1);

    const NormalAggregatorType *agg2
      = multiAgg->getNormalAggregator<int64_t, int64_t, GroupType>(1);
    ASSERT_TRUE(agg2);

    for (int i = 0; i < DOC_NUM; i++) {
        matchdoc::MatchDoc matchDoc = _mdAllocator->allocate((docid_t)i);
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
        _mdVector.push_back(matchDoc);
        multiAgg->aggregate(matchDoc);
    }
    multiAgg->endLayer(1);
    multiAgg->estimateResult(1);
    int64_t value = -1;
    ASSERT_TRUE(agg1->getFunResultOfKey<int64_t>("max(price)", 0, value));
    ASSERT_EQ((int64_t)8, value);

    double value2 = 22.2;
    ASSERT_TRUE(agg2->getFunResultOfKey<double>("min(price)", 1, value2));
    ASSERT_DOUBLE_EQ(1.0, value2);
    delete multiAgg;
}

TEST_F(TEST_CLASS_NAME, testCollectResult) {
    HA3_LOG(DEBUG, "Begin Test!");
    MultiAggregator *multiAgg = createMultiAggregator();
    for (int i = 0; i < DOC_NUM; i++) {
        matchdoc::MatchDoc matchDoc = _mdAllocator->allocate((docid_t)i);
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
        _mdVector.push_back(matchDoc);
        multiAgg->aggregate(matchDoc);
    }
    multiAgg->endLayer(1);
    multiAgg->estimateResult(1);
    AggregateResultsPtr aggResultsPtr = multiAgg->collectAggregateResult();
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
    delete multiAgg;
}
TEST_F(TEST_CLASS_NAME, testEstimateResult){
    HA3_LOG(DEBUG, "Begin Test!");
    double factor = 2.5;
    MultiAggregator *multiAgg = createMultiAggregator();
    for (int i = 0; i < DOC_NUM; i++) {
        matchdoc::MatchDoc matchDoc = _mdAllocator->allocate((docid_t)i);
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
        _mdVector.push_back(matchDoc);
        multiAgg->aggregate(matchDoc);
    }
    multiAgg->endLayer(1.0f);

    multiAgg->estimateResult(factor);
    ASSERT_EQ(uint32_t(10), multiAgg->getAggregateCount());

    AggregateResultsPtr aggResultsPtr = multiAgg->collectAggregateResult();
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
    delete multiAgg;
}

template <typename T>
void TEST_CLASS_NAME::makeFakeData_fun(std::vector<T> &fakeData, int count)
{
    for (int i = 0; i < count; i++) {
        fakeData.push_back((T)i);
    }
}

template <typename T>
void TEST_CLASS_NAME::makeFakeData_key(std::vector<T> &fakeData, int count)
{
    for (int i = 0; i < count; i++) {
        fakeData.push_back((T)(i % 2));
    }
}

END_HA3_NAMESPACE(search);
