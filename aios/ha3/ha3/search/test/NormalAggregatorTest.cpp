#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/search/AggregateFunction.h>
#include <matchdoc/Reference.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/search/NormalAggregator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/framework/AttributeExpressionPool.h>
#include <autil/MultiValueType.h>
#include <matchdoc/Reference.h>
#include <ha3/search/AggregateFunction.h>
#include <ha3/search/SumAggregateFunction.h>
#include <ha3/search/MaxAggregateFunction.h>
#include <ha3/search/MinAggregateFunction.h>
#include <ha3/search/AvgAggregateFunction.h>
#include <ha3/search/CountAggregateFunction.h>
#include <ha3/search/DistinctCountAggregateFunction.h>
#include <ha3/search/BinaryAttributeExpression.h>
#include <ha3/search/NormalAggregator.h>
#include <ha3/search/BatchAggregator.h>
#include <ha3/search/test/NormalAggregatorTest.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3_sdk/testlib/index/FakeMultiValueMaker.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(search);

HA3_LOG_SETUP(search, NormalAggregatorTest);


NormalAggregatorTest::NormalAggregatorTest() {
    _mode = GetParam();
}

NormalAggregatorTest::~NormalAggregatorTest() {
}

void NormalAggregatorTest::SetUp() {
    HA3_LOG(DEBUG, "setUp!");
    baseSetUp();
}

void NormalAggregatorTest::TearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    resetExpression();
    _mdVector.clear();
    baseTearDown();
}

TEST_P(NormalAggregatorTest, testMultiCharAggregator) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiChar> *keyExpr
        = _fakeAttrExprMaker->createStringAttrExpr("key", "string1,string2,string1");

    _attrExprPool->addNeedDeleteExpr(keyExpr);
    AttributeExpressionTyped<int32_t> *funExpr
        = _fakeAttrExprMaker->createAttrExpr<int32_t>("price", "3, 2, 1");
    _attrExprPool->addNeedDeleteExpr(funExpr);

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>,
            "price",vt_int32, funExpr);

    ASSERT_TRUE(fun);

    NormalAggregator<MultiChar>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<MultiChar>, keyExpr, 100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<MultiChar>, keyExpr, 100, _pool);
    }
    agg->addAggregateFunction(fun);

    aggMatchDoc(3, agg);

    const matchdoc::Reference<int32_t> *funResultRef
        = agg->getFunResultReference<int32_t>("max(price)");
    ASSERT_TRUE(funResultRef);

    ASSERT_EQ(2u, agg->getKeyCount());

    MultiCharMaker multiCharMaker;
    matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string1"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((int32_t)3, *(funResultRef->getPointer(resultSlab)));
    resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string2"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((int32_t)2, *(funResultRef->getPointer(resultSlab)));
    resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string100"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);

    int32_t value;
    ASSERT_TRUE(agg->getFunResultOfKey<int32_t>("max(price)", multiCharMaker.makeMultiChar("string1"), value));
    ASSERT_EQ((int32_t)3, value);

    ASSERT_TRUE(agg->getFunResultOfKey<int32_t>("max(price)", multiCharMaker.makeMultiChar("string2"), value));
    ASSERT_EQ((int32_t)2, value);

    ASSERT_TRUE(!agg->getFunResultOfKey<int32_t>("max(price)", multiCharMaker.makeMultiChar("string1111"), value));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testMultiCharAggregatorOfDistinctCount) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiChar> *keyExpr
        = _fakeAttrExprMaker->createStringAttrExpr("key", "string1,string2,string1");

    _attrExprPool->addNeedDeleteExpr(keyExpr);
    AttributeExpressionTyped<int32_t> *funExpr
        = _fakeAttrExprMaker->createAttrExpr<int32_t>("price", "3, 2, 1");
    _attrExprPool->addNeedDeleteExpr(funExpr);

    AggregateFunction *fun = POOL_NEW_CLASS(
        _pool, DistinctCountAggregateFunction<int32_t>, "price", funExpr);

    ASSERT_TRUE(fun);

    NormalAggregator<MultiChar>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<MultiChar>, keyExpr, 100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<MultiChar>, keyExpr, 100, _pool);
    }
    agg->addAggregateFunction(fun);

    aggMatchDoc(3, agg);

    const matchdoc::Reference<autil::HllCtx> *funResultRef
        = agg->getFunResultReference<autil::HllCtx>("distinct_count(price)");
    ASSERT_TRUE(funResultRef);

    ASSERT_EQ(2u, agg->getKeyCount());

    MultiCharMaker multiCharMaker;
    matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string1"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ("2", autil::StringUtil::toString(
                       *(funResultRef->getPointer(resultSlab))));
    resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string2"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ("1", autil::StringUtil::toString(
                       *(funResultRef->getPointer(resultSlab))));
    resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string100"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);

    autil::HllCtx value;
    ASSERT_TRUE(agg->getFunResultOfKey<autil::HllCtx>("distinct_count(price)", multiCharMaker.makeMultiChar("string1"), value));
    ASSERT_EQ("2", autil::StringUtil::toString(value));

    ASSERT_TRUE(agg->getFunResultOfKey<autil::HllCtx>("distinct_count(price)", multiCharMaker.makeMultiChar("string2"), value));
    ASSERT_EQ("1", autil::StringUtil::toString(value));

    ASSERT_TRUE(!agg->getFunResultOfKey<autil::HllCtx>("distinct_count(price)", multiCharMaker.makeMultiChar("string1111"), value));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testMultiStringAggregator) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiString> *keyExpr
        = _fakeAttrExprMaker->createMultiValueAttrExpr<MultiChar>
        ("key", "string1,string2#string1,string3#string2,string3");
    _attrExprPool->addNeedDeleteExpr(keyExpr);
    AttributeExpressionTyped<int32_t> *funExpr
        = _fakeAttrExprMaker->createAttrExpr<int32_t>("price", "3, 2, 1");
    _attrExprPool->addNeedDeleteExpr(funExpr);

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, SumAggregateFunction<int32_t>,
            "price",vt_int32, funExpr);

    ASSERT_TRUE(fun);

    NormalAggregator<MultiChar, MultiString>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        typedef NormalAggregator<MultiChar, MultiString> AggregatorType;
        agg = POOL_NEW_CLASS(_pool, AggregatorType, keyExpr, 100, _pool);
    } else {
        typedef BatchAggregator<MultiChar, MultiString> AggregatorType;
        agg = POOL_NEW_CLASS(_pool, AggregatorType, keyExpr, 100, _pool);
    }
    agg->addAggregateFunction(fun);

    aggMatchDoc(3, agg);

    const matchdoc::Reference<int32_t> *funResultRef
        = agg->getFunResultReference<int32_t>("sum(price)");
    ASSERT_TRUE(funResultRef);

    ASSERT_EQ(3u, agg->getKeyCount());

    MultiCharMaker multiCharMaker;
    matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string1"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((int32_t)5, *(funResultRef->getPointer(resultSlab)));
    resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string2"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((int32_t)4, *(funResultRef->getPointer(resultSlab)));
    resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string100"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);

    int32_t value;
    ASSERT_TRUE(agg->getFunResultOfKey<int32_t>("sum(price)", multiCharMaker.makeMultiChar("string1"), value));
    ASSERT_EQ((int32_t)5, value);

    ASSERT_TRUE(agg->getFunResultOfKey<int32_t>("sum(price)", multiCharMaker.makeMultiChar("string2"), value));
    ASSERT_EQ((int32_t)4, value);

    ASSERT_TRUE(!agg->getFunResultOfKey<int32_t>("sum(price)", multiCharMaker.makeMultiChar("string1111"), value));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testMultiStringAggregatorOfDistinctCount) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiString> *keyExpr
        = _fakeAttrExprMaker->createMultiValueAttrExpr<MultiChar>
        ("key", "string1,string2#string1,string3#string2,string3");
    _attrExprPool->addNeedDeleteExpr(keyExpr);
    AttributeExpressionTyped<int32_t> *funExpr
        = _fakeAttrExprMaker->createAttrExpr<int32_t>("price", "3, 2, 1");
    _attrExprPool->addNeedDeleteExpr(funExpr);

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, DistinctCountAggregateFunction<int32_t>,
            "price", funExpr);

    ASSERT_TRUE(fun);

    NormalAggregator<MultiChar, MultiString>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        typedef NormalAggregator<MultiChar, MultiString> AggregatorType;
        agg = POOL_NEW_CLASS(_pool, AggregatorType, keyExpr, 100, _pool);
    } else {
        typedef BatchAggregator<MultiChar, MultiString> AggregatorType;
        agg = POOL_NEW_CLASS(_pool, AggregatorType, keyExpr, 100, _pool);
    }
    agg->addAggregateFunction(fun);

    aggMatchDoc(3, agg);

    const matchdoc::Reference<autil::HllCtx> *funResultRef
        = agg->getFunResultReference<autil::HllCtx>("distinct_count(price)");
    ASSERT_TRUE(funResultRef);

    ASSERT_EQ(3u, agg->getKeyCount());

    MultiCharMaker multiCharMaker;
    matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string1"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ("2", autil::StringUtil::toString(
                       *(funResultRef->getPointer(resultSlab))));
    resultSlab =
        agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string2"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ("2", autil::StringUtil::toString(
                       *(funResultRef->getPointer(resultSlab))));
    resultSlab = agg->getResultMatchDoc(multiCharMaker.makeMultiChar("string100"));
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);

    autil::HllCtx value;
    ASSERT_TRUE(agg->getFunResultOfKey<autil::HllCtx>(
        "distinct_count(price)", multiCharMaker.makeMultiChar("string1"),
        value));
    ASSERT_EQ("2", autil::StringUtil::toString(value));

    ASSERT_TRUE(agg->getFunResultOfKey<autil::HllCtx>(
        "distinct_count(price)", multiCharMaker.makeMultiChar("string2"),
        value));
    ASSERT_EQ("2", autil::StringUtil::toString(value));

    ASSERT_TRUE(!agg->getFunResultOfKey<autil::HllCtx>("distinct_count(price)", multiCharMaker.makeMultiChar("string1111"), value));
    POOL_DELETE_CLASS(agg);
}


TEST_P(NormalAggregatorTest, testMultiFunctionAggregator_RT) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<int32_t> *expr1 = initFunExpression<int32_t>("fun_int32");
    MaxAggregateFunction<int32_t> *afun1
        = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price", vt_int32, expr1);

    AttributeExpressionTyped<double> *expr2 = initFunExpression<double>("fun_double");
    MinAggregateFunction<double> *afun2
        = POOL_NEW_CLASS(_pool, MinAggregateFunction<double>, "delivery", vt_double, expr2);

    AttributeExpressionTyped<int64_t> *expr3 = initFunExpression<int64_t>("fun_int64");
    AvgAggregateFunction<int64_t> *afun3
        = POOL_NEW_CLASS(_pool, AvgAggregateFunction<int64_t>, "price", vt_int64, expr3);

    CountAggregateFunction *afun4 = POOL_NEW_CLASS(_pool, CountAggregateFunction);

    AttributeExpressionTyped<int64_t> *keyExpr = initKeyExpression<int64_t>("key");

    NormalAggregator<int64_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int64_t>, keyExpr, 100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int64_t>, keyExpr, 100, _pool);
    }
    agg->addAggregateFunction(afun1);
    agg->addAggregateFunction(afun2);
    agg->addAggregateFunction(afun3);
    agg->addAggregateFunction(afun4);

    aggMatchDoc(DOC_NUM, agg);

    checkResult_RT<int32_t, int64_t>(agg, "max(price)", 8, 9);
    checkResult_RT<double, int64_t>(agg, "min(delivery)", 0, 1);
    checkResult_RT<int64_t, int64_t>(agg, "avg(price)", 4, 5);
    checkResult_RT<int64_t, int64_t>(agg, "count()", 5, 5);
    POOL_DELETE_CLASS(agg);
}

template <typename T, typename T_KEY>
void NormalAggregatorTest::checkResult_RT(NormalAggregator<T_KEY>* agg,
        const string &funName, T v1, T v2)
{
    const matchdoc::Reference<T> *funResultRef
        = agg->template getFunResultReference<T>(funName);
    ASSERT_TRUE(funResultRef);


    ASSERT_EQ(2u, agg->getKeyCount());

    matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc((T_KEY)0);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((T)v1, *(funResultRef->getPointer(resultSlab)));
    resultSlab = agg->getResultMatchDoc((T_KEY)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((T)v2, *(funResultRef->getPointer(resultSlab)));
    resultSlab = agg->getResultMatchDoc(31);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);

    T value;
    ASSERT_TRUE(agg->template getFunResultOfKey<T>(funName, 0, value));
    ASSERT_EQ((T)v1, value);

    ASSERT_TRUE(agg->template getFunResultOfKey<T>(funName, 1, value));
    ASSERT_EQ((T)v2, value);

    ASSERT_TRUE(!agg->template getFunResultOfKey<T>(funName, 31, value));
}

TEST_P(NormalAggregatorTest, testAggregatorLess_RT) {
    HA3_LOG(DEBUG, "Begin Test!");

    _keyAttributeExpr = initKeyExpression<int64_t>("key");
    _funAttributeExpr = initFunExpression<int64_t>("fun");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price", vt_double,
                         (AttributeExpressionTyped<int32_t> *)_funAttributeExpr);

    NormalAggregator<int64_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int64_t>, (AttributeExpressionTyped<int64_t> *)
               _keyAttributeExpr, 100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int64_t>, (AttributeExpressionTyped<int64_t> *)
                     _keyAttributeExpr, 100, _pool);
    }
    agg->addAggregateFunction(fun);

    //get result with no docs
    const matchdoc::Reference<int32_t> *funResultRef
        = agg->getFunResultReference<int32_t>("max(price)");
    ASSERT_TRUE(funResultRef);
    matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc((int64_t)0);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);

    int32_t value;
    ASSERT_TRUE(!agg->getFunResultOfKey<int32_t>("max(price)", 0, value));

    matchdoc::MatchDoc scoreDoc = _mdAllocator->allocate((docid_t)1);

    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != scoreDoc);
    _mdVector.push_back(scoreDoc);

    agg->setMatchDocAllocator(_mdAllocator);
    agg->aggregate(scoreDoc);
    if (_mode == AGG_BATCH) {
        agg->endLayer(1.0f);
        agg->estimateResult(1.0);
    }
    //get result with one doc(1)
    funResultRef = agg->getFunResultReference<int32_t>("max(price)");
    ASSERT_TRUE(funResultRef);
    resultSlab = agg->getResultMatchDoc((int64_t)0);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);
    resultSlab = agg->getResultMatchDoc((int64_t)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ(1, *(funResultRef->getPointer(resultSlab)));

    ASSERT_TRUE(agg->getFunResultOfKey<int32_t>("max(price)", 1, value));
    ASSERT_EQ(1, value);

    ASSERT_TRUE(!agg->getFunResultOfKey<int32_t>("max(price)", 0, value));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testAggregatorOneKeyCount) {
    HA3_LOG(DEBUG, "Begin Test!");
    _keyAttributeExpr = initKeyExpression<int64_t>("key");
    _funAttributeExpr = initFunExpression<float>("fun");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<float>, "price", vt_double,
            (AttributeExpressionTyped<float> *)_funAttributeExpr);
    ASSERT_TRUE(fun);

    NormalAggregator<int64_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int64_t>, (AttributeExpressionTyped<int64_t> *)_keyAttributeExpr,
                             1, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int64_t>, (AttributeExpressionTyped<int64_t> *)_keyAttributeExpr,
                             1, _pool);
    }
    agg->addAggregateFunction(fun);

    aggMatchDoc(DOC_NUM, agg);

    const matchdoc::Reference<float> *funResultRef
        = agg->getFunResultReference<float>("max(price)");
    ASSERT_TRUE(funResultRef);

    ASSERT_EQ(1u, agg->getKeyCount());

    matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc((int64_t)0);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((float)8, *(funResultRef->getPointer(resultSlab)));
    resultSlab = agg->getResultMatchDoc((int64_t)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);

    float value;
    ASSERT_TRUE(agg->getFunResultOfKey<float>("max(price)", 0, value));
    ASSERT_EQ((float)8, value);

    ASSERT_TRUE(!agg->getFunResultOfKey<float>("max(price)", 1, value));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testAggregatorNoKeyCount) {
    HA3_LOG(DEBUG, "Begin Test!");
    _keyAttributeExpr = initKeyExpression<int64_t>("key");
    _funAttributeExpr = initFunExpression<float>("fun");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<float>, "price",vt_double,
            (AttributeExpressionTyped<float> *)_funAttributeExpr);
    ASSERT_TRUE(fun);

    NormalAggregator<int64_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int64_t>,
                             (AttributeExpressionTyped<int64_t> *)_keyAttributeExpr,
                             0, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int64_t>,
                             (AttributeExpressionTyped<int64_t> *)_keyAttributeExpr,
                             0, _pool);
    }
    agg->addAggregateFunction(fun);

    aggMatchDoc(DOC_NUM, agg);

    const matchdoc::Reference<float> *funResultRef
        = agg->getFunResultReference<float>("max(price)");
    ASSERT_TRUE(funResultRef);

    ASSERT_EQ(0u, agg->getKeyCount());
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testAggregatorMoreKeyCount) {
    HA3_LOG(DEBUG, "Begin Test!");
    _funAttributeExpr = initFunExpression<float>("fun");
    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<float>, "price", vt_float,
            (AttributeExpressionTyped<float> *)_funAttributeExpr);

    _keyAttributeExpr = initFunExpression<int64_t>("uid");
    NormalAggregator<int64_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int64_t>,
            (AttributeExpressionTyped<int64_t> *)_keyAttributeExpr,
            10, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int64_t>,
            (AttributeExpressionTyped<int64_t> *)_keyAttributeExpr,
            10, _pool);
    }

    agg->addAggregateFunction(fun);

    aggMatchDoc(DOC_NUM, agg);

    const matchdoc::Reference<float> *funResultRef
        = agg->getFunResultReference<float>("max(price)");

    ASSERT_TRUE(funResultRef);
    ASSERT_EQ(10u, agg->getKeyCount());
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testMultiValueKeyAggregator) {
    HA3_LOG(DEBUG, "Begin Test!");
    _keyAttributeExpr = initMultiValueKeyExpression<int64_t>("key");
    _funAttributeExpr = initFunExpression<float>("fun");

    AggregateFunction *funMax = POOL_NEW_CLASS(_pool, MaxAggregateFunction<float>, "price",vt_double,
            (AttributeExpressionTyped<float> *)_funAttributeExpr);
    ASSERT_TRUE(funMax);

    AggregateFunction *funCount = POOL_NEW_CLASS(_pool, CountAggregateFunction);

    ASSERT_TRUE(funCount);

    NormalAggregator<int64_t, MultiValueType<int64_t> >* agg = NULL;
    if (_mode == AGG_NORMAL) {
        typedef NormalAggregator<int64_t, MultiValueType<int64_t> > AggregatorType;
        agg = POOL_NEW_CLASS(_pool, AggregatorType,
                             (AttributeExpressionTyped<MultiInt64> *)_keyAttributeExpr,
                             1000, _pool);
    } else {
        typedef BatchAggregator<int64_t, MultiValueType<int64_t> > AggregatorType;
        agg = POOL_NEW_CLASS(_pool, AggregatorType,
                             (AttributeExpressionTyped<MultiInt64> *)_keyAttributeExpr,
                             1000, _pool);
    }

    agg->addAggregateFunction(funMax);
    agg->addAggregateFunction(funCount);

    aggMatchDoc(DOC_NUM, agg);

    const matchdoc::Reference<float> *funResultRef
        = agg->getFunResultReference<float>("max(price)");
    ASSERT_TRUE(funResultRef);

    ASSERT_EQ(3u, agg->getKeyCount());

    matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc((int64_t)0);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((float)9, *(funResultRef->getPointer(resultSlab)));

    float floatValue;
    ASSERT_TRUE(agg->getFunResultOfKey<float>("max(price)", 0, floatValue));
    ASSERT_EQ((float)9, floatValue);

    int64_t int64Value;
    ASSERT_TRUE(agg->getFunResultOfKey<int64_t>("count()", 0, int64Value));
    ASSERT_EQ((int64_t)10, int64Value);

    ASSERT_TRUE(agg->getFunResultOfKey<float>("max(price)", 1, floatValue));
    ASSERT_EQ((float)9, floatValue);

    ASSERT_TRUE(agg->getFunResultOfKey<int64_t>("count()", 1, int64Value));
    ASSERT_EQ((int64_t)10, int64Value);

    ASSERT_TRUE(agg->getFunResultOfKey<float>("max(price)", 2, floatValue));
    ASSERT_EQ((float)9, floatValue);

    ASSERT_TRUE(agg->getFunResultOfKey<int64_t>("count()", 2, int64Value));
    ASSERT_EQ((int64_t)10, int64Value);

    ASSERT_TRUE(!agg->getFunResultOfKey<int64_t>("count()", 3, int64Value));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testCollectResultWithNoDoc) {
    HA3_LOG(DEBUG, "begin test");
    _keyAttributeExpr = initKeyExpression<int32_t>("userid");
    _funAttributeExpr = initFunExpression<int32_t>("price");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price",vt_int32,
            (AttributeExpressionTyped<int32_t> *)_funAttributeExpr);
    ASSERT_TRUE(fun);
    NormalAggregator<int32_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg =POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>,
                            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                            100, _pool);
    } else {
        agg =POOL_NEW_CLASS(_pool, BatchAggregator<int32_t>,
                            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                            100, _pool);
    }
    agg->addAggregateFunction(fun);

    aggMatchDoc(0, agg);

    AggregateResultsPtr aggResultsPtr = agg->collectAggregateResult();
    (*aggResultsPtr)[0]->constructGroupValueMap();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)0, aggResultPtr->getAggExprValueCount());
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testCollectResult) {
    HA3_LOG(DEBUG, "begin test");
    _keyAttributeExpr = initKeyExpression<int32_t>("userid");
    _funAttributeExpr = initFunExpression<int32_t>("price");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price",vt_int32,
            (AttributeExpressionTyped<int32_t> *)_funAttributeExpr);
    ASSERT_TRUE(fun);
    NormalAggregator<int32_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>, (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int32_t>, (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    }
    agg->addAggregateFunction(fun);

    aggMatchDoc(DOC_NUM, agg);
    AggregateResultsPtr aggResultsPtr = agg->collectAggregateResult();
    (*aggResultsPtr)[0]->constructGroupValueMap();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    int32_t aggFunResult = 0;
    ASSERT_EQ(8,
                         aggResultPtr->getAggFunResult<int32_t>("0", 0, aggFunResult));
    ASSERT_EQ(9,
                         aggResultPtr->getAggFunResult<int32_t>("1", 0, aggFunResult));

    ASSERT_EQ((uint32_t)1, aggResultPtr->getAggFunCount());
    ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testEstimateResult) {
    HA3_LOG(DEBUG, "begin test");

    double factor = 2.0;
    vector<int32_t> priceVector;
    makeFakeData_fun<int32_t>(priceVector, 20);
    AttributeExpressionTyped<int32_t> *expr1 =
        initFunExpressionWithFakeData<int32_t>("fun_int32", priceVector);
    MaxAggregateFunction<int32_t> *afun1
        = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price", vt_int32, expr1);

    vector<int32_t> deliveryVector;

    makeFakeData_fun<int32_t>(deliveryVector, 20);
    AttributeExpressionTyped<int32_t> *expr2 =
        initFunExpressionWithFakeData<int32_t>("fun_double", deliveryVector);
    MinAggregateFunction<int32_t> *afun2
        = POOL_NEW_CLASS(_pool, MinAggregateFunction<int32_t>, "delivery", vt_int32, expr2);

    AttributeExpressionTyped<int32_t> *expr3 =
        initFunExpressionWithFakeData<int32_t>("fun_int32_2", priceVector);

    SumAggregateFunction<int32_t> *afun3 =
        POOL_NEW_CLASS(_pool, SumAggregateFunction<int32_t>, "price", vt_int32, expr3);
    CountAggregateFunction *afun4 = POOL_NEW_CLASS(_pool, CountAggregateFunction);

    vector<int32_t> keyDataVector;
    makeFakeData_key<int32_t>(keyDataVector, 20);
    AttributeExpressionTyped<int32_t> *keyExpr =
        initKeyExpression<int32_t>("key");

    NormalAggregator<int32_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>, keyExpr, 100, _pool, 5, 2);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int32_t>, keyExpr, 100, _pool, 5, 2);

    }
    agg->addAggregateFunction(afun1);
    agg->addAggregateFunction(afun2);
    agg->addAggregateFunction(afun3);
    agg->addAggregateFunction(afun4);

    aggMatchDoc(DOC_NUM, agg);
    agg->estimateResult(factor);
    if (_mode == AGG_NORMAL) {
        ASSERT_EQ(uint32_t(7), agg->getAggregateCount());
    } else {
        ASSERT_EQ(uint32_t(2), agg->getAggregateCount());
    }

    AggregateResultsPtr aggResultsPtr = agg->collectAggregateResult();
    (*aggResultsPtr)[0]->constructGroupValueMap();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)4, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    int32_t aggFunResult = 0;

    if (_mode == AGG_NORMAL) {
        ASSERT_EQ(8,
                  aggResultPtr->getAggFunResult<int32_t>("0", 0, aggFunResult));
        ASSERT_EQ(3,
                  aggResultPtr->getAggFunResult<int32_t>("1", 0, aggFunResult));
        ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));

        ASSERT_EQ(0, aggResultPtr->getAggFunResult<int32_t>("0", 1, aggFunResult));
        ASSERT_EQ(1, aggResultPtr->getAggFunResult<int32_t>("1", 1, aggFunResult));
        ASSERT_EQ(string("min"), aggResultPtr->getAggFunName(1));


        ASSERT_EQ(68, aggResultPtr->getAggFunResult<int32_t>("0", 2, aggFunResult));
        ASSERT_EQ(8, aggResultPtr->getAggFunResult<int32_t>("1", 2, aggFunResult));
        ASSERT_EQ(string("sum"), aggResultPtr->getAggFunName(2));


        ASSERT_EQ((int64_t)14, aggResultPtr->getAggFunResult<int64_t>("0", 3, aggFunResult));
        ASSERT_EQ((int64_t)4, aggResultPtr->getAggFunResult<int64_t>("1", 3, aggFunResult));
        ASSERT_EQ(string("count"), aggResultPtr->getAggFunName(3));
    } else {
        ASSERT_EQ(0,
                  aggResultPtr->getAggFunResult<int32_t>("0", 0, aggFunResult));
        ASSERT_EQ(5,
                  aggResultPtr->getAggFunResult<int32_t>("1", 0, aggFunResult));
        ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));

        ASSERT_EQ(0, aggResultPtr->getAggFunResult<int32_t>("0", 1, aggFunResult));
        ASSERT_EQ(5, aggResultPtr->getAggFunResult<int32_t>("1", 1, aggFunResult));
        ASSERT_EQ(string("min"), aggResultPtr->getAggFunName(1));


        ASSERT_EQ(0, aggResultPtr->getAggFunResult<int32_t>("0", 2, aggFunResult));
        ASSERT_EQ(50, aggResultPtr->getAggFunResult<int32_t>("1", 2, aggFunResult));
        ASSERT_EQ(string("sum"), aggResultPtr->getAggFunName(2));


        ASSERT_EQ((int64_t)10, aggResultPtr->getAggFunResult<int64_t>("0", 3, aggFunResult));
        ASSERT_EQ((int64_t)10, aggResultPtr->getAggFunResult<int64_t>("1", 3, aggFunResult));
        ASSERT_EQ(string("count"), aggResultPtr->getAggFunName(3));

    }
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testCollectResultWithMultiFunction) {
    HA3_LOG(DEBUG, "begin test");
    _keyAttributeExpr = initKeyExpression<int32_t>("userid");
    AttributeExpression *attributeExpr1 = initFunExpression<int32_t>("price");
    AttributeExpression *attributeExpr2 = initFunExpression<int32_t>("delivery");
    AggregateFunction *fun1 = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price", vt_int32,
            (AttributeExpressionTyped<int32_t> *)attributeExpr1);
    AggregateFunction *fun2 = POOL_NEW_CLASS(_pool, MinAggregateFunction<int32_t>, "delivery", vt_int32,
            (AttributeExpressionTyped<int32_t> *)attributeExpr2);
    ASSERT_TRUE(fun1);
    ASSERT_TRUE(fun2);
    NormalAggregator<int32_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>,
            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
            100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int32_t>,
            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
            100, _pool);
    }
    agg->addAggregateFunction(fun1);
    agg->addAggregateFunction(fun2);

    aggMatchDoc(DOC_NUM, agg);

    AggregateResultsPtr aggResultsPtr = agg->collectAggregateResult();
(*aggResultsPtr)[0]->constructGroupValueMap();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    int32_t aggFunResult = 0;
    ASSERT_EQ(8,
                         aggResultPtr->getAggFunResult<int32_t>("0", 0, aggFunResult));
    ASSERT_EQ(9,
                         aggResultPtr->getAggFunResult<int32_t>("1", 0, aggFunResult));
    ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));

    ASSERT_EQ(0, aggResultPtr->getAggFunResult<int32_t>("0", 1, aggFunResult));
    ASSERT_EQ(1, aggResultPtr->getAggFunResult<int32_t>("1", 1, aggFunResult));
    ASSERT_EQ(string("min"), aggResultPtr->getAggFunName(1));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testSameAggregatorFunctionInOneQueryMax) {
    HA3_LOG(DEBUG, "begin test");
    _keyAttributeExpr = initKeyExpression<int32_t>("userid");
    AttributeExpressionTyped<int32_t> *expr1 = initFunExpression<int32_t>("max");
    MaxAggregateFunction<int32_t> *afun1
        = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price", vt_int32, expr1);

    vector<int32_t> fakedata2;
    for ( int i = 0 ; i < DOC_NUM ; ++i ) {
        fakedata2.push_back(i*10);
    }
    AttributeExpressionTyped<int32_t> *expr2 = initFunExpressionWithFakeData<int32_t>("max",fakedata2);
    MaxAggregateFunction<int32_t> *afun2
        = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "num", vt_int32, expr2);


    NormalAggregator<int32_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>,
                             (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int32_t>,
                             (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    }

    agg->addAggregateFunction(afun1);
    agg->addAggregateFunction(afun2);

    aggMatchDoc(DOC_NUM, agg);

    matchdoc::MatchDoc scoreDoc1 = _mdAllocator->allocate((docid_t)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != scoreDoc1);
    _mdVector.push_back(scoreDoc1);
    agg->aggregate(scoreDoc1);
    matchdoc::MatchDoc resultSlab;

    const matchdoc::Reference<int32_t> *funResultRef1
        = agg->getFunResultReference<int32_t>("max(price)");
    ASSERT_TRUE(funResultRef1);

    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ((int32_t)8, *(funResultRef1->getPointer(resultSlab)));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_EQ((int32_t)9, *(funResultRef1->getPointer(resultSlab)));

    const matchdoc::Reference<int32_t> *funResultRef2
        = agg->getFunResultReference<int32_t>("max(num)");
    ASSERT_TRUE(funResultRef2);

    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ((int32_t)80, *(funResultRef2->getPointer(resultSlab)));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_EQ((int32_t)90, *(funResultRef2->getPointer(resultSlab)));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testSameAggregatorFunctionInOneQueryMin) {
    HA3_LOG(DEBUG, "begin test");
    _keyAttributeExpr = initKeyExpression<int32_t>("userid");
    AttributeExpressionTyped<int32_t> *expr1 = initFunExpression<int32_t>("min");
    MinAggregateFunction<int32_t> *afun1
        = POOL_NEW_CLASS(_pool, MinAggregateFunction<int32_t>, "price", vt_int32, expr1);


    vector<int32_t> fakedata2;
    for ( int i = 1 ; i <= DOC_NUM ; ++i ) {
        fakedata2.push_back(i*10);
    }
    AttributeExpressionTyped<int32_t> *expr2 = initFunExpressionWithFakeData<int32_t>("min",fakedata2);
    MinAggregateFunction<int32_t> *afun2
        = POOL_NEW_CLASS(_pool, MinAggregateFunction<int32_t>, "num", vt_int32, expr2);

    NormalAggregator<int32_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>,
            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
            100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>,
            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
            100, _pool);
    }

    agg->addAggregateFunction(afun1);
    agg->addAggregateFunction(afun2);


    aggMatchDoc(DOC_NUM, agg);

    matchdoc::MatchDoc resultSlab;

    const matchdoc::Reference<int32_t> *funResultRef1
        = agg->getFunResultReference<int32_t>("min(price)");
    ASSERT_TRUE(funResultRef1);

    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ((int32_t)0, *(funResultRef1->getPointer(resultSlab)));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_EQ((int32_t)1, *(funResultRef1->getPointer(resultSlab)));

    const matchdoc::Reference<int32_t> *funResultRef2
        = agg->getFunResultReference<int32_t>("min(num)");
    ASSERT_TRUE(funResultRef2);

    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ((int32_t)10, *(funResultRef2->getPointer(resultSlab)));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_EQ((int32_t)20, *(funResultRef2->getPointer(resultSlab)));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testSameAggregatorFunctionInOneQuerySum) {
    HA3_LOG(DEBUG, "begin test");
    _keyAttributeExpr = initKeyExpression<int32_t>("userid");
    AttributeExpressionTyped<int32_t> *expr1 = initFunExpression<int32_t>("sum");
    SumAggregateFunction<int32_t> *afun1
        = POOL_NEW_CLASS(_pool, SumAggregateFunction<int32_t>, "price", vt_int32, expr1);


    vector<int32_t> fakedata2;
    for ( int i = 0 ; i < DOC_NUM ; ++i ) {
        fakedata2.push_back(10);
    }
    AttributeExpressionTyped<int32_t> *expr2 = initFunExpressionWithFakeData<int32_t>("sum",fakedata2);
    SumAggregateFunction<int32_t> *afun2
        = POOL_NEW_CLASS(_pool, SumAggregateFunction<int32_t>, "num", vt_int32, expr2);

    NormalAggregator<int32_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>,
                             (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int32_t>,
                             (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    }

    agg->addAggregateFunction(afun1);
    agg->addAggregateFunction(afun2);

    aggMatchDoc(DOC_NUM, agg);

    matchdoc::MatchDoc resultSlab;

    const matchdoc::Reference<int32_t> *funResultRef1
        = agg->getFunResultReference<int32_t>("sum(price)");
    ASSERT_TRUE(funResultRef1);

    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ((int32_t)20, *(funResultRef1->getPointer(resultSlab)));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_EQ((int32_t)25, *(funResultRef1->getPointer(resultSlab)));

    const matchdoc::Reference<int32_t> *funResultRef2
         = agg->getFunResultReference<int32_t>("sum(num)");
    ASSERT_TRUE(funResultRef2);

    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ((int32_t)50, *(funResultRef2->getPointer(resultSlab)));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_EQ((int32_t)50, *(funResultRef2->getPointer(resultSlab)));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testSameAggregatorFunctionInOneQueryAvg) {
    HA3_LOG(DEBUG, "begin test");
    _keyAttributeExpr = initKeyExpression<int32_t>("userid");
    AttributeExpressionTyped<int32_t> *expr1 = initFunExpression<int32_t>("avg");
    AvgAggregateFunction<int32_t> *afun1
        = POOL_NEW_CLASS(_pool, AvgAggregateFunction<int32_t>, "price", vt_int32, expr1);

    vector<int32_t> fakedata2;
    for ( int i = 0 ; i < DOC_NUM ; ++i ) {
        fakedata2.push_back(i*10);
    }
    AttributeExpressionTyped<int32_t> *expr2 = initFunExpressionWithFakeData<int32_t>("avg",fakedata2);

    AvgAggregateFunction<int32_t> *afun2
        = POOL_NEW_CLASS(_pool, AvgAggregateFunction<int32_t>, "num", vt_int32, expr2);

    NormalAggregator<int32_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>,
            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
            100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int32_t>,
            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
            100, _pool);
    }

    agg->addAggregateFunction(afun1);
    agg->addAggregateFunction(afun2);

    aggMatchDoc(DOC_NUM, agg);

    matchdoc::MatchDoc resultSlab;

    const matchdoc::Reference<int32_t> *funResultRef1
        = agg->getFunResultReference<int32_t>("avg(price)");
    ASSERT_TRUE(funResultRef1);

    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ((int32_t)4, *(funResultRef1->getPointer(resultSlab)));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_EQ((int32_t)5, *(funResultRef1->getPointer(resultSlab)));

    const matchdoc::Reference<int32_t> *funResultRef2
        = agg->getFunResultReference<int32_t>("avg(num)");
    ASSERT_TRUE(funResultRef2);

    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ((int32_t)40, *(funResultRef2->getPointer(resultSlab)));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_EQ((int32_t)50, *(funResultRef2->getPointer(resultSlab)));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testSameAggregatorFunctionInOneQueryDistinctCount) {
    HA3_LOG(DEBUG, "begin test");
    _keyAttributeExpr = initKeyExpression<int32_t>("userid");
    AttributeExpressionTyped<int32_t> *expr1 = initFunExpression<int32_t>("price");
    DistinctCountAggregateFunction<int32_t> *afun1
        = POOL_NEW_CLASS(_pool, DistinctCountAggregateFunction<int32_t>, "price",  expr1);


    vector<int32_t> fakedata2;
    for ( int i = 0 ; i < DOC_NUM ; ++i ) {
        fakedata2.push_back(10);
    }
    AttributeExpressionTyped<int32_t> *expr2 = initFunExpressionWithFakeData<int32_t>("num",fakedata2);
    DistinctCountAggregateFunction<int32_t> *afun2
        = POOL_NEW_CLASS(_pool, DistinctCountAggregateFunction<int32_t>, "num",  expr2);

    NormalAggregator<int32_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>,
                             (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int32_t>,
                             (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    }

    agg->addAggregateFunction(afun1);
    agg->addAggregateFunction(afun2);

    aggMatchDoc(DOC_NUM, agg);

    matchdoc::MatchDoc resultSlab;

    const matchdoc::Reference<autil::HllCtx> *funResultRef1 =
        agg->getFunResultReference<autil::HllCtx>("distinct_count(price)");
    ASSERT_TRUE(funResultRef1);

    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ("5", autil::StringUtil::toString(
                        *(funResultRef1->getPointer(resultSlab))));

    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_EQ("5", autil::StringUtil::toString(
                        *(funResultRef1->getPointer(resultSlab))));

    const matchdoc::Reference<autil::HllCtx> *funResultRef2
        = agg->getFunResultReference<autil::HllCtx>("distinct_count(num)");
    ASSERT_TRUE(funResultRef2);

    resultSlab = agg->getResultMatchDoc((int32_t)0);
    ASSERT_EQ("1", autil::StringUtil::toString(
                        *(funResultRef2->getPointer(resultSlab))));
    resultSlab = agg->getResultMatchDoc((int32_t)1);
    ASSERT_EQ("1", autil::StringUtil::toString(
                        *(funResultRef2->getPointer(resultSlab))));

    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testMultiFunctionAggregatorWithSampler) {
    HA3_LOG(DEBUG, "Begin Test!");

    vector<int32_t> priceVector;
    makeFakeData_fun<int32_t>(priceVector, 20);
    AttributeExpressionTyped<int32_t> *expr1 =
        initFunExpressionWithFakeData<int32_t>("fun_int32", priceVector);
    MaxAggregateFunction<int32_t> *afun1
        = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price", vt_int32, expr1);

    vector<int32_t> deliveryVector;
    makeFakeData_fun<int32_t>(deliveryVector, 20);
    AttributeExpressionTyped<int32_t> *expr2 =
        initFunExpressionWithFakeData<int32_t>("fun_double", deliveryVector);
    MinAggregateFunction<int32_t> *afun2
        = POOL_NEW_CLASS(_pool, MinAggregateFunction<int32_t>, "delivery", vt_int32, expr2);

    AttributeExpressionTyped<int32_t> *expr3 =
        initFunExpressionWithFakeData<int32_t>("fun_int32_2", priceVector);
    SumAggregateFunction<int32_t> *afun3
        = POOL_NEW_CLASS(_pool, SumAggregateFunction<int32_t>, "price", vt_int32, expr3);

    CountAggregateFunction *afun4 = POOL_NEW_CLASS(_pool, CountAggregateFunction);

    vector<int32_t> keyDataVector;
    makeFakeData_key<int32_t>(keyDataVector, 20);
    AttributeExpressionTyped<int32_t> *keyExpr =
        initKeyExpression<int32_t>("key");

    NormalAggregator<int32_t>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int32_t>, keyExpr, 100, _pool, 5, 2);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int32_t>, keyExpr, 100, _pool, 5, 2);
    }
    agg->addAggregateFunction(afun1);
    agg->addAggregateFunction(afun2);
    agg->addAggregateFunction(afun3);
    agg->addAggregateFunction(afun4);

    aggMatchDoc(DOC_NUM, agg);
    AggregateResultsPtr aggResultsPtr = agg->collectAggregateResult();
    (*aggResultsPtr)[0]->constructGroupValueMap();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)4, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    int32_t aggFunResult = 0;
    if (_mode == AGG_NORMAL) {
        ASSERT_EQ(8,
                  aggResultPtr->getAggFunResult<int32_t>("0", 0, aggFunResult));
        ASSERT_EQ(3,
                  aggResultPtr->getAggFunResult<int32_t>("1", 0, aggFunResult));
        ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));

        ASSERT_EQ(0, aggResultPtr->getAggFunResult<int32_t>("0", 1, aggFunResult));
        ASSERT_EQ(1, aggResultPtr->getAggFunResult<int32_t>("1", 1, aggFunResult));
        ASSERT_EQ(string("min"), aggResultPtr->getAggFunName(1));

        ASSERT_EQ(34, aggResultPtr->getAggFunResult<int32_t>("0", 2, aggFunResult));
        ASSERT_EQ(4, aggResultPtr->getAggFunResult<int32_t>("1", 2, aggFunResult));
        ASSERT_EQ(string("sum"), aggResultPtr->getAggFunName(2));

        ASSERT_EQ((int64_t)7, aggResultPtr->getAggFunResult<int64_t>("0", 3, aggFunResult));
        ASSERT_EQ((int64_t)2, aggResultPtr->getAggFunResult<int64_t>("1", 3, aggFunResult));
        ASSERT_EQ(string("count"), aggResultPtr->getAggFunName(3));
    } else {
        ASSERT_EQ(0,
                  aggResultPtr->getAggFunResult<int32_t>("0", 0, aggFunResult));
        ASSERT_EQ(5,
                  aggResultPtr->getAggFunResult<int32_t>("1", 0, aggFunResult));
        ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));

        ASSERT_EQ(0, aggResultPtr->getAggFunResult<int32_t>("0", 1, aggFunResult));
        ASSERT_EQ(5, aggResultPtr->getAggFunResult<int32_t>("1", 1, aggFunResult));
        ASSERT_EQ(string("min"), aggResultPtr->getAggFunName(1));

        ASSERT_EQ(0, aggResultPtr->getAggFunResult<int32_t>("0", 2, aggFunResult));
        ASSERT_EQ(25, aggResultPtr->getAggFunResult<int32_t>("1", 2, aggFunResult));
        ASSERT_EQ(string("sum"), aggResultPtr->getAggFunName(2));

        ASSERT_EQ((int64_t)5, aggResultPtr->getAggFunResult<int64_t>("0", 3, aggFunResult));
        ASSERT_EQ((int64_t)5, aggResultPtr->getAggFunResult<int64_t>("1", 3, aggFunResult));
        ASSERT_EQ(string("count"), aggResultPtr->getAggFunName(3));
    }

    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testAggregatorWithMaxSortCount) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<int> *expr
        = _fakeAttrExprMaker->createAttrExpr<int>("key", "0,1,1,2");
    _attrExprPool->addNeedDeleteExpr(expr);
    _keyAttributeExpr = expr;

    CountAggregateFunction *fun = POOL_NEW_CLASS(_pool, CountAggregateFunction);
    ASSERT_TRUE(fun);

    NormalAggregator<int>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int>,
                             (AttributeExpressionTyped<int> *)_keyAttributeExpr, 1,
                             _pool, 0, 1, 2);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int>,
                             (AttributeExpressionTyped<int> *)_keyAttributeExpr, 1,
                             _pool, 10, 10, 2);
    }
    agg->addAggregateFunction(fun);

    aggMatchDoc(4, agg);

    const matchdoc::Reference<int64_t> *funResultRef
        = agg->getFunResultReference<int64_t>("count()");
    ASSERT_TRUE(funResultRef);

    ASSERT_EQ(2u, agg->getKeyCount());

    matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc((int)0);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((int64_t)1, *(funResultRef->getPointer(resultSlab)));
    resultSlab = agg->getResultMatchDoc((int)1);
    ASSERT_EQ((int64_t)2, *(funResultRef->getPointer(resultSlab)));

    agg->endLayer(1.0f);
    AggregateResultsPtr aggResultsPtr = agg->collectAggregateResult();
    (*aggResultsPtr)[0]->constructGroupValueMap();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)1, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)1, aggResultPtr->getAggExprValueCount());

    ASSERT_EQ((int64_t)2, aggResultPtr->getAggFunResult<int64_t>("1", 0, (int64_t)0));
    ASSERT_EQ(string("count"), aggResultPtr->getAggFunName(0));
    POOL_DELETE_CLASS(agg);
}

TEST_P(NormalAggregatorTest, testSubGroupKey) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.attributes = "sub_key: int32_t: 0,1,1,2,3,2,3;"
                           "sub_id: int32_t : 0,1,1,2,1,2,3;"
                           "sub_price: int32_t: 1,1,3,1,2,1,3;"
                           "price: int32_t: 1,2,3";
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    AttributeExpressionCreator *attrExprCreator = new FakeAttributeExpressionCreator(
            _pool, indexPartReaderPtr, NULL, NULL, NULL, NULL, _mdAllocator->getSubDocRef());
    AttributeExpression *keyExpr = attrExprCreator->createAtomicExpr("sub_key");

    AttributeExpression *funExpr = attrExprCreator->createAtomicExpr("price");
    AggregateFunction *sumFun = POOL_NEW_CLASS(_pool, SumAggregateFunction<int32_t>,
            "price",vt_int32, (AttributeExpressionTyped<int> *)funExpr);
    funExpr = attrExprCreator->createAtomicExpr("sub_id");
    AggregateFunction *maxFun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>,
            "sub_id",vt_int32, (AttributeExpressionTyped<int> *)funExpr);
    CountAggregateFunction *countFun = POOL_NEW_CLASS(_pool, CountAggregateFunction);

    NormalAggregator<int>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregator<int>,
                             (AttributeExpressionTyped<int> *)keyExpr, 100,
                             _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregator<int>,
                             (AttributeExpressionTyped<int> *)keyExpr, 100,
                             _pool);
    }
    agg->addAggregateFunction(countFun);
    agg->addAggregateFunction(sumFun);
    agg->addAggregateFunction(maxFun);

    // create agg filter: sub_price > 1
    AttributeExpression *subPriceExpr = attrExprCreator->createAtomicExpr("sub_price");
    ConstAttributeExpression<int32_t> *constAttrExpr = new ConstAttributeExpression<int32_t>(1);
    BinaryAttributeExpression<std::greater, int32_t> *filterExpr =
        new BinaryAttributeExpression<std::greater, int32_t>(subPriceExpr, constAttrExpr);
    filterExpr->setIsSubExpression(true);
    Filter *aggFilter = POOL_NEW_CLASS(_pool, Filter, filterExpr);
    agg->setFilter(aggFilter);

    aggSubMatchDoc("3, 2, 2", agg);

    AggregateResultsPtr aggResultsPtr = agg->collectAggregateResult();
    (*aggResultsPtr)[0]->constructGroupValueMap();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)3, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    ASSERT_EQ((int64_t)1, aggResultPtr->getAggFunResult<int64_t>("1", 0, 0));
    ASSERT_EQ((int64_t)2, aggResultPtr->getAggFunResult<int64_t>("3", 0, 0));
    ASSERT_EQ(string("count"), aggResultPtr->getAggFunName(0));

    ASSERT_EQ((int32_t)1, aggResultPtr->getAggFunResult<int32_t>("1", 1, 0));
    ASSERT_EQ((int32_t)5, aggResultPtr->getAggFunResult<int32_t>("3", 1, 0));
    ASSERT_EQ(string("sum"), aggResultPtr->getAggFunName(1));

    ASSERT_EQ((int32_t)1, aggResultPtr->getAggFunResult<int32_t>("1", 2, 0));
    ASSERT_EQ((int32_t)3, aggResultPtr->getAggFunResult<int32_t>("3", 2, 0));
    ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(2));

    POOL_DELETE_CLASS(agg);
    DELETE_AND_SET_NULL(attrExprCreator);
    DELETE_AND_SET_NULL(constAttrExpr);
    DELETE_AND_SET_NULL(filterExpr);
}

// use macro INSTANTIATE_TEST_SUITE_P when gtest(version) > 1.8.0
INSTANTIATE_TEST_CASE_P(TypedTest, NormalAggregatorTest, testing::Values(AGG_NORMAL, AGG_BATCH));

END_HA3_NAMESPACE(search);
