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
#include <ha3/search/test/NormalAggregatorDenseMapTest.h>
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

HA3_LOG_SETUP(search, NormalAggregatorDenseMapTest);


NormalAggregatorDenseMapTest::NormalAggregatorDenseMapTest() : DOC_NUM(10){
    _mdAllocator = NULL;
    _funAttributeExpr = NULL;
    _keyAttributeExpr = NULL;
    _attrExprPool = NULL;
    _pool = new autil::mem_pool::Pool(1024);
    _mode = AGG_NORMAL;
}

NormalAggregatorDenseMapTest::~NormalAggregatorDenseMapTest() {
    delete _pool;
}

void NormalAggregatorDenseMapTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _mdAllocator = new common::Ha3MatchDocAllocator(_pool, true);
    _attrExprPool = new AttributeExpressionPool;
    _fakeAttrExprMaker.reset(new FakeAttributeExpressionMaker(_pool));
}

void NormalAggregatorDenseMapTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    _fakeAttrExprMaker.reset();
    for (vector<matchdoc::MatchDoc>::iterator it = _mdVector.begin();
         it != _mdVector.end(); it++)
    {
        _mdAllocator->deallocate(*it);
    }
    _mdVector.clear();
    DELETE_AND_SET_NULL(_mdAllocator);
    DELETE_AND_SET_NULL(_attrExprPool);
}

template <typename T>
void NormalAggregatorDenseMapTest::makeFakeData_fun(std::vector<T> &fakeData, int count)
{
    for (int i = 0; i < count; i++) {
        fakeData.push_back((T)i);
    }
}

template <typename T>
void NormalAggregatorDenseMapTest::makeFakeData_key(std::vector<T> &fakeData, int count, int mod)
{
    for (int i = 0; i < count; i++) {
        fakeData.push_back((T)(i % mod));
    }
}

template <typename T>
void NormalAggregatorDenseMapTest::makeFakeMutiValueData_key(std::vector<std::vector<T> > &fakeData, int count) {
    const int recordPerDoc = 3;
    for (int i = 0; i < count; i++) {
        std::vector<T> records;
        for (int j = 0; j < recordPerDoc; j++) {
            records.push_back(j);
        }
        fakeData.push_back(records);
    }
}

TEST_F(NormalAggregatorDenseMapTest, testINT_32Aggregator_RT) {
    HA3_LOG(DEBUG, "Begin Test!");
    _keyAttributeExpr = initKeyExpression<int64_t>("key");
    _funAttributeExpr = initFunExpression<int64_t>("attr");
    testAggregator_RT<int64_t, int64_t>(sizeof(int64_t));
}

TEST_F(NormalAggregatorDenseMapTest, testINT_64Aggregator_RT) {
    HA3_LOG(DEBUG, "Begin Test!");
    _keyAttributeExpr = initKeyExpression<int32_t>("key");
    _funAttributeExpr = initFunExpression<int64_t>("attr");
    testAggregator_RT<int64_t, int32_t>(sizeof(int64_t));
}

TEST_F(NormalAggregatorDenseMapTest, testFLOATAggregator_RT) {
    HA3_LOG(DEBUG, "Begin Test!");
    _keyAttributeExpr = initKeyExpression<float>("key");
    _funAttributeExpr = initFunExpression<float>("attr");
    testAggregator_RT<float, float>(sizeof(float));
}

TEST_F(NormalAggregatorDenseMapTest, testDOUBLEAggregator_RT) {
    HA3_LOG(DEBUG, "Begin Test!");
    _keyAttributeExpr = initKeyExpression<double>("key");
    _funAttributeExpr = initFunExpression<double>("attr");
    testAggregator_RT<double, double>(sizeof(double));
}

TEST_F(NormalAggregatorDenseMapTest, testMultiCharAggregator) {
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

    NormalAggregatorDenseMultiChar* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseMultiChar, keyExpr, 100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseMultiChar, keyExpr, 100, _pool);
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

TEST_F(NormalAggregatorDenseMapTest, testMultiCharAggregatorOfDistinctCount) {
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

    NormalAggregatorDenseMultiChar* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseMultiChar, keyExpr, 100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseMultiChar, keyExpr, 100, _pool);
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

TEST_F(NormalAggregatorDenseMapTest, testMultiStringAggregator) {
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

    NormalAggregator<MultiChar, MultiString, GroupMultiCharMap>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        typedef NormalAggregator<MultiChar, MultiString, GroupMultiCharMap> AggregatorType;
        agg = POOL_NEW_CLASS(_pool, AggregatorType, keyExpr, 100, _pool);
    } else {
        typedef BatchAggregator<MultiChar, MultiString, GroupMultiCharMap> AggregatorType;
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

TEST_F(NormalAggregatorDenseMapTest, testMultiStringAggregatorOfDistinctCount) {
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

    NormalAggregator<MultiChar, MultiString, GroupMultiCharMap>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        typedef NormalAggregator<MultiChar, MultiString, GroupMultiCharMap> AggregatorType;
        agg = POOL_NEW_CLASS(_pool, AggregatorType, keyExpr, 100, _pool);
    } else {
        typedef BatchAggregator<MultiChar, MultiString, GroupMultiCharMap> AggregatorType;
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

template <typename T, typename T_KEY>
void NormalAggregatorDenseMapTest::testAggregator_RT(int32_t size) {
    HA3_LOG(DEBUG, "Begin Test!");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<T>, "price",vt_double,
            (AttributeExpressionTyped<T> *)_funAttributeExpr);
    ASSERT_TRUE(fun);

    typedef std::tr1::unordered_map<T_KEY, matchdoc::MatchDoc> GroupTKEYMap;
    typedef NormalAggregator<T_KEY, T_KEY, GroupTKEYMap> NormalAggregatorTKEY;
    typedef BatchAggregator<T_KEY, T_KEY, GroupTKEYMap> BatchAggregatorTKEY;
    NormalAggregatorTKEY* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorTKEY, (AttributeExpressionTyped<T_KEY> *)_keyAttributeExpr,
                             100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorTKEY, (AttributeExpressionTyped<T_KEY> *)_keyAttributeExpr,
                             100, _pool);

    }
    agg->addAggregateFunction(fun);


    aggMatchDoc(DOC_NUM, agg);

    const matchdoc::Reference<T> *funResultRef
        = agg->template getFunResultReference<T>("max(price)");
    ASSERT_TRUE(funResultRef);

    ASSERT_EQ(2u, agg->getKeyCount());

    matchdoc::MatchDoc resultSlab = agg->getResultMatchDoc((T_KEY)0);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((T)8, *(funResultRef->getPointer(resultSlab)));
    resultSlab = agg->getResultMatchDoc((T_KEY)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((T)9, *(funResultRef->getPointer(resultSlab)));
    resultSlab = agg->getResultMatchDoc(31);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);

    T value;
    ASSERT_TRUE(agg->template getFunResultOfKey<T>("max(price)", 0, value));
    ASSERT_EQ((T)8, value);

    ASSERT_TRUE(agg->template getFunResultOfKey<T>("max(price)", 1, value));
    ASSERT_EQ((T)9, value);

    ASSERT_TRUE(!agg->template getFunResultOfKey<T>("max(price)", 31, value));
    POOL_DELETE_CLASS(agg);
}

TEST_F(NormalAggregatorDenseMapTest, testMultiFunctionAggregator_RT) {
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

    NormalAggregatorDenseInt64* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt64, keyExpr, 100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt64, keyExpr, 100, _pool);
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
void NormalAggregatorDenseMapTest::checkResult_RT(NormalAggregator<T_KEY, T_KEY,
        typename DenseMapTraits<T_KEY>::GroupMapType>* agg,
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

TEST_F(NormalAggregatorDenseMapTest, testAggregatorLess_RT) {
    HA3_LOG(DEBUG, "Begin Test!");

    _keyAttributeExpr = initKeyExpression<int64_t>("key");
    _funAttributeExpr = initFunExpression<int64_t>("fun");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price", vt_double,
                         (AttributeExpressionTyped<int32_t> *)_funAttributeExpr);

    NormalAggregatorDenseInt64* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt64, (AttributeExpressionTyped<int64_t> *)
               _keyAttributeExpr, 100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt64, (AttributeExpressionTyped<int64_t> *)
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

TEST_F(NormalAggregatorDenseMapTest, testAggregatorOneKeyCount) {
    HA3_LOG(DEBUG, "Begin Test!");
    _keyAttributeExpr = initKeyExpression<int64_t>("key");
    _funAttributeExpr = initFunExpression<float>("fun");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<float>, "price", vt_double,
            (AttributeExpressionTyped<float> *)_funAttributeExpr);
    ASSERT_TRUE(fun);

    NormalAggregatorDenseInt64* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt64, (AttributeExpressionTyped<int64_t> *)_keyAttributeExpr,
                             1, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt64, (AttributeExpressionTyped<int64_t> *)_keyAttributeExpr,
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

TEST_F(NormalAggregatorDenseMapTest, testAggregatorNoKeyCount) {
    HA3_LOG(DEBUG, "Begin Test!");
    _keyAttributeExpr = initKeyExpression<int64_t>("key");
    _funAttributeExpr = initFunExpression<float>("fun");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<float>, "price",vt_double,
            (AttributeExpressionTyped<float> *)_funAttributeExpr);
    ASSERT_TRUE(fun);

    NormalAggregatorDenseInt64* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt64,
                             (AttributeExpressionTyped<int64_t> *)_keyAttributeExpr,
                             0, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt64,
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

TEST_F(NormalAggregatorDenseMapTest, testAggregatorMoreKeyCount) {
    HA3_LOG(DEBUG, "Begin Test!");
    _funAttributeExpr = initFunExpression<float>("fun");
    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<float>, "price", vt_float,
            (AttributeExpressionTyped<float> *)_funAttributeExpr);

    _keyAttributeExpr = initFunExpression<int64_t>("uid");
    NormalAggregatorDenseInt64* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt64,
            (AttributeExpressionTyped<int64_t> *)_keyAttributeExpr,
            10, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt64,
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

TEST_F(NormalAggregatorDenseMapTest, testMultiValueKeyAggregator) {
    HA3_LOG(DEBUG, "Begin Test!");
    _keyAttributeExpr = initMultiValueKeyExpression<int64_t>("key");
    _funAttributeExpr = initFunExpression<float>("fun");

    AggregateFunction *funMax = POOL_NEW_CLASS(_pool, MaxAggregateFunction<float>, "price",vt_double,
            (AttributeExpressionTyped<float> *)_funAttributeExpr);
    ASSERT_TRUE(funMax);

    AggregateFunction *funCount = POOL_NEW_CLASS(_pool, CountAggregateFunction);

    ASSERT_TRUE(funCount);

    NormalAggregator<int64_t, MultiValueType<int64_t>, GroupInt64Map>* agg = NULL;
    if (_mode == AGG_NORMAL) {
        typedef NormalAggregator<int64_t, MultiValueType<int64_t>, GroupInt64Map> AggregatorType;
        agg = POOL_NEW_CLASS(_pool, AggregatorType,
                             (AttributeExpressionTyped<MultiInt64> *)_keyAttributeExpr,
                             1000, _pool);
    } else {
        typedef BatchAggregator<int64_t, MultiValueType<int64_t>, GroupInt64Map> AggregatorType;
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

TEST_F(NormalAggregatorDenseMapTest, testCollectResultWithNoDoc) {
    HA3_LOG(DEBUG, "begin test");
    _keyAttributeExpr = initKeyExpression<int32_t>("userid");
    _funAttributeExpr = initFunExpression<int32_t>("price");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price",vt_int32,
            (AttributeExpressionTyped<int32_t> *)_funAttributeExpr);
    ASSERT_TRUE(fun);
    NormalAggregatorDenseInt32 * agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg =POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32,
                            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                            100, _pool);
    } else {
        agg =POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32,
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

TEST_F(NormalAggregatorDenseMapTest, testCollectResult) {
    HA3_LOG(DEBUG, "begin test");
    _keyAttributeExpr = initKeyExpression<int32_t>("userid");
    _funAttributeExpr = initFunExpression<int32_t>("price");

    AggregateFunction *fun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int32_t>, "price",vt_int32,
            (AttributeExpressionTyped<int32_t> *)_funAttributeExpr);
    ASSERT_TRUE(fun);
    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32, (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32, (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
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

TEST_F(NormalAggregatorDenseMapTest, testEstimateResult) {
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

    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32, keyExpr, 100, _pool, 5, 2);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32, keyExpr, 100, _pool, 5, 2);

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
        ASSERT_EQ(uint32_t(5), agg->getAggregateCount());
    }

    AggregateResultsPtr aggResultsPtr = agg->collectAggregateResult();
    (*aggResultsPtr)[0]->constructGroupValueMap();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)4, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggExprValueCount());

    int32_t aggFunResult = 0;
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
    POOL_DELETE_CLASS(agg);
}

TEST_F(NormalAggregatorDenseMapTest, testCollectResultWithMultiFunction) {
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
    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32,
            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
            100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32,
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

TEST_F(NormalAggregatorDenseMapTest, testSameAggregatorFunctionInOneQueryMax) {
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


    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32,
                             (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32,
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

TEST_F(NormalAggregatorDenseMapTest, testSameAggregatorFunctionInOneQueryMin) {
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

    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32,
            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
            100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32,
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

TEST_F(NormalAggregatorDenseMapTest, testSameAggregatorFunctionInOneQuerySum) {
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

    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32,
                             (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32,
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

TEST_F(NormalAggregatorDenseMapTest, testSameAggregatorFunctionInOneQueryAvg) {
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

    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32,
            (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
            100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32,
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

TEST_F(NormalAggregatorDenseMapTest, testSameAggregatorFunctionInOneQueryDistinctCount) {
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

    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32,
                             (AttributeExpressionTyped<int32_t> *)_keyAttributeExpr,
                             100, _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32,
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

TEST_F(NormalAggregatorDenseMapTest, testMultiFunctionAggregatorWithSampler) {
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

    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32, keyExpr, 100, _pool, 5, 2);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32, keyExpr, 100, _pool, 5, 2);
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
    POOL_DELETE_CLASS(agg);
}

TEST_F(NormalAggregatorDenseMapTest, testAggregatorWithMaxSortCount) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<int> *expr
        = _fakeAttrExprMaker->createAttrExpr<int>("key", "0,1,1,2");
    _attrExprPool->addNeedDeleteExpr(expr);
    _keyAttributeExpr = expr;

    CountAggregateFunction *fun = POOL_NEW_CLASS(_pool, CountAggregateFunction);
    ASSERT_TRUE(fun);

    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32,
                             (AttributeExpressionTyped<int> *)_keyAttributeExpr, 1,
                             _pool, 0, 1, 2);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32,
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

TEST_F(NormalAggregatorDenseMapTest, testSubGroupKey) {
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

    NormalAggregatorDenseInt32* agg = NULL;
    if (_mode == AGG_NORMAL) {
        agg = POOL_NEW_CLASS(_pool, NormalAggregatorDenseInt32,
                             (AttributeExpressionTyped<int> *)keyExpr, 100,
                             _pool);
    } else {
        agg = POOL_NEW_CLASS(_pool, BatchAggregatorDenseInt32,
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

TEST_F(NormalAggregatorDenseMapTest, testGetIdxStr) {
    NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);

    for (int i = 0; i < 20; i++) {
        ASSERT_TRUE(normal.getIdxStr(i) == std::to_string(i));
    }
}

TEST_F(NormalAggregatorDenseMapTest, testGetCavaAggItemSize) {
    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        ASSERT_TRUE(normal.getCavaAggItemSize() == "16");
    }
    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        ASSERT_TRUE(normal.getCavaAggItemSize() == "24");
    }
    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        ASSERT_TRUE(normal.getCavaAggItemSize() == "32");
    }
    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        ASSERT_TRUE(normal.getCavaAggItemSize() == "40");
    }
    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        normal._funVector.push_back(POOL_NEW_CLASS(_pool, CountAggregateFunction));
        ASSERT_TRUE(normal.getCavaAggItemSize() == "48");
    }
}

TEST_F(NormalAggregatorDenseMapTest, testGetCavaAggRefName) {
    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, CountAggregateFunction);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggRefName(func, std::to_string(i));
            ASSERT_EQ(output, std::string("countRef") + std::to_string(i));
        }
    }

    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, SumAggregateFunction<int>, "sum", vt_int8, NULL);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggRefName(func, std::to_string(i));
            ASSERT_EQ(output, std::string("sumRef") + std::to_string(i));
        }
    }

    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, MinAggregateFunction<int>, "min", vt_int8, NULL);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggRefName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("minRef") + std::to_string(i));
        }
    }

    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int>, "max", vt_int8, NULL);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggRefName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("maxRef") + std::to_string(i));
        }
    }
}

TEST_F(NormalAggregatorDenseMapTest, testGetCavaAggExprName) {
    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, CountAggregateFunction);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggExprName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("countExpr") + std::to_string(i));
        }
    }

    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, SumAggregateFunction<int>, "sum", vt_int8, NULL);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggExprName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("sumExpr") + std::to_string(i));
        }
    }

    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, MinAggregateFunction<int>, "min", vt_int8, NULL);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggExprName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("minExpr") + std::to_string(i));
        }
    }

    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int>, "max", vt_int8, NULL);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggExprName(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("maxExpr") + std::to_string(i));
        }
    }
}

TEST_F(NormalAggregatorDenseMapTest, testGetCavaAggItemField) {
    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, CountAggregateFunction);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggItemField(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("count") + std::to_string(i));
        }
    }

    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, SumAggregateFunction<int>, "sum", vt_int8, NULL);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggItemField(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("sum") + std::to_string(i));
        }
    }

    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, MinAggregateFunction<int>, "min", vt_int8, NULL);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggItemField(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("min") + std::to_string(i));
        }
    }

    {
        NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
        auto func = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int>, "max", vt_int8, NULL);
        normal._funVector.push_back(func);
        for (int i = 0; i < 20; i++) {
            std::string output = normal.getCavaAggItemField(func, std::to_string(i));
            ASSERT_TRUE(output == std::string("max") + std::to_string(i));
        }
    }
}

TEST_F(NormalAggregatorDenseMapTest, testGetCavaAggExprGetFunc) {
    NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
#define CASE(vt_type, isMulti, ret, funName)                            \
    {                                                                   \
        std::string output;                                             \
        ASSERT_TRUE(normal.getCavaAggExprGetFunc(output, vt_type, isMulti) == ret); \
        if (ret) {                                                      \
            ASSERT_EQ(std::string(#funName), output);                   \
        }                                                               \
    }

    CASE(vt_int8, false, true, getInt8);
    CASE(vt_int16, false, true, getInt16);
    CASE(vt_int32, false, true, getInt32);
    CASE(vt_int64, false, true, getInt64);
    CASE(vt_uint8, false, true, getUInt8);
    CASE(vt_uint16, false, true, getUInt16);
    CASE(vt_uint32, false, true, getUInt32);
    CASE(vt_uint64, false, true, getUInt64);
    CASE(vt_float, false, true, getFloat);
    CASE(vt_double, false, true, getDouble);
    CASE(vt_string, false, true, getMChar);
    CASE(vt_unknown, false, false, unknow);

    CASE(vt_int8, true, true, getMInt8);
    CASE(vt_int16, true, true, getMInt16);
    CASE(vt_int32, true, true, getMInt32);
    CASE(vt_int64, true, true, getMInt64);
    CASE(vt_uint8, true, true, getMUInt8);
    CASE(vt_uint16, true, true, getMUInt16);
    CASE(vt_uint32, true, true, getMUInt32);
    CASE(vt_uint64, true, true, getMUInt64);
    CASE(vt_float, true, true, getMFloat);
    CASE(vt_double, true, true, getMDouble);
    CASE(vt_string, true, true, getMString);
    CASE(vt_unknown, true, false, unknow);

#undef CASE
}

TEST_F(NormalAggregatorDenseMapTest, testGetCavaAggRefSetFunc) {
    NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
#define CASE(vt_type, isMulti, ret, funName)                            \
    {                                                                   \
        std::string output;                                             \
        matchdoc::ValueType valueType;                                  \
        valueType.setBuiltinType(vt_type);                              \
        valueType.setMultiValue(isMulti);                               \
        valueType.setBuiltin();                                         \
        valueType.setStdType(true);                                     \
        ASSERT_TRUE(normal.getCavaAggRefSetFunc(output, valueType) == ret); \
        if (ret) {                                                      \
            ASSERT_TRUE(std::string(#funName) == output);               \
        }                                                               \
    }

    CASE(vt_int8, false, true, setInt8);
    CASE(vt_int16, false, true, setInt16);
    CASE(vt_int32, false, true, setInt32);
    CASE(vt_int64, false, true, setInt64);
    CASE(vt_uint8, false, true, setUInt8);
    CASE(vt_uint16, false, true, setUInt16);
    CASE(vt_uint32, false, true, setUInt32);
    CASE(vt_uint64, false, true, setUInt64);
    CASE(vt_float, false, true, setFloat);
    CASE(vt_double, false, true, setDouble);
    CASE(vt_string, false, true, setSTLString);
    CASE(vt_unknown, false, false, unknow);

    CASE(vt_int8, true, false, setMInt8);
    CASE(vt_int16, true, false, setMInt16);
    CASE(vt_int32, true, false, setMInt32);
    CASE(vt_int64, true, false, setMInt64);
    CASE(vt_uint8, true, false, setMUInt8);
    CASE(vt_uint16, true, false, setMUInt16);
    CASE(vt_uint32, true, false, setMUInt32);
    CASE(vt_uint64, true, false, setMUInt64);
    CASE(vt_float, true, false, setMFloat);
    CASE(vt_double, true, false, setMDouble);
    CASE(vt_string, true, false, setMString);
    CASE(vt_unknown, true, false, unknow);

#undef CASE
}

TEST_F(NormalAggregatorDenseMapTest, testGetCavaAggItemMapType) {
    NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
#define CASE(vt_type, ret, funName)                                     \
    {                                                                   \
        std::string output;                                             \
        ASSERT_TRUE(normal.getCavaAggItemMapType(output, vt_type) == ret); \
        if (ret) {                                                      \
            ASSERT_TRUE(std::string(#funName) == output);               \
        }                                                               \
    }

    CASE(vt_int8, true, ByteAggItemMap);
    CASE(vt_int16, true, ShortAggItemMap);
    CASE(vt_int32, true, IntAggItemMap);
    CASE(vt_int64, true, LongAggItemMap);
    CASE(vt_uint8, true, UByteAggItemMap);
    CASE(vt_uint16, true, UShortAggItemMap);
    CASE(vt_uint32, true, UIntAggItemMap);
    CASE(vt_uint64, true, ULongAggItemMap);
    CASE(vt_float, true, FloatAggItemMap);
    CASE(vt_double, true, DoubleAggItemMap);
    CASE(vt_string, true, MCharAggItemMap);
    CASE(vt_unknown, false, unknow);

#undef CASE
}

TEST_F(NormalAggregatorDenseMapTest, testGetCavaType) {
    NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);
#define CASE(vt_type, isMulti, ret, funName)                            \
    {                                                                   \
        std::string output;                                             \
        ASSERT_TRUE(normal.getCavaType(output, vt_type, isMulti) == ret); \
        if (ret) {                                                      \
            ASSERT_TRUE(std::string(#funName) == output);               \
        }                                                               \
    }

    CASE(vt_int8, false, true, byte);
    CASE(vt_int16, false, true, short);
    CASE(vt_int32, false, true, int);
    CASE(vt_int64, false, true, long);
    CASE(vt_uint8, false, true, ubyte);
    CASE(vt_uint16, false, true, ushort);
    CASE(vt_uint32, false, true, uint);
    CASE(vt_uint64, false, true, ulong);
    CASE(vt_float, false, true, float);
    CASE(vt_double, false, true, double);
    CASE(vt_string, false, true, MChar);
    CASE(vt_unknown, false, false, unknow);

    CASE(vt_int8, true, true, MInt8);
    CASE(vt_int16, true, true, MInt16);
    CASE(vt_int32, true, true, MInt32);
    CASE(vt_int64, true, true, MInt64);
    CASE(vt_uint8, true, true, MUInt8);
    CASE(vt_uint16, true, true, MUInt16);
    CASE(vt_uint32, true, true, MUInt32);
    CASE(vt_uint64, true, true, MUInt64);
    CASE(vt_float, true, true, MFloat);
    CASE(vt_double, true, true, MDouble);
    CASE(vt_string, true, true, MString);
    CASE(vt_unknown, true, false, unknow);

#undef CASE
}

TEST_F(NormalAggregatorDenseMapTest, testGetCavaMinMaxStr) {
    NormalAggregatorDenseInt32 normal(NULL, 1, _pool, 1, 2, 3);

#define CASE(bt_type, type, retValue)                                  \
    {                                                                   \
        std::string outputMin;                                          \
        std::string outputMax;                                          \
        matchdoc::ValueType vt;                                         \
        vt.setBuiltinType(bt_type);                                     \
        vt.setBuiltin();                                                \
        bool ret = normal.getCavaAggMinStr(outputMin, vt);              \
        ret |= normal.getCavaAggMaxStr(outputMax, vt);                  \
        ASSERT_TRUE(ret == retValue);                                   \
        if (ret) {                                                      \
            std::string suffix;                                         \
            if (bt_type == vt_float) {                                  \
                suffix = "F";                                           \
            } else if (bt_type == vt_double) {                          \
                suffix = "D";                                           \
            }                                                           \
            ASSERT_EQ(outputMin, std::to_string(util::NumericLimits<type>::min()) + suffix); \
            ASSERT_EQ(outputMax, std::to_string(util::NumericLimits<type>::max()) + suffix); \
        }                                                               \
    }

    CASE(vt_int8, int8_t, true);
    CASE(vt_int16, int16_t, true);
    CASE(vt_int32, int32_t, true);
    CASE(vt_uint8, uint8_t, true);
    CASE(vt_uint16, uint16_t, true);
    CASE(vt_float, float, true);
    CASE(vt_double, double, true);

    // CASE(vt_int64, int64_t, true);
    {
        std::string outputMin;
        std::string outputMax;
        matchdoc::ValueType vt;
        vt.setBuiltinType(vt_int64);
        vt.setBuiltin();
        bool ret = normal.getCavaAggMinStr(outputMin, vt);
        ret |= normal.getCavaAggMaxStr(outputMax, vt);
        ASSERT_TRUE(ret == true);
        if (ret) {
            ASSERT_EQ(outputMin, "-(1L<<63)");
            ASSERT_EQ(outputMax, std::to_string(util::NumericLimits<int64_t>::max()) + "L");
        }
    }
    //CASE(vt_uint32, uint32_t, true);
    {
        std::string outputMin;
        std::string outputMax;
        matchdoc::ValueType vt;
        vt.setBuiltinType(vt_uint32);
        vt.setBuiltin();
        bool ret = normal.getCavaAggMinStr(outputMin, vt);
        ret |= normal.getCavaAggMaxStr(outputMax, vt);
        ASSERT_TRUE(ret == true);
        if (ret) {
            ASSERT_EQ(outputMin, std::to_string(util::NumericLimits<uint32_t>::min()));
            ASSERT_EQ(outputMax, "-1");
        }
    }
    // CASE(vt_uint64, uint64_t, true);
    {
        std::string outputMin;
        std::string outputMax;
        matchdoc::ValueType vt;
        vt.setBuiltinType(vt_uint64);
        vt.setBuiltin();
        bool ret = normal.getCavaAggMinStr(outputMin, vt);
        ret |= normal.getCavaAggMaxStr(outputMax, vt);
        ASSERT_TRUE(ret == true);
        if (ret) {
            ASSERT_EQ(outputMin, std::to_string(util::NumericLimits<uint64_t>::min()));
            ASSERT_EQ(outputMax, "-1L");
        }
    }
    // test MultiChar
    {
        std::string outputMin;
        std::string outputMax;
        matchdoc::ValueType vt;
        vt.setBuiltinType(vt_string);
        vt.setBuiltin();
        bool ret = normal.getCavaAggMinStr(outputMin, vt);
        ASSERT_TRUE(ret == false);
        ret = normal.getCavaAggMaxStr(outputMax, vt);
        ASSERT_TRUE(ret == false);
    }

}

template <typename T>
AttributeExpressionTyped<T>* NormalAggregatorDenseMapTest::initKeyExpression(
        const string &name)
{
    vector<T> fakeData;
    makeFakeData_key(fakeData, DOC_NUM);//make fake key data
    AttributeExpressionTyped<T> *expr
        = _fakeAttrExprMaker->createAttrExpr<T>(name, StringUtil::toString(fakeData, ","));
    _attrExprPool->addNeedDeleteExpr(expr);
    return expr;
}

template <typename T>
AttributeExpressionTyped<T>*
NormalAggregatorDenseMapTest::initFunExpression(const string &name)
{
    vector<T> fakeData;
    makeFakeData_fun(fakeData, DOC_NUM);//make fake function data
    AttributeExpressionTyped<T> *expr
        = _fakeAttrExprMaker->createAttrExpr<T>(name, StringUtil::toString(fakeData, ","));
    _attrExprPool->addNeedDeleteExpr(expr);
    return expr;
}

template <typename T>
AttributeExpressionTyped<T>* NormalAggregatorDenseMapTest::initFunExpressionWithFakeData(
        const string &name , const vector<T> &fakeData)
{
    AttributeExpressionTyped<T> *expr =
        _fakeAttrExprMaker->createAttrExpr<T>(name, StringUtil::toString(fakeData, ","));
    _attrExprPool->addNeedDeleteExpr(expr);
    return expr;
}

template <typename T>
AttributeExpressionTyped<MultiValueType<T> >*
NormalAggregatorDenseMapTest::initMultiValueKeyExpression(const string &name)
{
    vector<vector<T> > fakeData;
    makeFakeMutiValueData_key(fakeData, DOC_NUM);//make fake key data

    AttributeExpressionTyped<MultiValueType<T> > *expr =
        _fakeAttrExprMaker->createMultiValueAttrExpr<T>(
                name, StringUtil::toString(fakeData, ",", "#"));

    _attrExprPool->addNeedDeleteExpr(expr);
    return expr;
}

void NormalAggregatorDenseMapTest::aggMatchDoc(int32_t docNum, Aggregator *agg) {
    agg->setMatchDocAllocator(_mdAllocator);
    for (int i = 0; i < docNum; i++) {
        matchdoc::MatchDoc scoreDoc = _mdAllocator->allocate((docid_t)i);
        _mdVector.push_back(scoreDoc);
        agg->aggregate(scoreDoc);
    }
    agg->endLayer(1.0f);
    agg->estimateResult(1.0);
}

void NormalAggregatorDenseMapTest::aggSubMatchDoc(
        const std::string &subDocNumStr, Aggregator *agg)
{
    vector<int> subDocNum;
    StringUtil::fromString(subDocNumStr, subDocNum, ",");
    agg->setMatchDocAllocator(_mdAllocator);
    int docNum = (int)subDocNum.size();
    docid_t subDocId = 0;
    for (int i = 0; i < docNum; i++) {
        matchdoc::MatchDoc doc = _mdAllocator->allocate((docid_t)i);
        for (int j = 0; j < subDocNum[i]; j++) {
            _mdAllocator->allocateSub(doc, subDocId++);
        }
        _mdVector.push_back(doc);
        agg->aggregate(doc);
    }
    agg->endLayer(1.0f);
    agg->estimateResult(1.0);
}

END_HA3_NAMESPACE(search);
