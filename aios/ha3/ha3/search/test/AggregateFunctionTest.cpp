#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/search/AggregateFunction.h>
#include <autil/MultiValueType.h>
#include <vector>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/MaxAggregateFunction.h>
#include <ha3/search/MinAggregateFunction.h>
#include <ha3/search/AvgAggregateFunction.h>
#include <ha3/search/CountAggregateFunction.h>
#include <ha3/search/SumAggregateFunction.h>
#include <ha3/search/DistinctCountAggregateFunction.h>
#include <ha3_sdk/testlib/search/FakeAttributeExpressionMaker.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/rank/MatchData.h>
#include <autil/StringUtil.h>
#include <autil/MultiValueType.h>
#include <memory>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(search);

class AggregateFunctionTest : public TESTBASE {
public:
    AggregateFunctionTest();
    ~AggregateFunctionTest();
public:
    void setUp();
    void tearDown();

protected:
    template <typename T> AttributeExpressionTyped<T>* initFunExpression();
    AttributeExpressionTyped<MultiInt32> *initFunExpressionDistinctCount();
    AttributeExpressionTyped<MultiString> *initFunExpressionMultiString();
    AttributeExpressionTyped<MultiChar> *initFunExpressionMultiChar();
    template <typename T>
    void makeFakeData_fun(std::vector<T> &fakeData, int count);
    template <typename T>
    void makeFakeDataMultiValue_fun(std::vector<std::vector<T> > &fakeData, int count);
    template <typename T>
    void makeFakeDataMultiValueDistinctCount(std::vector<std::vector<T> > &fakeData,
                                      int count);

  protected:
    void calculateMatchDocs(AggregateFunction *fun);
    void createInputMatchDocs(int docNum, AttributeExpression *_attributeExpr);
    void releaseInputMatchDocs();
protected:
    std::vector<matchdoc::MatchDoc> _mdVector;
    matchdoc::MatchDoc _outputSlab;
    matchdoc::MatchDocAllocator *_outputAllocator;
    common::Ha3MatchDocAllocator *_mdAllocator;

    autil::mem_pool::Pool *_pool;
    const int DOC_NUM;
    std::shared_ptr<FakeAttributeExpressionMaker> _fakeAttrExprMaker;
    AttributeExpressionTyped<int32_t> *_attributeExpr;
protected:
    HA3_LOG_DECLARE();
};

template <>
AttributeExpressionTyped<autil::MultiInt32>*
AggregateFunctionTest::initFunExpression<autil::MultiInt32>();

HA3_LOG_SETUP(search, AggregateFunctionTest);


AggregateFunctionTest::AggregateFunctionTest() : DOC_NUM(10000) {
    _outputSlab = matchdoc::INVALID_MATCHDOC;
    _outputAllocator = NULL;
    _mdAllocator =NULL;
    _pool = new autil::mem_pool::Pool(1024);
    _attributeExpr = NULL;
}

AggregateFunctionTest::~AggregateFunctionTest() {
    delete _pool;
}

void AggregateFunctionTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _outputAllocator = new matchdoc::MatchDocAllocator(_pool);
    _mdAllocator = new common::Ha3MatchDocAllocator(_pool);
    _outputSlab = matchdoc::INVALID_MATCHDOC;
    _fakeAttrExprMaker.reset(new FakeAttributeExpressionMaker(_pool));
    _attributeExpr = initFunExpression<int32_t>();
}

void AggregateFunctionTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");

    POOL_DELETE_CLASS(_attributeExpr);
    _fakeAttrExprMaker.reset();

    for (vector<matchdoc::MatchDoc>::iterator it = _mdVector.begin();
         it != _mdVector.end(); it++)
    {
        _mdAllocator->deallocate(*it);
    }
    _mdVector.clear();
    DELETE_AND_SET_NULL(_mdAllocator);

    if (matchdoc::INVALID_MATCHDOC != _outputSlab) {
        _outputAllocator->deallocate(_outputSlab);
        _outputSlab = matchdoc::INVALID_MATCHDOC;
    }
    DELETE_AND_SET_NULL(_outputAllocator);

    HA3_LOG(DEBUG, "tearDown:: _attributeExpr(%p)", _attributeExpr);
}

TEST_F(AggregateFunctionTest, testMaxAggregateFunction) {
    HA3_LOG(DEBUG, "Begin Test!");
    HA3_LOG(DEBUG, "_attributeExpr(%p)", _attributeExpr);

    MaxAggregateFunction<int32_t> fun("prd_count", vt_int32, _attributeExpr);

    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab);

    createInputMatchDocs(10, _attributeExpr);

    calculateMatchDocs(&fun);

    const matchdoc::Reference<int32_t> *varRef = fun.getReferenceTyped<int32_t>();
    ASSERT_EQ(9, *(varRef->getPointer(_outputSlab)));
    ASSERT_EQ(string("max(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int32, fun.getResultType());

}

TEST_F(AggregateFunctionTest, testMinAggregateFunction) {
    HA3_LOG(DEBUG, "Begin Test!");

    MinAggregateFunction<int32_t> fun("prd_count", vt_int32, _attributeExpr);
    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab);

    createInputMatchDocs(10, _attributeExpr);

    calculateMatchDocs(&fun);

    const matchdoc::Reference<int32_t> *varRef = fun.getReferenceTyped<int32_t>();
    ASSERT_EQ(0, *(varRef->getPointer(_outputSlab)));

    ASSERT_EQ(string("min(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int32, fun.getResultType());

}

TEST_F(AggregateFunctionTest, testAvgAggregateFunction) {
    HA3_LOG(DEBUG, "Begin Test!");

    AvgAggregateFunction<int32_t> fun("prd_count", vt_int32, _attributeExpr);
    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab);

    createInputMatchDocs(10, _attributeExpr);

    calculateMatchDocs(&fun);

    const matchdoc::Reference<int32_t> *varRef = fun.getReferenceTyped<int32_t>();
    ASSERT_EQ(4, *(varRef->getPointer(_outputSlab)));
    ASSERT_EQ(string("avg(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int32, fun.getResultType());

}

TEST_F(AggregateFunctionTest, testCountAggregateFunction) {
    HA3_LOG(DEBUG, "Begin Test!");

    CountAggregateFunction fun;
    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab);

    createInputMatchDocs(23, _attributeExpr);
    calculateMatchDocs(&fun);

    const matchdoc::Reference<int64_t> *varRef = fun.getReferenceTyped<int64_t>();
    ASSERT_EQ((int64_t)23, *(varRef->getPointer(_outputSlab)));

    ASSERT_EQ(string("count()"), fun.toString());
    ASSERT_EQ(vt_int64, fun.getResultType());
}

TEST_F(AggregateFunctionTest, testSumAggregateFunction) {
    HA3_LOG(DEBUG, "Begin Test!");

    SumAggregateFunction<int32_t> fun("prd_count", vt_int32, _attributeExpr);
    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab);

    createInputMatchDocs(10, _attributeExpr);
    calculateMatchDocs(&fun);
    const matchdoc::Reference<int32_t> *varRef = fun.getReferenceTyped<int32_t>();
    ASSERT_EQ(45, *(varRef->getPointer(_outputSlab)));

    ASSERT_EQ(string("sum(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int32, fun.getResultType());
}

TEST_F(AggregateFunctionTest, testDistinctCountAggregateFunction) {
    HA3_LOG(DEBUG, "Begin Test!");

    DistinctCountAggregateFunction<int32_t> fun("prd_count", _attributeExpr);
    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab, _pool);

    createInputMatchDocs(10000, _attributeExpr);
    calculateMatchDocs(&fun);

    ASSERT_EQ(string("distinct_count(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int64, fun.getResultType());
    int count = atoi(fun.getStrValue(_outputSlab).c_str());
    printf("----%s----", fun.getStrValue(_outputSlab).c_str());
    ASSERT_TRUE(count-10000 < 200 && 10000 - count < 200);
}

TEST_F(AggregateFunctionTest, testMaxAggregateFunctionWithMultiValueInt32) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiInt32> *attrExpr = initFunExpression<MultiInt32>();
    MaxAggregateFunction<int32_t, MultiInt32> fun("prd_count", vt_int32, attrExpr);

    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab);

    createInputMatchDocs(10, attrExpr);
    calculateMatchDocs(&fun);

    const matchdoc::Reference<int32_t> *varRef = fun.getReferenceTyped<int32_t>();
    ASSERT_EQ(11, *(varRef->getPointer(_outputSlab)));
    ASSERT_EQ(string("max(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int32, fun.getResultType());

    POOL_DELETE_CLASS(attrExpr);
}

TEST_F(AggregateFunctionTest, testMinAggregateFunctionWithMultiValueInt32) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiInt32> *attrExpr = initFunExpression<MultiInt32>();
    MinAggregateFunction<int32_t, MultiInt32> fun("prd_count", vt_int32, attrExpr);

    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab);

    createInputMatchDocs(10, attrExpr);
    calculateMatchDocs(&fun);

    const matchdoc::Reference<int32_t> *varRef = fun.getReferenceTyped<int32_t>();
    ASSERT_EQ(0, *(varRef->getPointer(_outputSlab)));
    ASSERT_EQ(string("min(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int32, fun.getResultType());

    POOL_DELETE_CLASS(attrExpr);
}

TEST_F(AggregateFunctionTest, testSumAggregateFunctionWithMultiValueInt32) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiInt32> *attrExpr = initFunExpression<MultiInt32>();
    SumAggregateFunction<int32_t, MultiInt32> fun("prd_count", vt_int32, attrExpr);
    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab);

    createInputMatchDocs(10, attrExpr);

    calculateMatchDocs(&fun);
    const matchdoc::Reference<int32_t> *varRef = fun.getReferenceTyped<int32_t>();
    ASSERT_EQ(165, *(varRef->getPointer(_outputSlab)));

    ASSERT_EQ(string("sum(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int32, fun.getResultType());

    POOL_DELETE_CLASS(attrExpr);
}

TEST_F(AggregateFunctionTest, testAvgAggregateFunctionWithMultiValueInt32) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiInt32> *attrExpr = initFunExpression<MultiInt32>();
    AvgAggregateFunction<int32_t, MultiInt32> fun("prd_count", vt_int32, attrExpr);

    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab);

    createInputMatchDocs(10, attrExpr);

    calculateMatchDocs(&fun);

    const matchdoc::Reference<int32_t> *varRef = fun.getReferenceTyped<int32_t>();
    ASSERT_EQ(5, *(varRef->getPointer(_outputSlab)));
    ASSERT_EQ(string("avg(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int32, fun.getResultType());

    POOL_DELETE_CLASS(attrExpr);
}

TEST_F(AggregateFunctionTest, testDistinctCountAggregateFunctionWithMultiValueInt32) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiInt32> *attrExpr = initFunExpressionDistinctCount();
    DistinctCountAggregateFunction<MultiInt32, int32_t> fun("prd_count", attrExpr);
    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab, _pool);
    createInputMatchDocs(10000, attrExpr);
    calculateMatchDocs(&fun);
    ASSERT_EQ(string("distinct_count(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int64, fun.getResultType());
    int count = atoi(fun.getStrValue(_outputSlab).c_str());
    printf("----%s----", fun.getStrValue(_outputSlab).c_str());
    ASSERT_TRUE(count-20000 < 200 && 20000 - count < 200);
    POOL_DELETE_CLASS(attrExpr);
}

TEST_F(AggregateFunctionTest, testDistinctCountAggregateFunctionWithMultiString) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiString> *attrExpr = initFunExpressionMultiString();
    DistinctCountAggregateFunction<MultiString, MultiChar> fun("prd_count", attrExpr);
    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab, _pool);
    createInputMatchDocs(10000, attrExpr);
    calculateMatchDocs(&fun);
    ASSERT_EQ(string("distinct_count(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int64, fun.getResultType());
    int count = atoi(fun.getStrValue(_outputSlab).c_str());
    printf("----%s----", fun.getStrValue(_outputSlab).c_str());
    ASSERT_TRUE(count-20000 < 200 && 20000 - count < 200);
    POOL_DELETE_CLASS(attrExpr);
}

TEST_F(AggregateFunctionTest, testDistinctCountAggregateFunctionWithMultiChar) {
    HA3_LOG(DEBUG, "Begin Test!");

    AttributeExpressionTyped<MultiChar> *attrExpr = initFunExpressionMultiChar();
    DistinctCountAggregateFunction<MultiChar> fun("prd_count", attrExpr);
    fun.declareResultReference(_outputAllocator);
    _outputSlab = _outputAllocator->allocate();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _outputSlab);
    fun.initFunction(_outputSlab, _pool);
    createInputMatchDocs(10000, attrExpr);
    calculateMatchDocs(&fun);
    ASSERT_EQ(string("distinct_count(prd_count)"), fun.toString());
    ASSERT_EQ(vt_int64, fun.getResultType());
    int count = atoi(fun.getStrValue(_outputSlab).c_str());
    printf("----%s----", fun.getStrValue(_outputSlab).c_str());
    ASSERT_TRUE(count-10000 < 200 && 10000 - count < 200);
    POOL_DELETE_CLASS(attrExpr);
}


void AggregateFunctionTest::calculateMatchDocs(AggregateFunction *fun) {
    for (vector<matchdoc::MatchDoc>::iterator it = _mdVector.begin();
         it != _mdVector.end(); it++)
    {
        fun->calculate(*it, _outputSlab);
    }
    fun->endLayer(1, 1.0f, _outputSlab);
}

void AggregateFunctionTest::createInputMatchDocs(int docNum,
        AttributeExpression *_attributeExpr)
{
    for (int i = 0; i < docNum; i++) {
        matchdoc::MatchDoc doc = _mdAllocator->allocate((docid_t)i);
        HA3_LOG(DEBUG, "DOCID: %d", doc.getDocId());
        _attributeExpr->evaluate(doc);
        _mdVector.push_back(doc);
    }
}

template <typename T>
AttributeExpressionTyped<T>* AggregateFunctionTest::initFunExpression()
{
    vector<T> fakeData;
    makeFakeData_fun(fakeData, DOC_NUM);
    AttributeExpressionTyped<T> *expr =
        _fakeAttrExprMaker->createAttrExpr<T>("uid", StringUtil::toString(fakeData, ","));
    return expr;
}

AttributeExpressionTyped<MultiChar>* AggregateFunctionTest::initFunExpressionMultiChar()
{
    vector<int32_t> fakeData;
    makeFakeData_fun(fakeData, DOC_NUM);
    AttributeExpressionTyped<MultiChar> *expr =
        _fakeAttrExprMaker->createStringAttrExpr("uid", StringUtil::toString(fakeData, ","));
    return expr;
}


template <typename T>
void AggregateFunctionTest::makeFakeData_fun(std::vector<T> &fakeData, int count)
{
    for (int i = 0; i < count; i++) {
        fakeData.push_back((T)i);
    }
}

AttributeExpressionTyped<MultiInt32>* AggregateFunctionTest::initFunExpressionDistinctCount()
{
    vector<vector<int32_t> > fakeData;
    makeFakeDataMultiValueDistinctCount(fakeData, 10000);
    AttributeExpressionTyped<MultiInt32> *expr =
        _fakeAttrExprMaker->createMultiValueAttrExpr<int32_t>("uid_multi_value",
                StringUtil::toString(fakeData, ",", "#"));
    return expr;
}

AttributeExpressionTyped<MultiString>* AggregateFunctionTest::initFunExpressionMultiString()
{
    vector<vector<int32_t> > fakeData;
    makeFakeDataMultiValueDistinctCount(fakeData, 10000);
    AttributeExpressionTyped<MultiString> *expr =
        _fakeAttrExprMaker->createMultiValueAttrExpr<MultiChar>("uid_multi_string",
                StringUtil::toString(fakeData, ",", "#"));
    return expr;
}

template <>
AttributeExpressionTyped<MultiInt32>* AggregateFunctionTest::initFunExpression<MultiInt32>()
{
    vector<vector<int32_t> > fakeData;
    makeFakeDataMultiValue_fun(fakeData, 10);
    AttributeExpressionTyped<MultiInt32> *expr =
        _fakeAttrExprMaker->createMultiValueAttrExpr<int32_t>("uid_multi_value",
                StringUtil::toString(fakeData, ",", "#"));
    return expr;
}

template <typename T>
void AggregateFunctionTest::makeFakeDataMultiValue_fun(vector<vector<T> > &fakeData, int count)
{
    const int recordsPerDoc = 3;
    for (int i = 0; i < count; i++) {
        vector<T> record;
        for (int j = 0; j < recordsPerDoc; j++) {
            record.push_back((T)(i + j));
        }
        fakeData.push_back(record);
    }
}

template <typename T>
void AggregateFunctionTest::makeFakeDataMultiValueDistinctCount(vector<vector<T> > &fakeData, int count)
{
    const int recordsPerDoc = 3;
    for (int i = 0; i < 2 * count; i += 2) {
        vector<T> record;
        for (int j = 0; j < recordsPerDoc; j++) {
            record.push_back((T)(i + j));
        }
        fakeData.push_back(record);
    }
}

END_HA3_NAMESPACE(search);
