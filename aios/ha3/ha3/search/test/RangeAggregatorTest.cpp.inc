#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <matchdoc/MatchDoc.h>
#include <vector>
#include <ha3/search/RangeAggregator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/framework/AttributeExpressionPool.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <ha3/search/MaxAggregateFunction.h>
#include <ha3/search/CountAggregateFunction.h>
#include <ha3/search/RangeAggregator.h>
#include <autil/StringUtil.h>
#include <ha3_sdk/testlib/search/FakeAttributeExpressionMaker.h>
#include <autil/MultiValueType.h>
#include <iostream>
#include <vector>
#include <memory>

using namespace std;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(search);


DefGroupType(int32_t, Int32GroupType);
typedef RangeAggregator<int32_t, int32_t, Int32GroupType> Int32RangeAggregatorType;
DefGroupType(int64_t, Int64GroupType);
typedef RangeAggregator<int64_t, int64_t, Int64GroupType> Int64RangeAggregatorType;
DefGroupType(double, DoubleGroupType);
typedef RangeAggregator<double, double, DoubleGroupType> DoubleRangeAggregatorType;

class TEST_CLASS_NAME : public TESTBASE {
public:
    TEST_CLASS_NAME();
    ~TEST_CLASS_NAME();
public:
    void setUp();
    void tearDown();

protected:
    void makeFakeData32(std::vector<int32_t> &fakeData, int count);
    void makeFakeData64(std::vector<int64_t> &fakeData, int count);
    void makeFakeDataMultiValue32(std::vector<std::vector<int32_t> > &fakeData, int count);
    void doAggregateMatchDocs(uint32_t aggThreshold = 0, uint32_t sampleStep = 1);
protected:
    common::Ha3MatchDocAllocator *_mdAllocator;

    std::vector<int32_t> _ranges32;
    std::vector<matchdoc::MatchDoc> _mdVector;
    Int32RangeAggregatorType * _agg;
    AttributeExpressionPool *_attrExprPool;
    autil::mem_pool::Pool *_pool;
    const int DOC_NUM;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, TEST_CLASS_NAME);


TEST_CLASS_NAME::TEST_CLASS_NAME() 
    : DOC_NUM(13)
{ 
    _mdAllocator = NULL;
    _agg = NULL;
    _attrExprPool = NULL;
    _pool = new autil::mem_pool::Pool(1024);
}

TEST_CLASS_NAME::~TEST_CLASS_NAME() {
    delete _pool;
}

void TEST_CLASS_NAME::makeFakeData32(vector<int32_t> &fakeData, int count) {
    for (int i = 0; i < count; i++) {
        fakeData.push_back((i * 10) % 50);
    }
}

void TEST_CLASS_NAME::makeFakeData64(vector<int64_t> &fakeData, int count) {
    for (int i = 0; i < count; i++) {
        fakeData.push_back((int64_t)i * 2);
    }
}

void TEST_CLASS_NAME::makeFakeDataMultiValue32(vector<vector<int32_t> > &fakeData, int count) {
    const int recordPerDoc = 3;
    
    for (int i = 0; i < count; i++) {
        vector<int32_t> records;
        for (int j = 0; j < recordPerDoc; j++) {
            records.push_back((j * 10 + i * 10) % 50);
        }
        fakeData.push_back(records);
    }
}

void TEST_CLASS_NAME::setUp() { 
    HA3_LOG(DEBUG, "setUp!");

    _attrExprPool = new AttributeExpressionPool;
}

void TEST_CLASS_NAME::doAggregateMatchDocs(uint32_t aggThreshold, uint32_t sampleStep) {
    _mdAllocator = new common::Ha3MatchDocAllocator(_pool);
    FakeAttributeExpressionMaker fakeAttrExprMaker(_pool);

    //construct Aggregator
    vector<int32_t> fakeData_key;
    makeFakeData32(fakeData_key, DOC_NUM);
    AttributeExpressionTyped<int32_t> *keyExpr
        = fakeAttrExprMaker.createAttrExpr<int32_t>("price", StringUtil::toString(fakeData_key, ","));
    _attrExprPool->addNeedDeleteExpr(keyExpr);
    vector<int32_t> range32;
    range32.push_back(20);
    range32.push_back(31);
    range32.push_back(35);
    _agg = POOL_NEW_CLASS(_pool, Int32RangeAggregatorType, keyExpr, range32, _pool, 1000,
            aggThreshold, sampleStep);

    //construct the attribute expression used in aggregate function
    vector<int64_t> fakeData_fun;
    makeFakeData64(fakeData_fun, DOC_NUM);
    AttributeExpressionTyped<int64_t> *funExpr
        = fakeAttrExprMaker.createAttrExpr<int64_t>("del", StringUtil::toString(fakeData_fun, ","));    
    _attrExprPool->addNeedDeleteExpr(funExpr);
    //construct aggregator function
    MaxAggregateFunction<int64_t> *maxAggFun
        = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int64_t>, "delivery", vt_int64, funExpr);
    CountAggregateFunction *countAggFun = POOL_NEW_CLASS(_pool, CountAggregateFunction);
                   
    _agg->addAggregateFunction(maxAggFun);
    _agg->addAggregateFunction(countAggFun);
    
    //initialize the MatchDoc
    for (int i = 0; i < DOC_NUM; i++) {
        matchdoc::MatchDoc matchDoc = _mdAllocator->allocate((docid_t)i);
        _mdVector.push_back(matchDoc);
        _agg->aggregate(matchDoc);
    }    
}

void TEST_CLASS_NAME::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    if (_agg) {
        POOL_DELETE_CLASS(_agg);
        _agg = NULL;
    }

    if (_mdAllocator) {
        for (vector<matchdoc::MatchDoc>::iterator it = _mdVector.begin();
             it != _mdVector.end(); it++)
        {
            _mdAllocator->deallocate(*it);
        }
        _mdVector.clear();

        delete _mdAllocator;
        _mdAllocator = NULL;
    }    

    DELETE_AND_SET_NULL(_attrExprPool);
}

TEST_F(TEST_CLASS_NAME, testGetResultSlab) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    doAggregateMatchDocs();

    const matchdoc::Reference<int32_t> *funResultRef32 
        = _agg->getFunResultReference<int32_t>("max(delivery)");
    ASSERT_TRUE(!funResultRef32);

    const matchdoc::Reference<int64_t> *funResultRef 
        = _agg->getFunResultReference<int64_t>("max(delivery)");
    ASSERT_TRUE(funResultRef);
    
    ASSERT_EQ(3u, _agg->getKeyCount());

    auto resultSlab = _agg->getResultMatchDoc((int32_t)10);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((int64_t)22, *(funResultRef->getPointer(resultSlab)));
    resultSlab = _agg->getResultMatchDoc(20);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((int64_t)24, *(funResultRef->getPointer(resultSlab)));

    resultSlab = _agg->getResultMatchDoc(31);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == resultSlab);

    resultSlab = _agg->getResultMatchDoc(3500);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != resultSlab);
    ASSERT_EQ((int64_t)18, *(funResultRef->getPointer(resultSlab)));
}

TEST_F(TEST_CLASS_NAME, testGetResultOfKey) {
    HA3_LOG(DEBUG, "Begin Test!");

    doAggregateMatchDocs();

    int64_t value;
    ASSERT_TRUE(_agg->getFunResultOfKey("max(delivery)", 10, value));

    ASSERT_EQ((int64_t)22, value);

    ASSERT_TRUE(_agg->getFunResultOfKey("max(delivery)", 20, value));
    ASSERT_EQ((int64_t)24, value);

    ASSERT_TRUE(!_agg->getFunResultOfKey("max(delivery)", 31, value));
    ASSERT_TRUE(!_agg->getFunResultOfKey("max(delivery)", 33, value));

    ASSERT_TRUE(_agg->getFunResultOfKey("max(delivery)", 35, value));
    ASSERT_EQ((int64_t)18, value);
}

TEST_F(TEST_CLASS_NAME, testKeyIterations) {
    HA3_LOG(DEBUG, "Begin Test!");

    doAggregateMatchDocs();
    
    const matchdoc::Reference<int64_t> *funResultRef 
        = _agg->getFunResultReference<int64_t>("max(delivery)");
    ASSERT_TRUE(funResultRef);

    map<int32_t,  matchdoc::MatchDoc> groupMap(_agg->begin(), _agg->end());
    map<int32_t,  matchdoc::MatchDoc>::const_iterator it =
        groupMap.begin();
    ASSERT_TRUE(it != groupMap.end());

    ASSERT_EQ(util::NumericLimits<int32_t>::min(), it->first);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != it->second);
    ASSERT_EQ((int64_t)22, *(funResultRef->getPointer(it->second)));

    it++;
    ASSERT_TRUE(it != groupMap.end());
    ASSERT_EQ(20, it->first);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != it->second);
    ASSERT_EQ((int64_t)24, *(funResultRef->getPointer(it->second)));

    it++;
    ASSERT_TRUE(it != groupMap.end());
    ASSERT_EQ(35, it->first);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != it->second);
    ASSERT_EQ((int64_t)18, *(funResultRef->getPointer(it->second)));

    it++;
    ASSERT_TRUE(it == groupMap.end());
}

TEST_F(TEST_CLASS_NAME, testCollectResult) {
    doAggregateMatchDocs();

    AggregateResultsPtr aggResultsPtr = _agg->collectAggregateResult();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    aggResultPtr->constructGroupValueMap();
    ASSERT_TRUE(aggResultPtr.get());
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)3, aggResultPtr->getAggExprValueCount());

    int64_t aggFunResult = 0;
    ASSERT_EQ((int64_t)22, 
                         aggResultPtr->getAggFunResult<int64_t>("-2147483648:20", 0, aggFunResult));
    ASSERT_EQ((int64_t)24,
                         aggResultPtr->getAggFunResult<int64_t>("20:31", 0, aggFunResult));
    ASSERT_EQ((int64_t)18,
                         aggResultPtr->getAggFunResult<int64_t>("35:2147483647", 0, aggFunResult));

    ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));
}


TEST_F(TEST_CLASS_NAME, testOneRangeInt32) { 
    HA3_LOG(DEBUG, "Begin Test!");

    vector<int32_t> ranges;
    Int32RangeAggregatorType agg(NULL, ranges, _pool);

    int32_t minValue = util::NumericLimits<int32_t>::min();
    int32_t maxValue = util::NumericLimits<int32_t>::max();
    ASSERT_EQ(minValue, agg.getGroupKeyValue(10));
    ASSERT_EQ(minValue, agg.getGroupKeyValue(-10000));
    ASSERT_EQ(minValue, agg.getGroupKeyValue(0));
    ASSERT_EQ(minValue, agg.getGroupKeyValue(maxValue));
}

TEST_F(TEST_CLASS_NAME, testOneRangeInt64) { 
    HA3_LOG(DEBUG, "Begin Test!");

    vector<int64_t> ranges;
    Int64RangeAggregatorType agg(NULL, ranges, _pool);

    int64_t minValue = util::NumericLimits<int64_t>::min();
    ASSERT_EQ(minValue, agg.getGroupKeyValue(345369872394296));
    ASSERT_EQ(minValue, agg.getGroupKeyValue(-1234));
    ASSERT_EQ(minValue, agg.getGroupKeyValue(0));
}

TEST_F(TEST_CLASS_NAME, testOneRangeDouble) { 
    HA3_LOG(DEBUG, "Begin Test!");

    vector<double> ranges;
    ranges.push_back(-10000.88);
    ranges.push_back(11.0);
    DoubleRangeAggregatorType agg(NULL, ranges, _pool);

    double minValue = util::NumericLimits<double>::max() * (-1);
    ASSERT_DOUBLE_EQ(-10000.88, 
                     agg.getGroupKeyValue(10.0));
    ASSERT_DOUBLE_EQ(-10000.88, 
                     agg.getGroupKeyValue(-10000.88));
    ASSERT_DOUBLE_EQ(minValue, 
                     agg.getGroupKeyValue(-111111.0));
    ASSERT_DOUBLE_EQ(11.0, 
                     agg.getGroupKeyValue(13.0));
}

TEST_F(TEST_CLASS_NAME, testAggregatorWithSampler) { 
    HA3_LOG(DEBUG, "Begin Test!");
    doAggregateMatchDocs(5, 2);
    _agg->endLayer(1.0f);
    AggregateResultsPtr aggResultsPtr = _agg->collectAggregateResult();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    aggResultPtr->constructGroupValueMap();
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)3, aggResultPtr->getAggExprValueCount());

    int32_t aggFunResult = 0;
    stringstream ss;
    ss << util::NumericLimits<int32_t>::min();
    string minValue = ss.str();
    aggResultPtr->getAggFunResult<int64_t>(minValue, 0, aggFunResult);

    ASSERT_EQ((int64_t)20, 
                         aggResultPtr->getAggFunResult<int64_t>("-2147483648:20",
                                 0, aggFunResult));
    ASSERT_EQ((int64_t)24, 
                         aggResultPtr->getAggFunResult<int64_t>("20:31", 
                                 0, aggFunResult));
    ASSERT_EQ((int64_t)8, 
                         aggResultPtr->getAggFunResult<int64_t>("35:2147483647", 
                                 0, aggFunResult));
    ASSERT_EQ(string("max"), 
                         aggResultPtr->getAggFunName(0));

    ASSERT_EQ((int64_t)6, 
                         aggResultPtr->getAggFunResult<int64_t>("-2147483648:20",
                                 1, aggFunResult));
    ASSERT_EQ((int64_t)6,
                         aggResultPtr->getAggFunResult<int64_t>("20:31", 
                                 1, aggFunResult));
    ASSERT_EQ((int64_t)1, 
                         aggResultPtr->getAggFunResult<int64_t>("35:2147483647", 
                                 1, aggFunResult));
    ASSERT_EQ(string("count"), aggResultPtr->getAggFunName(1));
}

TEST_F(TEST_CLASS_NAME, testAggregatorWithMultiValueInt32Key) { 
    HA3_LOG(DEBUG, "Begin Test!");
    _mdAllocator = new common::Ha3MatchDocAllocator(_pool);
    FakeAttributeExpressionMaker fakeAttrExprMaker(_pool);

    //construct Aggregator
    vector<vector<int32_t> > fakeData_key;
    makeFakeDataMultiValue32(fakeData_key, 8);
    /* Data is :
     [docid]:[key value]:[fun expr value]
     [0]:[ 0, 10, 20]:[0]
     [1]:[10, 20, 30]:[2]
     [2]:[20, 30, 40]:[4]
     [3]:[30, 40,  0]:[6]
     [4]:[40,  0, 10]:[8]
     [5]:[ 0, 10, 20]:[10]
     [6]:[10, 20, 30]:[12]
     [7]:[20, 30, 40]:[14]
    */

    AttributeExpressionTyped<MultiInt32> *keyExpr
        = fakeAttrExprMaker.createMultiValueAttrExpr<int32_t>("price",
                StringUtil::toString(fakeData_key, ",", "#"));
    vector<int32_t> range32;
    range32.push_back(20);
    range32.push_back(31);
    range32.push_back(35);
    _attrExprPool->addNeedDeleteExpr(keyExpr);


    typedef RangeAggregator<int32_t, MultiInt32, Int32GroupType> AggregatorType;
    AggregatorType *agg = 
        POOL_NEW_CLASS(_pool, AggregatorType, keyExpr, range32, 
                       _pool, 1000, 0, 1);
    
    //construct the attribute expression used in aggregate function
    vector<int64_t> fakeData_fun;
    makeFakeData64(fakeData_fun, 8);
    AttributeExpressionTyped<int64_t> *funExpr
        = fakeAttrExprMaker.createAttrExpr<int64_t>("del", StringUtil::toString(fakeData_fun, ","));    
    _attrExprPool->addNeedDeleteExpr(funExpr);
    
    //construct aggregator function
    MaxAggregateFunction<int64_t> *maxAggFun
        = POOL_NEW_CLASS(_pool, MaxAggregateFunction<int64_t>, 
                         "delivery", vt_int64, funExpr);

    CountAggregateFunction *countAggFun = POOL_NEW_CLASS(_pool, CountAggregateFunction);

    agg->addAggregateFunction(maxAggFun);
    agg->addAggregateFunction(countAggFun);
    
    //initialize the MatchDoc
    for (int i = 0; i < 8; i++) {
        matchdoc::MatchDoc matchDoc = _mdAllocator->allocate((docid_t)i);
        _mdVector.push_back(matchDoc);
        agg->aggregate(matchDoc);
    }    
    agg->endLayer(1.0f);

    AggregateResultsPtr aggResultsPtr = agg->collectAggregateResult();
    ASSERT_EQ((size_t)1, aggResultsPtr->size());

    AggregateResultPtr aggResultPtr = (*aggResultsPtr)[0];
    aggResultPtr->constructGroupValueMap();
    ASSERT_TRUE(aggResultPtr);
    ASSERT_EQ((uint32_t)2, aggResultPtr->getAggFunCount());
    ASSERT_EQ((uint32_t)3, aggResultPtr->getAggExprValueCount());

    int32_t aggFunResult = 0;
    stringstream ss;
    ss << util::NumericLimits<int32_t>::min();
    string minValue = ss.str();
    aggResultPtr->getAggFunResult<int64_t>(minValue, 0, aggFunResult);

    ASSERT_EQ((int64_t)12, 
                         aggResultPtr->getAggFunResult<int64_t>("-2147483648:20", 0, aggFunResult));
    ASSERT_EQ((int64_t)14,
                         aggResultPtr->getAggFunResult<int64_t>("20:31", 0, aggFunResult));
    ASSERT_EQ((int64_t)14,
                         aggResultPtr->getAggFunResult<int64_t>("35:2147483647", 0, aggFunResult));
    ASSERT_EQ(string("max"), aggResultPtr->getAggFunName(0));

    ASSERT_EQ((int64_t)9, 
                         aggResultPtr->getAggFunResult<int64_t>("-2147483648:20", 1, aggFunResult));
    ASSERT_EQ((int64_t)11,
                         aggResultPtr->getAggFunResult<int64_t>("20:31", 1, aggFunResult));
    ASSERT_EQ((int64_t)4,
                         aggResultPtr->getAggFunResult<int64_t>("35:2147483647", 1, aggFunResult));
    ASSERT_EQ(string("count"), aggResultPtr->getAggFunName(1));
    POOL_DELETE_CLASS(agg);
}

END_HA3_NAMESPACE(search);

