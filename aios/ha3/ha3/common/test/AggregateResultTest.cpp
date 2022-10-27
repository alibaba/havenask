#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/AggregateResult.h>
#include <autil/mem_pool/Pool.h>
#include <autil/legacy/any.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include <string>
#include <ha3/common/AggregateResult.h>

using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace std;

USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(common);

class AggregateResultTest : public TESTBASE {
public:
    AggregateResultTest();
    ~AggregateResultTest();
public:
    void setUp();
    void tearDown();
protected:
    void addAggFunResult(AggregateResult* aggResult);
protected:
    autil::mem_pool::Pool _pool;
    AggregateResult *_aggResult;
    matchdoc::MatchDocAllocator *_allocator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AggregateResultTest);


AggregateResultTest::AggregateResultTest() { 
    _allocator = new matchdoc::MatchDocAllocator(&_pool);
}

AggregateResultTest::~AggregateResultTest() { 
    delete _allocator;
}

void AggregateResultTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _aggResult = new AggregateResult();
    addAggFunResult(_aggResult);
}

void AggregateResultTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _aggResult;
    _aggResult = NULL;
}

TEST_F(AggregateResultTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    ASSERT_TRUE(_aggResult);
    ASSERT_EQ(string("max"), _aggResult->getAggFunName(0));
                //    ASSERT_EQ(vt_int, _aggResult->getAggFunResultType(0));
    ASSERT_EQ((uint32_t)1, _aggResult->getAggFunCount());

    //only the 'uid1' value for 'uid' field.
    ASSERT_EQ((uint32_t)1, _aggResult->getAggExprValueCount());

    //test get aggregation function result through 'getAggFunResult' interface.
    int32_t defaultValue = 0;
    ASSERT_EQ(99, _aggResult->getAggFunResult("uid1", 0, defaultValue));
    ASSERT_EQ(0, _aggResult->getAggFunResult("uid222", 0, defaultValue));

    const matchdoc::MatchDoc aggFunResults = _aggResult->getAggFunResults("uid1");
    ASSERT_TRUE(aggFunResults != matchdoc::INVALID_MATCHDOC);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == _aggResult->getAggFunResults("uid222"));

    //test get aggregate function result through VariableReference and VariableSlab
    const matchdoc::Reference<int32_t>* refer 
        = _aggResult->getAggFunResultRefer<int32_t>(0);
    ASSERT_TRUE(refer);
    auto aggFunResult = refer->get(aggFunResults);
    ASSERT_EQ(99, aggFunResult);

    //there is only one aggregate function.
    ASSERT_TRUE(!_aggResult->getAggFunResultRefer<int32_t>(1));
}

TEST_F(AggregateResultTest, testSerializeAndDeserializeWithoutJson) {
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(_aggResult);
    
    autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &_pool);
    _aggResult->serialize(dataBuffer);

    AggregateResult aggResult2;
    aggResult2.deserialize(dataBuffer, &_pool);
    aggResult2.constructGroupValueMap();
    
    ASSERT_EQ(string("PAGERANK"), aggResult2.getGroupExprStr());
    ASSERT_EQ(string("max"), aggResult2.getAggFunName(0));
    ASSERT_EQ(string("max(id)"), aggResult2.getAggFunDesStr(0));
                  //    ASSERT_EQ(vt_int, aggResult2.getAggFunResultType(0));
    ASSERT_EQ((uint32_t)1, aggResult2.getAggFunCount());

    //only the 'uid1' value for 'uid' field.
    ASSERT_EQ((uint32_t)1, aggResult2.getAggExprValueCount());

    //test get aggregation function result through 'getAggFunResult' interface.
    int32_t aggFunResult = 0;
    ASSERT_EQ(99, aggResult2.getAggFunResult("uid1", 0, aggFunResult));
    ASSERT_EQ(0, aggResult2.getAggFunResult("uid222", 0, aggFunResult));

    const matchdoc::MatchDoc aggFunResults = aggResult2.getAggFunResults("uid1");
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != aggFunResults);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == aggResult2.getAggFunResults("uid222"));

    //test get aggregate function result through VariableReference and VariableSlab
    const matchdoc::Reference<int32_t>* refer 
        = aggResult2.getAggFunResultRefer<int32_t>(0);
    ASSERT_TRUE(refer);
    aggFunResult = refer->get(aggFunResults);
    ASSERT_EQ(99, aggFunResult);

    //there is only one aggregate function.
    ASSERT_TRUE(!aggResult2.getAggFunResultRefer<int32_t>(1));
}

TEST_F(AggregateResultTest, testSerializeAndDeserializeEmptyAggregateResultWithoutJson) {
    HA3_LOG(DEBUG, "Begin Test!");


    autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &_pool);
    AggregateResult aggResult;
    
    aggResult.serialize(dataBuffer);

    AggregateResult aggResult2;
    aggResult2.deserialize(dataBuffer, &_pool);
    
    ASSERT_TRUE(!aggResult2.getMatchDocAllocator());
    ASSERT_EQ((uint32_t)0, aggResult2.getAggExprValueCount());
    ASSERT_EQ((uint32_t)0, aggResult2.getAggFunCount());
}

void AggregateResultTest::addAggFunResult(AggregateResult* aggResult) {
    matchdoc::MatchDocAllocatorPtr allocatorPtr(new matchdoc::MatchDocAllocator(&_pool));
    matchdoc::Reference<string> *groupKeyRef
        = allocatorPtr->declare<string>(common::GROUP_KEY_REF, SL_CACHE);
    matchdoc::Reference<int32_t> *reference
        = allocatorPtr->declare<int32_t>("maxFunResult", SL_CACHE);

    //construct a AggFunResults(VariableSlab) and add into AggregateResult.
    matchdoc::MatchDoc aggFunResults = allocatorPtr->allocate();
    //resultValue is the function result value.
    groupKeyRef->set(aggFunResults, "uid1");
    int32_t resultValue = 99;
    reference->set(aggFunResults, resultValue);
    aggResult->setMatchDocAllocator(allocatorPtr);

    aggResult->addAggFunResults(aggFunResults);
    aggResult->addAggFunName("max");
    aggResult->addAggFunParameter("id");

    aggResult->setGroupExprStr("PAGERANK");
    aggResult->constructGroupValueMap();
}


END_HA3_NAMESPACE(common);

