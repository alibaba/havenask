#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/AggResultReader.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/common/Result.h>
#include <ha3/common/test/ResultConstructor.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class AggResultReaderTest : public TESTBASE {
public:
    AggResultReaderTest();
    ~AggResultReaderTest();
public:
    void setUp();
    void tearDown();
protected:
    void prepareAggResults();
protected:
    autil::mem_pool::Pool _pool;
    AggregateResultsPtr _aggResults;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AggResultReaderTest);


AggResultReaderTest::AggResultReaderTest() { 
}

AggResultReaderTest::~AggResultReaderTest() { 
}

void AggResultReaderTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    prepareAggResults();
}

void AggResultReaderTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AggResultReaderTest, testGetAggResultWithAggSingleFunResult) { 
    HA3_LOG(DEBUG, "Begin Test!");
    AggResultReader reader(_aggResults);
    
    AggSingleFunResultReader<int64_t> singleResult;
    map<string, int64_t> values;
    
    ASSERT_TRUE(reader.getAggSingleFunResultReader("group1", "sum(id1 ) ",singleResult));
    ASSERT_EQ((size_t)2, singleResult.size());
    for (AggSingleFunResultReader<int64_t>::Iterator it = singleResult.begin();
         it != singleResult.end(); ++it) 
    {
        values[it->getKeyValue()] = it->getFunValue();
    }

    ASSERT_EQ((size_t)2, values.size());
    ASSERT_EQ((int64_t)11, values["key1"]);
    ASSERT_EQ((int64_t)111, values["key2"]);

    values.clear();
    ASSERT_TRUE(reader.getAggSingleFunResultReader("group1", "count()",singleResult));
    ASSERT_EQ((size_t)2, singleResult.size());
    for (AggSingleFunResultReader<int64_t>::Iterator it = singleResult.begin();
         it != singleResult.end(); ++it) 
    {
        values[it->getKeyValue()] = it->getFunValue();
    }

    ASSERT_EQ((size_t)2, values.size());
    ASSERT_EQ((int64_t)22, values["key1"]);
    ASSERT_EQ((int64_t)222, values["key2"]);

    values.clear();
    ASSERT_TRUE(reader.getAggSingleFunResultReader(" group1  +  2 *  group2 ", 
                    "min ( 1 + 2 *id)", singleResult));
    ASSERT_EQ((size_t)2, singleResult.size());
    for (AggSingleFunResultReader<int64_t>::Iterator it = singleResult.begin();
         it != singleResult.end(); ++it) 
    {
        values[it->getKeyValue()] = it->getFunValue();
    }
    ASSERT_EQ((size_t)2, values.size());
    ASSERT_EQ((int64_t)34, values["key3"]);
    ASSERT_EQ((int64_t)345, values["key4"]);
    
    AggSingleFunResultReader<uint64_t> wrongTypeSingleResult;
    ASSERT_TRUE(!reader.getAggSingleFunResultReader("group1", "count()", wrongTypeSingleResult));
}

TEST_F(AggResultReaderTest, testGetAggResult) {
    HA3_LOG(DEBUG, "Begin Test!");

    AggResultReader reader(_aggResults);
    
    int64_t value;
    ASSERT_TRUE(reader.getAggResult("group1", "sum(id1 ) ", "key1", value));
    ASSERT_EQ((int64_t)11, value);
    ASSERT_TRUE(reader.getAggResult("group1", "sum(id1 ) ", "key2", value));
    ASSERT_EQ((int64_t)111, value);

    ASSERT_TRUE(!reader.getAggResult("group1", "sum(id1) ", "key3", value));
    ASSERT_TRUE(!reader.getAggResult("group1", "sum(id1 + 1) ", "key1", value));
    ASSERT_TRUE(!reader.getAggResult("group1 + 2", "sum(id1) ", "key1", value));

    uint64_t wrongTypeValue;
    ASSERT_TRUE(!reader.getAggResult("group1", "sum(id1 ) ", "key1", wrongTypeValue));
}

TEST_F(AggResultReaderTest, testGetAggResultWithEmpty) {
    HA3_LOG(DEBUG, "Begin Test!");

    AggregateResultsPtr emptyAggResults;
    AggResultReader reader1(emptyAggResults);
    AggResultReader reader2(_aggResults);
    int64_t value;
    
    ASSERT_TRUE(!reader1.getAggResult("group1", "sum(id1) ", "key1", value));
    ASSERT_TRUE(!reader2.getAggResult("", "sum(id1) ", "key1", value));
    ASSERT_TRUE(!reader2.getAggResult("group1", "", "key1", value));    
}

void AggResultReaderTest::prepareAggResults() {
    ResultConstructor resultConstructor;
    ResultPtr resultPtr(new Result);

    resultConstructor.fillAggregateResult(
            resultPtr, 
            "sum,count,min,max",
            "key1, 11, 22, 33, 44 #key2, 111, 222, 333, 444",
            &_pool,
            "group1",
            "id1 ,, id2,id3");
    
    resultConstructor.fillAggregateResult(
            resultPtr, 
            "sum,count,min,max",
            "key3, 12, 23, 34, 45 #key4, 123, 234, 345, 456",
            &_pool,
            "(group1+(2*group2))",
            "(id+1),,(1+(2*id)),id");

    _aggResults.reset(new AggregateResults(resultPtr->getAggregateResults()));
}

END_HA3_NAMESPACE(common);

