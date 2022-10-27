#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/SimpleDistinctBoostScorer.h>

BEGIN_HA3_NAMESPACE(rank);

class SimpleDistinctBoostScorerTest : public TESTBASE {
public:
    SimpleDistinctBoostScorerTest();
    ~SimpleDistinctBoostScorerTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, SimpleDistinctBoostScorerTest);

SimpleDistinctBoostScorerTest::SimpleDistinctBoostScorerTest() { 
}

SimpleDistinctBoostScorerTest::~SimpleDistinctBoostScorerTest() { 
}

void SimpleDistinctBoostScorerTest::setUp() { 
}

void SimpleDistinctBoostScorerTest::tearDown() { 
}

TEST_F(SimpleDistinctBoostScorerTest, testSimpleDistinctBoostScorer_1_1) { 
    HA3_LOG(DEBUG, "Begin Test!");
    SimpleDistinctBoostScorer dbs(1,1);
    float boost = 0.0;
    boost = dbs.calculateBoost(0, 0.1);    
    ASSERT_DOUBLE_EQ(1.0, boost);
    boost = dbs.calculateBoost(1, 0.1);
    ASSERT_DOUBLE_EQ(0.0, boost);
    boost = dbs.calculateBoost(2, 0.01234);
    ASSERT_DOUBLE_EQ(0.0, boost);
    boost = dbs.calculateBoost(3, 0.01234);    
    ASSERT_DOUBLE_EQ(0.0, boost);
}

TEST_F(SimpleDistinctBoostScorerTest, testSimpleDistinctBoostScorer_2_1) { 
    SimpleDistinctBoostScorer dbs(2,1);
    float boost = dbs.calculateBoost(0, 0.1);
    ASSERT_DOUBLE_EQ(2.0, boost);
    boost = dbs.calculateBoost(1, 0.1);
    ASSERT_DOUBLE_EQ(1.0, boost);
    boost = dbs.calculateBoost(2, 0.01234);
    ASSERT_DOUBLE_EQ(0.0, boost);
    boost = dbs.calculateBoost(3, 0.01234);
    ASSERT_DOUBLE_EQ(0.0, boost);
}


TEST_F(SimpleDistinctBoostScorerTest, testSimpleDistinctBoostScorer_2_2) { 
    SimpleDistinctBoostScorer dbs(2,2);
    float boost = 0.0;
    boost = dbs.calculateBoost(0, 0.01234);
    ASSERT_DOUBLE_EQ(2.0, boost);
    boost = dbs.calculateBoost(1, 0.1);
    ASSERT_DOUBLE_EQ(2.0, boost);
    boost = dbs.calculateBoost(2, 0.01234);
    ASSERT_DOUBLE_EQ(1.0, boost);
    boost = dbs.calculateBoost(3, 0.01234);
    ASSERT_DOUBLE_EQ(1.0, boost);
    boost = dbs.calculateBoost(4, 0.01234);
    ASSERT_DOUBLE_EQ(0.0, boost);
    boost = dbs.calculateBoost(5, 0.01234);
    ASSERT_DOUBLE_EQ(0.0, boost);
    boost = dbs.calculateBoost(6, 0.01234);
    ASSERT_DOUBLE_EQ(0.0, boost);

}

END_HA3_NAMESPACE(rank);

