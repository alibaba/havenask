#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/AggregateSampler.h>

BEGIN_HA3_NAMESPACE(search);

class AggregateSamplerTest : public TESTBASE {
public:
    AggregateSamplerTest();
    ~AggregateSamplerTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, AggregateSamplerTest);


AggregateSamplerTest::AggregateSamplerTest() { 
}

AggregateSamplerTest::~AggregateSamplerTest() { 
}

void AggregateSamplerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AggregateSamplerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AggregateSamplerTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    AggregateSampler aggSampler(1000, 10);
    
    for (int i = 0; i < 1000; i ++) {
        ASSERT_TRUE(aggSampler.sampling());
        ASSERT_TRUE(!aggSampler.isBeginOfSample());
        ASSERT_EQ(uint32_t(1), aggSampler.getFactor());
    }

    ASSERT_TRUE(!aggSampler.sampling());
    ASSERT_TRUE(aggSampler.isBeginOfSample());
    ASSERT_EQ(uint32_t(10), aggSampler.getFactor());
    
    for (int i = 1; i < 10000; i++) {
        bool isSampled = i % 10 == 9;
        ASSERT_TRUE(isSampled == aggSampler.sampling());
        ASSERT_TRUE(!aggSampler.isBeginOfSample());
        ASSERT_EQ(uint32_t(10), aggSampler.getFactor());
    }
    ASSERT_EQ(uint32_t(2000), aggSampler.getAggregateCount());
}

END_HA3_NAMESPACE(search);

