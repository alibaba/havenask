#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/AggSamplerConfigInfo.h>

using namespace std;
BEGIN_HA3_NAMESPACE(config);

class AggSamplerConfigInfoTest : public TESTBASE {
public:
    AggSamplerConfigInfoTest();
    ~AggSamplerConfigInfoTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, AggSamplerConfigInfoTest);


AggSamplerConfigInfoTest::AggSamplerConfigInfoTest() { 
}

AggSamplerConfigInfoTest::~AggSamplerConfigInfoTest() { 
}

void AggSamplerConfigInfoTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AggSamplerConfigInfoTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AggSamplerConfigInfoTest, testJsonizeWithoutBatchSampleMaxCount) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string jsonStr = "\
    {                                           \
        \"aggThreshold\" : 1000,                \
        \"aggBatchMode\" : true                 \
    }                                           \
";
    AggSamplerConfigInfo aggInfo;
    FromJsonString(aggInfo, jsonStr);
    ASSERT_EQ((uint32_t)1000, aggInfo.getAggThreshold());
    ASSERT_TRUE(aggInfo.getAggBatchMode());
    ASSERT_EQ((uint32_t)1000, aggInfo.getBatchSampleMaxCount());

    string jsonStr2 = ToJsonString(aggInfo);
    AggSamplerConfigInfo aggInfo2;
    FromJsonString(aggInfo2, jsonStr2);
    ASSERT_EQ((uint32_t)1000, aggInfo2.getAggThreshold());
    ASSERT_TRUE(aggInfo2.getAggBatchMode());
    ASSERT_EQ((uint32_t)1000, aggInfo2.getBatchSampleMaxCount());    
}

TEST_F(AggSamplerConfigInfoTest, testJsonizeWithBatchSampleMaxCount) {
    HA3_LOG(DEBUG, "Begin Test!");
    string jsonStr = "\
    {                                           \
        \"aggThreshold\" : 1000,                \
        \"batchSampleMaxCount\" : 2000,         \
        \"aggBatchMode\" : true                 \
    }                                           \
";
    AggSamplerConfigInfo aggInfo;
    FromJsonString(aggInfo, jsonStr);
    ASSERT_EQ((uint32_t)1000, aggInfo.getAggThreshold());
    ASSERT_TRUE(aggInfo.getAggBatchMode());
    ASSERT_EQ((uint32_t)2000, aggInfo.getBatchSampleMaxCount());

    string jsonStr2 = ToJsonString(aggInfo);
    AggSamplerConfigInfo aggInfo2;
    FromJsonString(aggInfo2, jsonStr2);
    ASSERT_EQ((uint32_t)1000, aggInfo2.getAggThreshold());
    ASSERT_TRUE(aggInfo2.getAggBatchMode());
    ASSERT_EQ((uint32_t)2000, aggInfo2.getBatchSampleMaxCount());    
}

END_HA3_NAMESPACE(config);

