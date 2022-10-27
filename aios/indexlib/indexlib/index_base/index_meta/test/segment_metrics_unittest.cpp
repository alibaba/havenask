#include "indexlib/index_base/index_meta/test/segment_metrics_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentMetricsTest);

SegmentMetricsTest::SegmentMetricsTest()
{
}

SegmentMetricsTest::~SegmentMetricsTest()
{
}

void SegmentMetricsTest::CaseSetUp()
{
}

void SegmentMetricsTest::CaseTearDown()
{
}

void SegmentMetricsTest::TestSimpleProcess()
{
    SegmentMetrics metrics;
    metrics.Set<uint32_t>("group_1", "key", 4);

    uint32_t value;
    ASSERT_TRUE(metrics.Get<uint32_t>("group_1", "key", value));
    ASSERT_EQ((uint32_t)4, value);

    // repeated set
    metrics.Set<uint32_t>("group_1", "key", 8);
    ASSERT_TRUE(metrics.Get<uint32_t>("group_1", "key", value));
    ASSERT_EQ((uint32_t)8, value);

    // not exist param
    ASSERT_FALSE(metrics.Get<uint32_t>("group_1", "key_no_exist", value));

    // not exist group
    ASSERT_FALSE(metrics.Get<uint32_t>("group_no_exist", "key", value));

    // repeated set group, with wrong type
    metrics.Set<float>("group_1", "key1", 8);
    ASSERT_ANY_THROW(metrics.Get<uint32_t>("group_1", "key1", value));
    float value1;
    ASSERT_TRUE(metrics.Get<float>("group_1", "key1", value1));
    ASSERT_EQ((float)8, value1);
}

void SegmentMetricsTest::TestToString()
{
    SegmentMetrics metrics;
    metrics.Set<uint32_t>("group_1", "key", 4);
    uint32_t value;
    ASSERT_TRUE(metrics.Get<uint32_t>("group_1", "key", value));
    ASSERT_EQ((uint32_t)4, value);

    string jsonStr = metrics.ToString();
    SegmentMetrics metrics2;
    metrics2.FromString(jsonStr);
    ASSERT_TRUE(metrics2.Get<uint32_t>("group_1", "key", value));
    ASSERT_EQ((uint32_t)4, value);
}

IE_NAMESPACE_END(index_base);

