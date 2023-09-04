#include "indexlib/framework/SegmentMetrics.h"

#include "unittest/unittest.h"

using namespace std;

namespace indexlib::framework {

class SegmentMetricsTest : public TESTBASE
{
public:
    SegmentMetricsTest() = default;
    ~SegmentMetricsTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(SegmentMetricsTest, TestSimpleProcess)
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
    // ASSERT_ANY_THROW(metrics.Get<uint32_t>("group_1", "key1", value));
    float value1;
    ASSERT_TRUE(metrics.Get<float>("group_1", "key1", value1));
    ASSERT_EQ((float)8, value1);
}

TEST_F(SegmentMetricsTest, TestMerge)
{
    SegmentMetrics metrics1;
    metrics1.Set<uint32_t>("group_1", "key", 4);
    metrics1.Set<uint32_t>("group_1", "key_count", 1);
    metrics1.SetKeyCount(1);

    SegmentMetrics metrics2;
    metrics2.Set<uint32_t>("group_2", "key", 8);
    metrics2.Set<uint32_t>("group_2", "key_count", 1);
    metrics2.SetKeyCount(1);

    SegmentMetrics metrics;
    ASSERT_TRUE(metrics.MergeSegmentMetrics(metrics1));
    uint32_t value;
    size_t keyCount;
    ASSERT_TRUE(metrics.Get<uint32_t>("group_1", "key", value));
    ASSERT_EQ((uint32_t)4, value);
    ASSERT_TRUE(metrics.Get<uint32_t>("group_1", "key_count", value));
    ASSERT_EQ((uint32_t)1, value);
    ASSERT_TRUE(metrics.GetKeyCount(keyCount));
    ASSERT_EQ((size_t)1, keyCount);
    ASSERT_FALSE(metrics.Get<uint32_t>("group_2", "key", value));

    ASSERT_TRUE(metrics.MergeSegmentMetrics(metrics2));
    ASSERT_TRUE(metrics.Get<uint32_t>("group_1", "key", value));
    ASSERT_EQ((uint32_t)4, value);
    ASSERT_TRUE(metrics.Get<uint32_t>("group_1", "key_count", value));
    ASSERT_EQ((uint32_t)1, value);
    ASSERT_TRUE(metrics.Get<uint32_t>("group_2", "key", value));
    ASSERT_EQ((uint32_t)8, value);
    ASSERT_TRUE(metrics.Get<uint32_t>("group_2", "key_count", value));
    ASSERT_EQ((uint32_t)1, value);
    ASSERT_TRUE(metrics.GetKeyCount(keyCount));
    ASSERT_EQ((size_t)2, keyCount);
}

TEST_F(SegmentMetricsTest, TestToString)
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
} // namespace indexlib::framework
