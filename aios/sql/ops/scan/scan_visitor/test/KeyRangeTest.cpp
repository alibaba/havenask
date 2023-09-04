#include "sql/ops/scan/KeyRange.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "indexlib/table/normal_table/DimensionDescription.h"
#include "unittest/unittest.h"

namespace sql {

class KeyRangeTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
};

TEST_F(KeyRangeTest, testConstruct) {
    KeyRangeTyped<int64_t> keyRange("attr1");
    ASSERT_EQ("attr1", keyRange._attrName);
}

TEST_F(KeyRangeTest, testAdd) {
    KeyRangeTyped<int64_t> keyRange("attr1");
    keyRange.add(1, 10);
    ASSERT_EQ(1, keyRange._ranges.size());
    ASSERT_EQ(1, keyRange._ranges[0].first);
    ASSERT_EQ(10, keyRange._ranges[0].second);
}

TEST_F(KeyRangeTest, testTrimRange) {
    KeyRangeTyped<int64_t> keyRange("k1");
    {
        keyRange._ranges = {{1, 10}, {3, 5}, {8, 12}, {11, 15}, {30, 50}};
        keyRange.trimRange();
        std::vector<std::pair<int64_t, int64_t>> expected = {{1, 15}, {30, 50}};
        ASSERT_EQ(expected, keyRange._ranges);
    }
    {
        keyRange._ranges = {{1, 10}};
        keyRange.trimRange();
        std::vector<std::pair<int64_t, int64_t>> expected = {{1, 10}};
        ASSERT_EQ(expected, keyRange._ranges);
    }
    {
        keyRange._ranges = {};
        keyRange.trimRange();
        std::vector<std::pair<int64_t, int64_t>> expected = {};
        ASSERT_EQ(expected, keyRange._ranges);
    }
    {
        keyRange._ranges = {{1, 10}, {11, 15}, {8, 12}, {3, 5}};
        keyRange.trimRange();
        std::vector<std::pair<int64_t, int64_t>> expected = {{1, 15}};
        ASSERT_EQ(expected, keyRange._ranges);
    }
}

TEST_F(KeyRangeTest, testInsertAndTrim) {
    KeyRangeTyped<int64_t> keyRange1("k1");
    KeyRangeTyped<int64_t> keyRange2("k2");
    {
        keyRange1._ranges = {};
        keyRange2._ranges = {{1, 10}, {12, 15}};
        keyRange1.insertAndTrim(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {{1, 10}, {12, 15}};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        keyRange1._ranges = {{1, 10}, {12, 15}};
        keyRange2._ranges = {};
        keyRange1.insertAndTrim(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {{1, 10}, {12, 15}};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        keyRange1._ranges = {};
        keyRange2._ranges = {};
        keyRange1.insertAndTrim(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        keyRange1._ranges = {{1, 10}, {11, 15}, {40, 45}};
        keyRange2._ranges = {{3, 5}, {8, 12}, {30, 50}};
        keyRange1.insertAndTrim(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {{1, 15}, {30, 50}};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        KeyRangeTyped<int32_t> keyRange3("k3");
        keyRange1._ranges = {{1, 10}, {12, 15}};
        keyRange3._ranges = {{1, 50}};
        keyRange1.insertAndTrim(&keyRange3);
        std::vector<std::pair<int64_t, int64_t>> expected = {{1, 10}, {12, 15}};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
}

TEST_F(KeyRangeTest, testIntersectionRanges) {
    KeyRangeTyped<int64_t> keyRange1("k1");
    KeyRangeTyped<int64_t> keyRange2("k1");
    {
        keyRange1._ranges = {{12, 15}};
        keyRange2._ranges = {{1, 10}};
        keyRange1.intersectionRanges(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        keyRange1._ranges = {{12, std::numeric_limits<int64_t>::max()}};
        keyRange2._ranges = {{std::numeric_limits<int64_t>::min(), 10}};
        keyRange1.intersectionRanges(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        keyRange1._ranges = {{10, std::numeric_limits<int64_t>::max()}};
        keyRange2._ranges = {{std::numeric_limits<int64_t>::min(), 12}};
        keyRange1.intersectionRanges(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {{10, 12}};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        keyRange1._ranges = {};
        keyRange2._ranges = {{1, 10}};
        keyRange1.intersectionRanges(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        keyRange1._ranges = {{12, 15}};
        keyRange2._ranges = {};
        keyRange1.intersectionRanges(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        keyRange1._ranges = {};
        keyRange2._ranges = {};
        keyRange1.intersectionRanges(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        keyRange1._ranges = {{1, 10}, {13, 15}, {40, 45}};
        keyRange2._ranges = {{3, 5}, {8, 12}, {30, 50}};
        keyRange1.intersectionRanges(&keyRange2);
        std::vector<std::pair<int64_t, int64_t>> expected = {{3, 5}, {8, 10}, {40, 45}};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
    {
        KeyRangeTyped<int32_t> keyRange3("k3");
        keyRange1._ranges = {{1, 10}, {12, 15}};
        keyRange3._ranges = {{1, 50}};
        keyRange1.insertAndTrim(&keyRange3);
        std::vector<std::pair<int64_t, int64_t>> expected = {{1, 10}, {12, 15}};
        ASSERT_EQ(expected, keyRange1._ranges);
    }
}

TEST_F(KeyRangeTest, testConvertDimenDescription) {

    KeyRangeTyped<int64_t> keyRange("k1");
    {
        keyRange._ranges = {{1, 10}, {15, 15}, {20, 50}};
        auto dimen = keyRange.convertDimenDescription();
        ASSERT_TRUE(dimen);
        ASSERT_EQ("k1", dimen->name);
        ASSERT_EQ(11, dimen->values.size());
        std::vector<std::string> expected
            = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "15"};
        ASSERT_EQ(expected, dimen->values);
        ASSERT_EQ(1, dimen->ranges.size());
        ASSERT_EQ("20", dimen->ranges[0].from);
        ASSERT_EQ("50", dimen->ranges[0].to);
    }
    {
        keyRange._ranges = {};
        auto dimen = keyRange.convertDimenDescription();
        ASSERT_TRUE(dimen);
        ASSERT_EQ("k1", dimen->name);
        ASSERT_TRUE(dimen->values.empty());
        ASSERT_TRUE(dimen->ranges.empty());
    }
    {
        keyRange._ranges = {{std::numeric_limits<int64_t>::min(), 10}};
        auto dimen = keyRange.convertDimenDescription();
        ASSERT_TRUE(dimen);
        ASSERT_EQ("k1", dimen->name);
        ASSERT_TRUE(dimen->values.empty());
        ASSERT_TRUE(!dimen->ranges.empty());
    }
    {
        keyRange._ranges = {{std::numeric_limits<int64_t>::min(), -10}};
        auto dimen = keyRange.convertDimenDescription();
        ASSERT_TRUE(dimen);
        ASSERT_EQ("k1", dimen->name);
        ASSERT_TRUE(dimen->values.empty());
        ASSERT_TRUE(!dimen->ranges.empty());
    }
    {
        keyRange._ranges = {{-10, std::numeric_limits<int64_t>::max()}};
        auto dimen = keyRange.convertDimenDescription();
        ASSERT_TRUE(dimen);
        ASSERT_EQ("k1", dimen->name);
        ASSERT_TRUE(dimen->values.empty());
        ASSERT_TRUE(!dimen->ranges.empty());
    }
    {
        keyRange._ranges = {{10, std::numeric_limits<int64_t>::max()}};
        auto dimen = keyRange.convertDimenDescription();
        ASSERT_TRUE(dimen);
        ASSERT_EQ("k1", dimen->name);
        ASSERT_TRUE(dimen->values.empty());
        ASSERT_TRUE(!dimen->ranges.empty());
    }
}

} // namespace sql
