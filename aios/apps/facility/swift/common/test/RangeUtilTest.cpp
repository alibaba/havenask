#include "swift/common/RangeUtil.h"

#include <cstddef>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "unittest/unittest.h"

using namespace std;
namespace swift {
namespace common {

class RangeUtilTest : public TESTBASE {
protected:
    void checkRangePairs(uint16_t partitionCount, const string &rangeStr);
    void checkRangePairs(uint16_t partitionCount, uint16_t rangeCount, const string &rangeStr);
    void checkPartionsIds(uint16_t partitionCount, uint16_t from, uint16_t to, const string &pids);
};

TEST_F(RangeUtilTest, testSplitPartitions) {
    checkRangePairs(0, "");
    checkRangePairs(3, "0_21845 21846_43690 43691_65535");
    checkRangePairs(7,
                    "0_9362 9363_18725 18726_28087 28088_37449 "
                    "37450_46811 46812_56173 56174_65535");
    checkRangePairs(10,
                    "0_6553 6554_13107 13108_19661 19662_26215 26216_32769"
                    " 32770_39323 39324_45876 45877_52429 "
                    "52430_58982 58983_65535");
    checkRangePairs(4,
                    2,
                    "0_8191 8192_16383 16384_24575 24576_32767 32768_40959"
                    " 40960_49151 49152_57343 57344_65535");
}

void RangeUtilTest::checkRangePairs(uint16_t partitionCount, const string &rangeStr) {
    RangeUtil range(partitionCount);
    vector<pair<uint16_t, uint16_t>> &partitions = range._partitions;
    EXPECT_EQ((size_t)partitionCount, partitions.size());

    const vector<string> &rangeVec = autil::StringUtil::split(rangeStr, " ");
    EXPECT_EQ(rangeVec.size(), partitions.size());
    for (size_t i = 0; i < partitions.size(); ++i) {
        string range =
            autil::StringUtil::toString(partitions[i].first) + "_" + autil::StringUtil::toString(partitions[i].second);
        EXPECT_EQ(rangeVec[i], range);
    }
}

void RangeUtilTest::checkRangePairs(uint16_t partitionCount, uint16_t rangeCount, const string &rangeStr) {
    RangeUtil range(partitionCount, rangeCount);
    vector<pair<uint16_t, uint16_t>> &partitions = range._mergeRanges;
    EXPECT_EQ((size_t)partitionCount * rangeCount, partitions.size());

    const vector<string> &rangeVec = autil::StringUtil::split(rangeStr, " ");
    EXPECT_EQ(rangeVec.size(), partitions.size());
    for (size_t i = 0; i < partitions.size(); ++i) {
        string range =
            autil::StringUtil::toString(partitions[i].first) + "_" + autil::StringUtil::toString(partitions[i].second);
        EXPECT_EQ(rangeVec[i], range);
    }
}

void RangeUtilTest::checkPartionsIds(uint16_t partitionCount, uint16_t from, uint16_t to, const string &pids) {
    RangeUtil range(partitionCount);
    vector<uint32_t> partitionIds = range.getPartitionIds(from, to);
    string actualPids;
    for (size_t i = 0; i < partitionIds.size(); ++i) {
        actualPids += autil::StringUtil::toString(partitionIds[i]) + " ";
    }
    autil::StringUtil::trim(actualPids);
    ASSERT_EQ(pids, actualPids);
}

TEST_F(RangeUtilTest, testGetPartitionIds) {
    checkPartionsIds(10, 1, 3, "0");
    checkPartionsIds(10, 0, 6554, "0 1");
    checkPartionsIds(10, 0, 6553, "0");
    checkPartionsIds(10, 0, 6567, "0 1");
    checkPartionsIds(10, 6567, 0, "");
    checkPartionsIds(10, 0, 65535, "0 1 2 3 4 5 6 7 8 9");
}

TEST_F(RangeUtilTest, testGetPartitionId) {
    {
        RangeUtil range(10);
        ASSERT_EQ((int32_t)0, range.getPartitionId(2));
        ASSERT_EQ((int32_t)1, range.getPartitionId(6554));
        ASSERT_EQ((int32_t)2, range.getPartitionId(19661));
    }
    {
        RangeUtil range(1);
        ASSERT_EQ((int32_t)0, range.getPartitionId(0));
        ASSERT_EQ((int32_t)0, range.getPartitionId(1233));
        ASSERT_EQ((int32_t)0, range.getPartitionId(65535));
    }
}

TEST_F(RangeUtilTest, testGetMergeHashId) {
    {
        RangeUtil range(4, 2);
        ASSERT_EQ((uint16_t)8191, range.getMergeHashId(0));
        ASSERT_EQ((uint16_t)8191, range.getMergeHashId(100));
        ASSERT_EQ((uint16_t)8191, range.getMergeHashId(8191));
        ASSERT_EQ((uint16_t)16383, range.getMergeHashId(8192));
        ASSERT_EQ((uint16_t)65535, range.getMergeHashId(65535));
    }
    {
        RangeUtil range(65536, 1);
        for (uint32_t i = 0; i < 65536; i++) {
            ASSERT_EQ((uint16_t)i, range.getMergeHashId(i));
        }
    }
    {
        RangeUtil range(32768, 2);
        for (uint32_t i = 0; i < 65536; i++) {
            ASSERT_EQ((uint16_t)i, range.getMergeHashId(i));
        }
    }
    {
        RangeUtil range(32768, 1);
        for (uint32_t i = 0; i < 65536; i++) {
            if (i % 2 == 0) {
                ASSERT_EQ((uint16_t)i + 1, range.getMergeHashId(i));
            } else {
                ASSERT_EQ((uint16_t)i, range.getMergeHashId(i));
            }
        }
    }
    {
        RangeUtil range(1, 65536);
        for (uint32_t i = 0; i < 65536; i++) {
            ASSERT_EQ((uint16_t)i, range.getMergeHashId(i));
        }
    }
    {
        RangeUtil range(2, 32768);
        for (uint32_t i = 0; i < 65536; i++) {
            ASSERT_EQ((uint16_t)i, range.getMergeHashId(i));
        }
    }
}

TEST_F(RangeUtilTest, testGetPartRange) {
    {
        RangeUtil range(1);
        uint32_t from = 0, to = 0;
        ASSERT_TRUE(range.getPartRange(0, from, to));
        ASSERT_EQ(0, from);
        ASSERT_EQ(65535, to);
        ASSERT_FALSE(range.getPartRange(1, from, to));
    }
    {
        RangeUtil range(2);
        uint32_t from = 0, to = 0;
        ASSERT_TRUE(range.getPartRange(0, from, to));
        ASSERT_EQ(0, from);
        ASSERT_EQ(32767, to);
        ASSERT_TRUE(range.getPartRange(1, from, to));
        ASSERT_EQ(32768, from);
        ASSERT_EQ(65535, to);
        ASSERT_FALSE(range.getPartRange(2, from, to));
    }
    {
        RangeUtil range(3);
        uint32_t from = 0, to = 0;
        ASSERT_TRUE(range.getPartRange(0, from, to));
        EXPECT_EQ(0, from);
        EXPECT_EQ(21845, to);
        EXPECT_TRUE(range.getPartRange(1, from, to));
        EXPECT_EQ(21846, from);
        EXPECT_EQ(43690, to);
        EXPECT_TRUE(range.getPartRange(2, from, to));
        EXPECT_EQ(43691, from);
        EXPECT_EQ(65535, to);
        EXPECT_FALSE(range.getPartRange(3, from, to));
        EXPECT_FALSE(range.getPartRange(4, from, to));
    }
}

TEST_F(RangeUtilTest, testExactlyInOnePart) {
    {
        RangeUtil range(1);
        uint32_t pid = 100;
        EXPECT_FALSE(range.exactlyInOnePart(0, 0, pid));
        EXPECT_FALSE(range.exactlyInOnePart(0, 32768, pid));
        EXPECT_FALSE(range.exactlyInOnePart(0, 65534, pid));
        EXPECT_FALSE(range.exactlyInOnePart(1, 65535, pid));
        EXPECT_TRUE(range.exactlyInOnePart(0, 65535, pid));
        EXPECT_EQ(0, pid);
    }
    {
        RangeUtil range(2);
        uint32_t pid = 100;
        EXPECT_FALSE(range.exactlyInOnePart(0, 32766, pid));
        EXPECT_FALSE(range.exactlyInOnePart(1, 32767, pid));
        EXPECT_FALSE(range.exactlyInOnePart(0, 32768, pid));
        EXPECT_FALSE(range.exactlyInOnePart(0, 65535, pid));
        EXPECT_EQ(100, pid);
        EXPECT_TRUE(range.exactlyInOnePart(0, 32767, pid));
        EXPECT_EQ(0, pid);
        EXPECT_FALSE(range.exactlyInOnePart(32767, 65535, pid));
        EXPECT_TRUE(range.exactlyInOnePart(32768, 65535, pid));
        EXPECT_EQ(1, pid);
    }
    {
        RangeUtil range(3); // 0~21845, 21846~43690, 43691~65535
        uint32_t pid = 100;
        EXPECT_FALSE(range.exactlyInOnePart(0, 65535, pid));
        EXPECT_FALSE(range.exactlyInOnePart(0, 21846, pid));
        EXPECT_TRUE(range.exactlyInOnePart(0, 21845, pid));
        EXPECT_EQ(0, pid);
        EXPECT_FALSE(range.exactlyInOnePart(21845, 43690, pid));
        EXPECT_TRUE(range.exactlyInOnePart(21846, 43690, pid));
        EXPECT_EQ(1, pid);
        EXPECT_FALSE(range.exactlyInOnePart(43690, 65535, pid));
        EXPECT_TRUE(range.exactlyInOnePart(43691, 65535, pid));
        EXPECT_EQ(2, pid);
    }
}

} // namespace common
} // namespace swift
