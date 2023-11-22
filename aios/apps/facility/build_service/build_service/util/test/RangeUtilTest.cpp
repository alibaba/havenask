#include "build_service/util/RangeUtil.h"

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

namespace build_service { namespace util {

class RangeUtilTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() {}
    void tearDown() {}

protected:
    void checkSplitRange(uint32_t rangeFrom, uint32_t rangeTo, uint32_t partitionCount,
                         const std::string& expectRangeStr = "", uint32_t parallelNum = 1);

    void checkGetParallelInfoByRange(uint32_t rangeFrom, uint32_t rangeTo, uint32_t partitionCount,
                                     uint32_t parallelCount);
    void checkSplitRangeBySwiftPartition(uint32_t rangeFrom, uint32_t rangeTo, uint32_t totalSwiftPartitionCount,
                                         uint32_t parallelNum, const std::string& expectedRangeStr);
};

TEST_F(RangeUtilTest, testSplitRange)
{
    checkSplitRange(0, 999, 2, "0_499 500_999");
    checkSplitRange(0, 999, 3, "0_333 334_666 667_999");
    checkSplitRange(0, 0, 2, "0_0");
    checkSplitRange(0, 0, 1, "0_0");
    checkSplitRange(0, 1, 1, "0_1");
    checkSplitRange(0, 3, 0, "");
    checkSplitRange(0, 4, 3, "0_1 2_3 4_4");

    checkSplitRange(1, 3, 4, "1_1 2_2 3_3");
    checkSplitRange(1, 3, 3, "1_1 2_2 3_3");
    checkSplitRange(100, 199, 3, "100_133 134_166 167_199");
    checkSplitRange(1, 20, 3, "1_7 8_14 15_20");
    checkSplitRange(35533, 65535, 3, "35533_45533 45534_55534 55535_65535");

    checkSplitRange(0, 65535, 2, "0_16383 16384_32767 32768_49151 49152_65535", 2);
    checkSplitRange(0, 65535, 0, "", 2);
    checkSplitRange(0, 65535, 2, "0_32767 32768_65535", 1);
    checkSplitRange(0, 65535, 3, "0_21845 21846_43690 43691_65535", 1);
}

TEST_F(RangeUtilTest, testSplitRangeBySwiftPartition)
{
    // invalid input cases
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/0, /*rangeTo=*/65535, /*totalSwiftPartitionCount=*/5,
                                    /*parallelNum=*/2,
                                    /*expectedRangeStr=*/"");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/
                                    0, /*rangeTo=*/65535, /*totalSwiftPartitionCount=*/0, /*parallelNum=*/2,
                                    /*expectedRangeStr=*/"");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/321, /*rangeTo=*/123, /*totalSwiftPartitionCount=*/2,
                                    /*parallelNum=*/2,
                                    /*expectedRangeStr=*/"");

    // edge cases
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/321, /*rangeTo=*/321, /*totalSwiftPartitionCount=*/2,
                                    /*parallelNum=*/2,
                                    /*expectedRangeStr=*/"321_321");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/0, /*rangeTo=*/1, /*totalSwiftPartitionCount=*/2,
                                    /*parallelNum=*/2,
                                    /*expectedRangeStr=*/"0_1");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/0, /*rangeTo=*/1, /*totalSwiftPartitionCount=*/65536,
                                    /*parallelNum=*/65536,
                                    /*expectedRangeStr=*/"0_0 1_1");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/0, /*rangeTo=*/0, /*totalSwiftPartitionCount=*/65536,
                                    /*parallelNum=*/65536,
                                    /*expectedRangeStr=*/"0_0");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/
                                    0, /*rangeTo=*/65535, /*totalSwiftPartitionCount=*/1, /*parallelNum=*/1,
                                    /*expectedRangeStr=*/"0_65535");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/
                                    0, /*rangeTo=*/65535, /*totalSwiftPartitionCount=*/1, /*parallelNum=*/3,
                                    /*expectedRangeStr=*/"0_65535");

    // normal cases
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/0, /*rangeTo=*/65535, /*totalSwiftPartitionCount=*/2,
                                    /*parallelNum=*/4,
                                    /*expectedRangeStr=*/"0_32767 32768_65535");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/0, /*rangeTo=*/65535, /*totalSwiftPartitionCount=*/4,
                                    /*parallelNum=*/2,
                                    /*expectedRangeStr=*/"0_32767 32768_65535");

    // complicated cases
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/0, /*rangeTo=*/10922, /*totalSwiftPartitionCount=*/128,
                                    /*parallelNum=*/5,
                                    /*expectedRangeStr=*/"0_2559 2560_5119 5120_7167 7168_9215 9216_10922");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/10923, /*rangeTo=*/21845, /*totalSwiftPartitionCount=*/128,
                                    /*parallelNum=*/4,
                                    /*expectedRangeStr=*/"10923_13823 13824_16895 16896_19455 19456_21845");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/43692, /*rangeTo=*/54613, /*totalSwiftPartitionCount=*/128,
                                    /*parallelNum=*/1,
                                    /*expectedRangeStr=*/"43692_54613");
    checkSplitRangeBySwiftPartition(/*rangeFrom=*/43692, /*rangeTo=*/54613, /*totalSwiftPartitionCount=*/1,
                                    /*parallelNum=*/4,
                                    /*expectedRangeStr=*/"43692_54613");
}

TEST_F(RangeUtilTest, testGetParallelInfoByRange)
{
    for (uint32_t partCount = 1; partCount < 32; partCount++) {
        for (uint32_t parallelNum = 1; parallelNum < 20; parallelNum++) {
            checkGetParallelInfoByRange(0, 65535, partCount, parallelNum);
        }
    }
    checkGetParallelInfoByRange(0, 65535, 50, 50);
    checkGetParallelInfoByRange(0, 65535, 100, 100);
}

void RangeUtilTest::checkSplitRangeBySwiftPartition(uint32_t rangeFrom, uint32_t rangeTo,
                                                    uint32_t totalSwiftPartitionCount, uint32_t parallelNum,
                                                    const std::string& expectedRangeStr)
{
    std::vector<proto::Range> rangeVec =
        RangeUtil::splitRangeBySwiftPartition(rangeFrom, rangeTo, totalSwiftPartitionCount, parallelNum);
    std::string rangeStr;
    for (size_t i = 0; i < rangeVec.size(); ++i) {
        if (i > 0) {
            rangeStr += " ";
        }
        rangeStr +=
            autil::StringUtil::toString(rangeVec[i].from()) + "_" + autil::StringUtil::toString(rangeVec[i].to());
    }
    EXPECT_EQ(expectedRangeStr, rangeStr)
        << "rangeFrom=" << rangeFrom << ", rangeTo=" << rangeTo
        << ", totalSwiftPartitionCount=" << totalSwiftPartitionCount << ", parallelNum=" << parallelNum;
}

void RangeUtilTest::checkSplitRange(uint32_t rangeFrom, uint32_t rangeTo, uint32_t partitionCount,
                                    const std::string& expectRangeStr, uint32_t parallelNum)
{
    std::vector<proto::Range> rangeVec;
    if (parallelNum != 1) {
        rangeVec = RangeUtil::splitRange(rangeFrom, rangeTo, partitionCount, parallelNum);
    } else {
        rangeVec = RangeUtil::splitRange(rangeFrom, rangeTo, partitionCount);
    }
    std::string rangeStr;
    for (size_t i = 0; i < rangeVec.size(); ++i) {
        if (i > 0) {
            rangeStr += " ";
        }
        rangeStr +=
            autil::StringUtil::toString(rangeVec[i].from()) + "_" + autil::StringUtil::toString(rangeVec[i].to());
    }
    EXPECT_EQ(expectRangeStr, rangeStr);
}

void RangeUtilTest::checkGetParallelInfoByRange(uint32_t rangeFrom, uint32_t rangeTo, uint32_t partitionCount,
                                                uint32_t parallelNum)
{
    std::vector<proto::Range> subRanges = RangeUtil::splitRange(rangeFrom, rangeTo, partitionCount, parallelNum);
    for (size_t i = 0; i < subRanges.size(); i++) {
        uint32_t expectPartitionIdx = i / parallelNum;
        uint32_t expectParallelIdx = i % parallelNum;
        uint32_t partitionIdx = 0;
        uint32_t parallelIdx = 0;
        ASSERT_TRUE(RangeUtil::getParallelInfoByRange(rangeFrom, rangeTo, partitionCount, parallelNum, subRanges[i],
                                                      partitionIdx, parallelIdx));
        ASSERT_EQ(expectPartitionIdx, partitionIdx);
        ASSERT_EQ(expectParallelIdx, parallelIdx);

        proto::Range wrongRange;
        wrongRange.set_from(subRanges[i].from() + 1);
        wrongRange.set_to(subRanges[i].to() - 1);
        ASSERT_FALSE(RangeUtil::getParallelInfoByRange(rangeFrom, rangeTo, partitionCount, parallelNum, wrongRange,
                                                       partitionIdx, parallelIdx));
    }
}

}} // namespace build_service::util
