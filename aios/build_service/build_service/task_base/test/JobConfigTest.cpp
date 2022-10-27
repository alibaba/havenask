#include "build_service/test/unittest.h"
#include "build_service/task_base/JobConfig.h"
#include "build_service/proto/test/ProtoMatcher.h"

using namespace std;
using namespace testing;
using namespace build_service::proto;
namespace build_service {
namespace task_base {

class JobConfigTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
public:
    vector<proto::Range> splitBuildParts(
            uint32_t partCount, uint32_t buildParallelNum);
    proto::Range calculateMapRange(uint32_t partFrom, uint32_t partCount,
                                   uint32_t instanceId, uint32_t mapReduceRatio,
                                   uint32_t buildParallelNum, bool needPartition);
    proto::Range calculateReduceRange(
            uint32_t partFrom, uint32_t partCount,
            uint32_t buildParallelNum, uint32_t instanceId);
    proto::Range calculateMergeMapRange(
            uint32_t partFrom, uint32_t partCount, uint32_t instanceId);

    proto::Range calculateMergeReduceRange(
            uint32_t partFrom, uint32_t partCount,
            uint32_t mergeParallelNum, uint32_t instanceId);

};

void JobConfigTest::setUp() {
}

void JobConfigTest::tearDown() {
}

MATCHER_P3(ReduceRangeMatcher, from, to, reduceId, "") {
    for (uint32_t i = from; i <= (uint32_t)to; ++i) {
        if (reduceId != arg[i]) {
            *result_listener << "idx[" << i << "] is not equal to " << reduceId;
            return false;
        }
    }
    return true;
}

TEST_F(JobConfigTest, testCalcMapToReduceTableNormal) {
    std::vector<int32_t> table = JobConfig::calcMapToReduceTable(0, 2, 2, 1);
    EXPECT_THAT(table, ReduceRangeMatcher(0, 32767, 0));
    EXPECT_THAT(table, ReduceRangeMatcher(32768, 65535, 1));
}

TEST_F(JobConfigTest, testCalcMapToReduceTablePartFrom) {
    std::vector<int32_t> table = JobConfig::calcMapToReduceTable(1, 1, 2, 1);
    EXPECT_THAT(table, ReduceRangeMatcher(0, 32767, -1));
    EXPECT_THAT(table, ReduceRangeMatcher(32768, 65535, 0));
}

TEST_F(JobConfigTest, testCalcMapToReduceTableParallel) {
    std::vector<int32_t> table = JobConfig::calcMapToReduceTable(0, 2, 2, 2);
    EXPECT_THAT(table, ReduceRangeMatcher(0, 16383, 0));
    EXPECT_THAT(table, ReduceRangeMatcher(16384, 32767, 1));
    EXPECT_THAT(table, ReduceRangeMatcher(32768, 49151, 2));
    EXPECT_THAT(table, ReduceRangeMatcher(49152, 65535, 3));
}

TEST_F(JobConfigTest, testCalcMapToReduceTablePartCount) {
    std::vector<int32_t> table = JobConfig::calcMapToReduceTable(0, 1, 2, 2);
    EXPECT_THAT(table, ReduceRangeMatcher(0, 16383, 0));
    EXPECT_THAT(table, ReduceRangeMatcher(16384, 32767, 1));
    EXPECT_THAT(table, ReduceRangeMatcher(32768, 49151, -1));
    EXPECT_THAT(table, ReduceRangeMatcher(49152, 65535, -1));
}

TEST_F(JobConfigTest, testCalcMapToReduceTablePartCountAndFrom) {
    std::vector<int32_t> table = JobConfig::calcMapToReduceTable(1, 1, 2, 2);
    EXPECT_THAT(table, ReduceRangeMatcher(0, 16383, -1));
    EXPECT_THAT(table, ReduceRangeMatcher(16384, 32767, -1));
    EXPECT_THAT(table, ReduceRangeMatcher(32768, 49151, 0));
    EXPECT_THAT(table, ReduceRangeMatcher(49152, 65535, 1));
}

TEST_F(JobConfigTest, testSplitBuildParts) {
    EXPECT_THAT(splitBuildParts(1, 1), ElementsAre(
                    RangeEq(0, 65535)));
    EXPECT_THAT(splitBuildParts(4, 1), ElementsAre(
                    RangeEq(0, 16383),
                    RangeEq(16384, 32767),
                    RangeEq(32768, 49151),
                    RangeEq(49152, 65535)));
    EXPECT_THAT(splitBuildParts(1, 4), ElementsAre(
                    RangeEq(0, 16383),
                    RangeEq(16384, 32767),
                    RangeEq(32768, 49151),
                    RangeEq(49152, 65535)));
    EXPECT_THAT(splitBuildParts(2, 2), ElementsAre(
                    RangeEq(0, 16383),
                    RangeEq(16384, 32767),
                    RangeEq(32768, 49151),
                    RangeEq(49152, 65535)));
}

TEST_F(JobConfigTest, testCalculateReduceRange) {
    EXPECT_THAT(calculateReduceRange(0, 1, 1, 0), RangeEq(0, 65535));
    EXPECT_THAT(calculateReduceRange(0, 4, 1, 2), RangeEq(32768, 49151));
    EXPECT_THAT(calculateReduceRange(2, 4, 1, 0), RangeEq(32768, 49151));
    EXPECT_THAT(calculateReduceRange(1, 4, 2, 0), RangeEq(16384, 24575));
}

TEST_F(JobConfigTest, testCalculateMapRange) {
    EXPECT_THAT(calculateMapRange(0, 1, 0, 1, 1, false), RangeEq(0, 65535));
    EXPECT_THAT(calculateMapRange(0, 1, 0, 2, 1, false), RangeEq(0, 65535));
    EXPECT_THAT(calculateMapRange(0, 4, 2, 1, 1, false), RangeEq(32768, 49151));
    EXPECT_THAT(calculateMapRange(2, 4, 0, 1, 1, false), RangeEq(32768, 49151));

    EXPECT_THAT(calculateMapRange(0, 1, 0, 1, 1, true), RangeEq(0, 65535));
    EXPECT_THAT(calculateMapRange(0, 1, 0, 2, 1, true), RangeEq(0, 32767));
    EXPECT_THAT(calculateMapRange(0, 4, 2, 1, 1, true), RangeEq(32768, 49151));
    EXPECT_THAT(calculateMapRange(2, 4, 0, 1, 1, true), RangeEq(0, 16383));

    EXPECT_THAT(calculateMapRange(1, 4, 0, 1, 2, false), RangeEq(16384, 24575));
}

TEST_F(JobConfigTest, testCalculateMergeMapRange) {
    EXPECT_THAT(calculateMergeMapRange(0, 1, 0), RangeEq(0, 65535));
    EXPECT_THAT(calculateMergeMapRange(0, 4, 2), RangeEq(32768, 49151));
    EXPECT_THAT(calculateMergeMapRange(2, 4, 0), RangeEq(32768, 49151));
}

TEST_F(JobConfigTest, testCalculateMergeReduceRange) {
    EXPECT_THAT(calculateMergeReduceRange(0, 1, 2, 1), RangeEq(0, 65535));
    EXPECT_THAT(calculateMergeReduceRange(0, 1, 2, 0), RangeEq(0, 65535));
    EXPECT_THAT(calculateMergeReduceRange(0, 4, 2, 5), RangeEq(32768, 49151));
}

proto::Range JobConfigTest::calculateMergeMapRange(
        uint32_t partFrom, uint32_t partCount, uint32_t instanceId)
{
    JobConfig jobConfig;
    jobConfig._buildPartitionFrom = partFrom;
    jobConfig._config.partitionCount = partCount;
    return jobConfig.calculateMergeMapRange(instanceId);
}

proto::Range JobConfigTest::calculateMergeReduceRange(
        uint32_t partFrom, uint32_t partCount,
        uint32_t mergeParallelNum, uint32_t instanceId)
{
    JobConfig jobConfig;
    jobConfig._buildPartitionFrom = partFrom;
    jobConfig._config.partitionCount = partCount;
    jobConfig._config.mergeParallelNum = mergeParallelNum;
    return jobConfig.calculateMergeReduceRange(instanceId);
}

vector<proto::Range> JobConfigTest::splitBuildParts(
        uint32_t partCount, uint32_t buildParallelNum)
{
    JobConfig jobConfig;
    jobConfig._config.partitionCount = partCount;
    jobConfig._config.buildParallelNum = buildParallelNum;
    return jobConfig.splitBuildParts();
}

proto::Range JobConfigTest::calculateMapRange(
        uint32_t partFrom, uint32_t partCount,
        uint32_t instanceId, uint32_t mapReduceRatio,
        uint32_t buildParallelNum, bool needPartition)
{
    JobConfig jobConfig;
    jobConfig._buildPartitionFrom = partFrom;
    jobConfig._config.partitionCount = partCount;
    jobConfig._config.mapReduceRatio = mapReduceRatio;
    jobConfig._config.buildParallelNum = buildParallelNum;
    jobConfig._config.needPartition = needPartition;
    return jobConfig.calculateBuildMapRange(instanceId);
}

proto::Range JobConfigTest::calculateReduceRange(
        uint32_t partFrom, uint32_t partCount,
        uint32_t buildParallelNum, uint32_t instanceId)
{
    JobConfig jobConfig;
    jobConfig._buildPartitionFrom = partFrom;
    jobConfig._config.partitionCount = partCount;
    jobConfig._config.buildParallelNum = buildParallelNum;
    jobConfig._config.needPartition = true;
    return jobConfig.calculateBuildReduceRange(instanceId);
}

}
}
