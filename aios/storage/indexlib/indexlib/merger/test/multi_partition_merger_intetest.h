#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/multi_partition_merger.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class MultiPartitionMergerInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    MultiPartitionMergerInteTest();
    ~MultiPartitionMergerInteTest();

    DECLARE_CLASS_NAME(MultiPartitionMergerInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMergeWithDeleteAndUpdate();
    void TestMergeWithSubDocDeleteAndUpdate(bool withPackage = false);
    void TestMergePackageWithSubDocDeleteAndUpdate();

    void TestMultiPartFullMergeWithAllSegmentsMergeToOneSegment();
    void TestMultiPartFullMergeWithMultiSegmentsMergeToOneSegment();
    void TestMultiPartFullMergeWithSingleSegmentMergeToOneSegment();
    void TestSinglePartFullMergeWithAllSegmentsMergeToOneSegment();
    void TestSinglePartFullMergeWithMultiSegmentsMergeToOneSegment();
    void TestSinglePartFullMergeWithSingleSegmentMergeToOneSegment();

    void TestIncOptimizeMergeWithAllSegmentsMergeToOneSegment();
    void TestIncOptimizeMergeWithMultiSegmentsMergeToOneSegment();
    void TestIncOptimizeMergeWithSingleSegmentMergeToOneSegment();

    void TestIncBalanceTreeMerge();
    void TestIncPriorityQueueMerge();
    void TestIncRealtimeMerge();

    void TestMaxDocCountWithFullMerge();
    void TestMaxDocCountWithIncOptimizeMerge();

    void TestOptimizeMergeToOneSegmentMergeTwice();
    void TestOptimizeMergeToMultiSegmentMergeTwice();

    void TestMergeWithEmptyPartitions();
    void TestDiffPartitionHasSamePk();

    void TestMergerMetrics();
    void TestMergeCounters();
    void TestMergeLocatorAndTimestamp();
    void TestMergeWithStopTimestamp();

private:
    void MakeOnePartitionData(const config::IndexPartitionSchemaPtr& schema,
                              const config::IndexPartitionOptions& options, const std::string& partRootDir,
                              const std::string& docStrs);

    void DoTestMerge(bool isFullMerge, bool isMultiPart, const std::string& mergeStrategy,
                     const std::string& mergeStrategyParam, bool isMergeTwice = false);

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(UsePackFile, MultiPartitionMergerInteTest, testing::Values(true, false));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestMergeWithDeleteAndUpdate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestMergeWithSubDocDeleteAndUpdate);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestMergePackageWithSubDocDeleteAndUpdate);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest,
                                   TestMultiPartFullMergeWithAllSegmentsMergeToOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest,
                                   TestMultiPartFullMergeWithMultiSegmentsMergeToOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest,
                                   TestMultiPartFullMergeWithSingleSegmentMergeToOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest,
                                   TestSinglePartFullMergeWithAllSegmentsMergeToOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest,
                                   TestSinglePartFullMergeWithMultiSegmentsMergeToOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest,
                                   TestSinglePartFullMergeWithSingleSegmentMergeToOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestIncOptimizeMergeWithAllSegmentsMergeToOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest,
                                   TestIncOptimizeMergeWithMultiSegmentsMergeToOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest,
                                   TestIncOptimizeMergeWithSingleSegmentMergeToOneSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestIncBalanceTreeMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestIncPriorityQueueMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestMaxDocCountWithFullMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestMaxDocCountWithIncOptimizeMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestOptimizeMergeToOneSegmentMergeTwice);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestOptimizeMergeToMultiSegmentMergeTwice);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestMergeWithEmptyPartitions);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestDiffPartitionHasSamePk);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestMergerMetrics);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestMergeCounters);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestMergeLocatorAndTimestamp);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(MultiPartitionMergerInteTest, TestMergeWithStopTimestamp);
}} // namespace indexlib::merger
