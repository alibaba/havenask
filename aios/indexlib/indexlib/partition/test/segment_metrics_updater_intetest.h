#ifndef __INDEXLIB_SEGMENTATTRIBUTEUPDATERTEST_H
#define __INDEXLIB_SEGMENTATTRIBUTEUPDATERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"

IE_NAMESPACE_BEGIN(partition);

class SegmentMetricsUpdaterTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    SegmentMetricsUpdaterTest();
    ~SegmentMetricsUpdaterTest();

    DECLARE_CLASS_NAME(SegmentMetricsUpdaterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSortBuild();
    void TestSubDoc();

    void TestLifecycleSimple();
    void TestLifecycleForEmptyOutputSegment();
    void TestLifecycleFromEmptyMetrics();

private:
    index_base::SegmentMetrics LoadSegmentMetrics(segmentid_t segId);
    index_base::SegmentMetrics LoadSegmentMetrics(
        test::PartitionStateMachine& psm, segmentid_t segId);
    index_base::SegmentInfo LoadSegmentInfo(segmentid_t segId, bool isSub);
    template <typename T>
    void assertAttribute(const index_base::SegmentMetrics& metrics, const std::string& attrName,
        T attrMin, T attrMax, size_t checkedDocCount);
    void assertLifecycle(const index_base::SegmentMetrics& metrics, const std::string& expected);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::string mRootDir;
    bool mMultiTargetSegment;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(SegmentMetricsUpdaterTestMode, SegmentMetricsUpdaterTest, testing::Values(false, true));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestSortBuild);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestLifecycleSimple);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(
    SegmentMetricsUpdaterTestMode, TestLifecycleForEmptyOutputSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestLifecycleFromEmptyMetrics);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SEGMENTATTRIBUTEUPDATERTEST_H
