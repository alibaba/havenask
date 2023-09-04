#ifndef __INDEXLIB_SEGMENTATTRIBUTEUPDATERTEST_H
#define __INDEXLIB_SEGMENTATTRIBUTEUPDATERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

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
    framework::SegmentMetrics LoadSegmentMetrics(segmentid_t segId);
    framework::SegmentMetrics LoadSegmentMetrics(test::PartitionStateMachine& psm, segmentid_t segId);
    index_base::SegmentInfo LoadSegmentInfo(test::PartitionStateMachine& psm, segmentid_t segId, bool isSub);
    template <typename T>
    void assertAttribute(const framework::SegmentMetrics& metrics, const std::string& attrName, T attrMin, T attrMax,
                         size_t checkedDocCount);
    void assertLifecycle(const framework::SegmentMetrics& metrics, const std::string& expected);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::string mRootDir;
    bool mMultiTargetSegment;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(SegmentMetricsUpdaterTestMode, SegmentMetricsUpdaterTest,
                                     testing::Values(false, true));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestSortBuild);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestLifecycleSimple);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestLifecycleForEmptyOutputSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SegmentMetricsUpdaterTestMode, TestLifecycleFromEmptyMetrics);
}} // namespace indexlib::partition

#endif //__INDEXLIB_SEGMENTATTRIBUTEUPDATERTEST_H
