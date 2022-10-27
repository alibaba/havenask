#ifndef __INDEXLIB_LIFECYCLELOADTEST_H
#define __INDEXLIB_LIFECYCLELOADTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"

IE_NAMESPACE_BEGIN(partition);

class LifecycleLoadTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    LifecycleLoadTest();
    ~LifecycleLoadTest();

    DECLARE_CLASS_NAME(LifecycleLoadTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    index_base::SegmentMetrics LoadSegmentMetrics(
        test::PartitionStateMachine& psm, segmentid_t segId);

    void CheckFileStat(file_system::IndexlibFileSystemPtr fileSystem, std::string filePath,
        file_system::FSOpenType expectOpenType, file_system::FSFileType expectFileType);

    void assertLifecycle(const index_base::SegmentMetrics& metrics, const std::string& expected);    

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::string mRootDir;
    bool mMultiTargetSegment;    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
    LifecycleLoadTestMode, LifecycleLoadTest, testing::Values(false, true));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(LifecycleLoadTestMode, TestSimpleProcess);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_LIFECYCLELOADTEST_H
