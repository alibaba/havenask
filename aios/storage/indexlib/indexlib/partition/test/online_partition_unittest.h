#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OnlinePartitionTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    OnlinePartitionTest();
    ~OnlinePartitionTest();

    DECLARE_CLASS_NAME(OnlinePartitionTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleOpen();
    void TestOpen();
    void TestOpenResetRtAndJoinPath();
    void TestClose();
    void TestCloseWithReaderHeld();
    void TestCheckMemoryStatus();
    void TestReOpenNewSegmentWithNoNewRtVersion();
    void TestReopenWithMissingFiles();
    void TestCreateNewSchema();
    void TestAddVirtualAttributeDataCleaner();
    void TestReclaimRtIndexNotEffectReader();
    void TestOpenWithInvalidPartitionMeta();
    void TestOpenEnableLoadSpeedLimit();
    void TestOpenAndForceReopenDisableLoadSpeedLimit();
    void TestReOpenDoesNotDisableLoadSpeedLimit();
    void TestOpenWithPrepareIntervalTask();
    void TestExecuteTask();
    void TestCheckMemoryStatusWithTryLock();
    void TestLazyInitWriter();
    void TestDisableSSEPforDeltaOptimize();
    void TestDisableField();
    void TestDisableOnlyPackAttribute();
    void TestDisableAllAttribute();
    void TestDisableSummary();
    void TestMultiThreadLoadPatchWhenDisableField();
    void TestCleanIndexFiles();
    void TestOptimizedNormalReopen();
    void TestOptimizedNormalReopenWithReclaimBuilding();
    void TestSeriesReopen();
    void TestOptimizedNormalReopenWithSub();
    void TestOptimizedNormalReopenReclaimBuilding();
    void TestOptimizedNormalReopenWithDumping();
    void TestAsyncDump();
    void TestRtVersionHasTemperatureMeta();
    void TestDeplayDeduperWithTemperature();
    void TestTemperatureIndexCallBack();
    void TestTemperatureCallBack2();
    void TestVersionWithPatchInfo();
    void TestTemperaturePkLookup();
    void TestTemperatureLayerChange();
    void TestSortBuildWithSplit();
    void TestDpRemoteWithTemperature();
    void TestOnlinePartitionDirectWriteToRemote();
    void TestCounterInfo();

private:
    void PrepareData(const config::IndexPartitionOptions& options, bool hasSub = false);

    bool DoTestCanReopen(uint32_t memLimit, uint32_t maxReopenMemUse, uint32_t indexExpandSize, uint32_t curMemUse);

    bool DoTestCanForceReopen(uint32_t memLimit, uint32_t versionSize, uint32_t rtIndexSize, uint32_t writerSize,
                              uint32_t opqSize);
    void InnerTestReclaimRtIndexNotEffectReader(std::string docStrings, const index_base::Version& incVersion,
                                                const config::IndexPartitionOptions& options, size_t checkDocCount);
    void CheckDocCount(size_t expectedDocCount, const IndexPartitionPtr& indexPartition);

    void DeployToLocal(test::PartitionStateMachine& psm, std::string dpToLocalFile, std::string openMode,
                       versionid_t targetVersion, const std::string& primaryPath, const std::string& secondaryPath);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, OnlinePartitionTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestSimpleOpen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOpen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOpenResetRtAndJoinPath);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestClose);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestCloseWithReaderHeld);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestCheckMemoryStatus);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestReOpenNewSegmentWithNoNewRtVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestReopenWithMissingFiles);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestCreateNewSchema);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestAddVirtualAttributeDataCleaner);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestReclaimRtIndexNotEffectReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOpenEnableLoadSpeedLimit);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOpenAndForceReopenDisableLoadSpeedLimit);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestReOpenDoesNotDisableLoadSpeedLimit);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOpenWithInvalidPartitionMeta);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOpenWithPrepareIntervalTask);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestExecuteTask);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestCheckMemoryStatusWithTryLock);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestLazyInitWriter);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestDisableSSEPforDeltaOptimize);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestDisableField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestDisableOnlyPackAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestDisableAllAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestDisableSummary);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestMultiThreadLoadPatchWhenDisableField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestCleanIndexFiles);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOptimizedNormalReopen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOptimizedNormalReopenWithReclaimBuilding);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestSeriesReopen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOptimizedNormalReopenWithSub);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOptimizedNormalReopenReclaimBuilding);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOptimizedNormalReopenWithDumping);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestAsyncDump);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestRtVersionHasTemperatureMeta);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestDeplayDeduperWithTemperature);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestTemperatureIndexCallBack);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestTemperatureCallBack2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestTemperaturePkLookup);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestTemperatureLayerChange);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestSortBuildWithSplit);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestDpRemoteWithTemperature);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestOnlinePartitionDirectWriteToRemote);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionTest, TestCounterInfo);

}} // namespace indexlib::partition
