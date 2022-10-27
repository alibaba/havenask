#ifndef __INDEXLIB_ONLINEPARTITIONTEST_H
#define __INDEXLIB_ONLINEPARTITIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/online_partition.h"

IE_NAMESPACE_BEGIN(partition);

class OnlinePartitionTest : public INDEXLIB_TESTBASE
{
public:
    OnlinePartitionTest();
    ~OnlinePartitionTest();

    DECLARE_CLASS_NAME(OnlinePartitionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
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
    void TestMultiThreadLoadPatchWhenDisableField();
    void TestInitPathMetaCache();
    void TestCleanIndexFiles();

private:
    void PrepareData(const config::IndexPartitionOptions& options, bool hasSub = false);

    bool DoTestCanReopen(uint32_t memLimit, uint32_t maxReopenMemUse,
                         uint32_t indexExpandSize, uint32_t curMemUse);

    bool DoTestCanForceReopen(uint32_t memLimit, uint32_t versionSize,
                              uint32_t rtIndexSize, uint32_t writerSize,
                              uint32_t opqSize);
    void InnerTestReclaimRtIndexNotEffectReader(
            std::string docStrings, const index_base::Version& incVersion,
            const config::IndexPartitionOptions& options, size_t checkDocCount);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestOpen);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestOpenResetRtAndJoinPath);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestClose);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestCloseWithReaderHeld);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestCheckMemoryStatus);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestReOpenNewSegmentWithNoNewRtVersion);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestReopenWithMissingFiles);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestCreateNewSchema);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestAddVirtualAttributeDataCleaner);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestReclaimRtIndexNotEffectReader);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestOpenEnableLoadSpeedLimit);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestOpenAndForceReopenDisableLoadSpeedLimit);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestReOpenDoesNotDisableLoadSpeedLimit);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestOpenWithInvalidPartitionMeta);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestOpenWithPrepareIntervalTask);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestExecuteTask);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestCheckMemoryStatusWithTryLock);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestLazyInitWriter);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestDisableSSEPforDeltaOptimize);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestDisableField);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestDisableOnlyPackAttribute);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestDisableAllAttribute);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestMultiThreadLoadPatchWhenDisableField);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestInitPathMetaCache);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionTest, TestCleanIndexFiles);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINEPARTITIONTEST_H
