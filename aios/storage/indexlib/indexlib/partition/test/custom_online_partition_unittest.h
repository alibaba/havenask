#ifndef __INDEXLIB_CUSTOMONLINEPARTITIONTEST_H
#define __INDEXLIB_CUSTOMONLINEPARTITIONTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/framework/TabletDeployer.h"
#include "indexlib/partition/custom_online_partition.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(partition, IndexBuilder);

namespace indexlibv2 { namespace framework {
struct VersionDeployDescription;
}} // namespace indexlibv2::framework

namespace indexlib { namespace partition {

class CustomOnlinePartitionTest : public INDEXLIB_TESTBASE
{
public:
    CustomOnlinePartitionTest();
    ~CustomOnlinePartitionTest();

    DECLARE_CLASS_NAME(CustomOnlinePartitionTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleOpen();
    void TestSimpleReopen();
    void TestMemoryControl();
    void TestCheckMemoryStatus();
    void TestDropRtDocWhenTsEarlyThanIncVersionTs();
    void TestOnlineKeepVersionCount();
    void TestEstimateDiffVersionLockSize();
    void TestAsyncDumpOnline();
    void TestBuildingSegmentReaderWillBeReleasedInAsyncDump();
    void TestAsynDumpWithReopen();
    void TestFlushRtSegments();
    void TestFlushRtSegmentsWithMemControlPolicy();
    void TestCheckRealtimeBuildTask();
    void TestLoadRemainFlushRealtimeIndex();
    void TestFlushRealtimeIndexWithRemainRtIndex();
    void TestFlushRealtimeIndexWithForceReopen();
    void TestFlushRealtimeIndexForRemoteLoadConfig();
    void TestBugMayCauseDeadlock();
    void TestBugMayCauseCoreDump();
    void TestOpenWithVersionDeployDescription();
    void TestRefreshLifecycleWhenCommitVersion();

private:
    void PrepareBuildData(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                          const std::string& docString, const std::string& rootDir);

    std::vector<document::DocumentPtr> CreateDocuments(const std::string& rawDocString) const;
    void DumpThread(CustomOnlinePartition* indexPartition);
    void BuildThread(IndexBuilderPtr indexBuilder);
    void CleanResourceThread(CustomOnlinePartition* indexPartition);
    void CheckLocalFiles(const std::string indexRoot, const std::set<std::string>& whiteList,
                         const std::set<std::string>& blackList);
    void CheckDeployIndexMeta(const std::string& indexRoot,
                              const indexlibv2::framework::TabletDeployer::GetDeployIndexMetaInputParams& inputParams,
                              indexlibv2::framework::VersionDeployDescription* versionDpDesc);

private:
    std::string mRootDir;
    std::string mPluginRoot;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOfflineOptions;
    config::IndexPartitionOptions mOnlineOptions;
    bool mIsFinish;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestSimpleOpen);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestSimpleReopen);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestMemoryControl);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestCheckMemoryStatus);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestDropRtDocWhenTsEarlyThanIncVersionTs);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestOnlineKeepVersionCount);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestEstimateDiffVersionLockSize);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestAsyncDumpOnline);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestBuildingSegmentReaderWillBeReleasedInAsyncDump);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestAsynDumpWithReopen);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestFlushRtSegments);
// INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestFlushRtSegmentsWithMemControlPolicy);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestCheckRealtimeBuildTask)
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestLoadRemainFlushRealtimeIndex);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestFlushRealtimeIndexWithRemainRtIndex);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestFlushRealtimeIndexWithForceReopen);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestFlushRealtimeIndexForRemoteLoadConfig);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestBugMayCauseDeadlock);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestBugMayCauseCoreDump);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestOpenWithVersionDeployDescription);
INDEXLIB_UNIT_TEST_CASE(CustomOnlinePartitionTest, TestRefreshLifecycleWhenCommitVersion);

}} // namespace indexlib::partition

#endif //__INDEXLIB_CUSTOMONLINEPARTITIONTEST_H
