#include "autil/CommonMacros.h"
#include "autil/NetUtil.h"
#include "autil/Scope.h"
#include "fslib/fs/ExceptionTrigger.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/index_task/Constant.h"
#include "indexlib/framework/mock/MockIdGenerator.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/kv/IKVIterator.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/table/kv_table/KVReaderImpl.h"
#include "indexlib/table/kv_table/KVTabletFactory.h"
#include "indexlib/table/kv_table/KVTabletReader.h"
#include "indexlib/table/kv_table/KVTabletSessionReader.h"
#include "indexlib/table/kv_table/KVTabletWriter.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using fslib::fs::ExceptionTrigger;
using namespace indexlibv2::index;
using namespace indexlibv2::framework;

namespace indexlibv2 { namespace table {

// param: value format (default|impact|plain)
class KVTabletInteTest : public indexlib::INDEXLIB_TESTBASE_WITH_PARAM<std::string>
{
public:
    KVTabletInteTest() {}
    ~KVTabletInteTest() = default;

    void CaseSetUp() override
    {
        unsetenv("IS_TEST_MODE");

        _tabletOptions = CreateOnlineMultiShardOptions();
        std::string field = "string1:string;string2:string";
        _tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2", -1, GET_CASE_PARAM());
    }
    std::string docString(std::vector<std::string> docPrefix)
    {
        std::string ret;
        for (auto prefix : docPrefix) {
            _offset++;
            ret += prefix + std::to_string(_offset) + ";";
        }
        return ret;
    }

    std::string docString1()
    {
        std::vector<std::string> docPrefix = {
            "cmd=add,string1=1,string2=abc,ts=1,locator=0:", "cmd=add,string1=2,string2=cde,ts=2,locator=0:",
            "cmd=add,string1=3,string2=fgh,ts=3,locator=0:", "cmd=add,string1=321,string2=fgh,ts=3,locator=0:",
            "cmd=add,string1=4,string2=ijk,ts=4,locator=0:"};
        return docString(docPrefix);
    }

    std::string docString2()
    {
        std::vector<std::string> docPrefix = {
            "cmd=add,string1=1,string2=ijk,ts=5,locator=0:",  "cmd=add,string1=12,string2=Acde,ts=6,locator=0:",
            "cmd=add,string1=3,string2=Afgh,ts=7,locator=0:", "cmd=add,string1=14,string2=Aijk,ts=8,locator=0:",
            "cmd=add,string1=15,string2=15,ts=9,locator=0:",  "cmd=delete,string1=15,ts=10,locator=0:",
            "cmd=delete,string1=2,ts=13,locator=0:"};
        return docString(docPrefix);
    }
    std::string docString3()
    {
        std::vector<std::string> docPrefix = {
            "cmd=add,string1=1,string2=ijk,ts=12,locator=0:", "cmd=add,string1=12,string2=12,ts=13,locator=0:",
            "cmd=add,string1=3,string2=Afgh,ts=14,locator=0:", "cmd=add,string1=15,string2=15,ts=15,locator=0:"};
        return docString(docPrefix);
    }

    void CaseTearDown() override {}

private:
    std::string GetLocalRoot() const { return GET_TEMP_DATA_PATH() + "/local_index/"; }
    std::string GetRemoteRoot() const { return std::string("LOCAL://") + GET_TEMP_DATA_PATH() + "/remote_index/"; }
    IndexRoot GetIndexRoot() const { return IndexRoot(GetLocalRoot(), GetRemoteRoot()); }
    versionid_t GetPrivateVersionId(versionid_t versionId)
    {
        return versionId | framework::Version::PRIVATE_VERSION_ID_MASK;
    }
    versionid_t GetPublicVersionId(versionid_t versionId)
    {
        return versionId | framework::Version::PUBLIC_VERSION_ID_MASK;
    }
    void InnerTestBuildForceDump(bool enableCompactHashKey, bool enableShortOffset, size_t maxValueFileSize,
                                 std::string hashType, size_t totalDocSize, size_t segmentNum, std::string dirName);
    void CheckLevelInfo(const std::shared_ptr<Tablet>& tablet, uint32_t levelCount, uint32_t shardCount,
                        LevelTopology topology, std::string levelSegments);
    std::shared_ptr<config::TabletOptions> CreateOnlineMultiShardOptions(const std::string& userDefinedJsonStr = "");
    std::shared_ptr<config::TabletOptions> CreateOfflineMultiShardOptions(const std::string& userDefinedJsonStr = "");
    framework::Locator MakeLocator(uint64_t src, int64_t minOffset,
                                   std::vector<std::tuple<uint32_t, uint32_t, int64_t>> progressParam);
    std::shared_ptr<MockIdGenerator> MakeIdGenerator(const std::vector<segmentid_t>& segmentIdList,
                                                     const std::vector<versionid_t>& versionIdList);

    std::pair<Status, bool> IsFenceExist(const std::string& root, const TableTestHelper* helper)
    {
        auto dir = indexlib::file_system::Directory::GetPhysicalDirectory(root);
        return dir->GetIDirectory()
            ->IsExist(framework::TabletTestAgent(helper->GetTablet()).TEST_GetFence().GetFenceName())
            .StatusWith();
    }

private:
    framework::ImportOptions _importOptions;
    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::shared_ptr<config::TabletSchema> _tabletSchema;
    int64_t _offset = 0;
};

INSTANTIATE_TEST_CASE_P(TestDefaultValueFormat, KVTabletInteTest, testing::Values(""));
INSTANTIATE_TEST_CASE_P(TestPlainValueFormat, KVTabletInteTest, testing::Values("plain"));
INSTANTIATE_TEST_CASE_P(TestImpactValueFormat, KVTabletInteTest, testing::Values("impact"));

class FakeKVReader : public KVReaderImpl
{
public:
    FakeKVReader(schemaid_t id) : KVReaderImpl(id) {}
    ~FakeKVReader() {}

public:
    static void setFail(bool fail) { _fail = fail; }

    FL_LAZY(KVResultStatus)
    InnerGet(const KVReadOptions* readOptions, keytype_t key, autil::StringView& value,
             KVMetricsCollector* metricsCollector = NULL) const noexcept override
    {
        if (_fail) {
            FL_CORETURN KVResultStatus::FAIL;
        } else {
            FL_CORETURN FL_COAWAIT KVReaderImpl::InnerGet(readOptions, key, value, metricsCollector);
        }
    }

private:
    inline static bool _fail = false;
};

class FakeKVTabletReader : public KVTabletReader
{
public:
    FakeKVTabletReader(const std::shared_ptr<config::ITabletSchema>& schema) : KVTabletReader(schema) {}
    Status DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                  const framework::ReadResource& readResource) override
    {
        assert(_schema->GetIndexConfigs().size() == 1);
        auto indexConfig = _schema->GetIndexConfigs()[0];
        const auto& indexType = indexConfig->GetIndexType();
        const auto& indexName = indexConfig->GetIndexName();
        auto indexReader = std::make_unique<FakeKVReader>(_schema->GetSchemaId());
        auto status = indexReader->Open(indexConfig, tabletData.get());
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "create indexReader IndexType[%s] indexName[%s] failed, status[%s].", indexType.c_str(),
                      indexName.c_str(), status.ToString().c_str());
            return status;
        }
        _indexReaderMap[std::pair(indexType, indexName)] = std::move(indexReader);
        return Status::OK();
    }
};

class FakeKVTabletWriter : public KVTabletWriter
{
public:
    FakeKVTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema, const config::TabletOptions* options)
        : KVTabletWriter(schema, options)
    {
    }
    std::shared_ptr<KVTabletReader> CreateKVTabletReader() override
    {
        return std::make_shared<FakeKVTabletReader>(_schema);
    }
};

class FakeKVTabletFactory : public KVTabletFactory
{
public:
    std::unique_ptr<ITabletFactory> Create() const override { return std::make_unique<FakeKVTabletFactory>(); }
    std::unique_ptr<framework::TabletWriter>
    CreateTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema) override
    {
        return std::make_unique<FakeKVTabletWriter>(schema, _options->GetTabletOptions());
    }
};

std::shared_ptr<config::TabletOptions>
KVTabletInteTest::CreateOnlineMultiShardOptions(const std::string& userDefinedJsonStr)
{
    std::string defaultJsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";
    std::string jsonStr = userDefinedJsonStr.empty() ? defaultJsonStr : userDefinedJsonStr;
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);
    tabletOptions->TEST_GetOnlineConfig().TEST_SetNeedDeployIndex(false);
    tabletOptions->TEST_GetOnlineConfig().TEST_SetNeedReadRemoteIndex(true);
    tabletOptions->TEST_GetOnlineConfig().MutableLoadConfigList().SetLoadMode(
        indexlib::file_system::LoadConfig::LoadMode::REMOTE_ONLY);

    return tabletOptions;
}

std::shared_ptr<config::TabletOptions>
KVTabletInteTest::CreateOfflineMultiShardOptions(const std::string& userDefinedJsonStr)
{
    std::string defaultJsonStr = R"( {
    "offline_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";
    std::string jsonStr = userDefinedJsonStr.empty() ? defaultJsonStr : userDefinedJsonStr;
    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->SetIsOnline(false);
    tabletOptions->SetIsLeader(true);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);
    return tabletOptions;
}

framework::Locator KVTabletInteTest::MakeLocator(uint64_t src, int64_t minOffset,
                                                 std::vector<std::tuple<uint32_t, uint32_t, int64_t>> progressParam)
{
    framework::Locator locator(src, minOffset);
    std::vector<base::Progress> progress;
    for (auto [from, to, ts] : progressParam) {
        base::Progress::Offset offset = {ts, 0};
        progress.emplace_back(from, to, offset);
    }
    locator.SetMultiProgress({progress});
    return locator;
}
std::shared_ptr<MockIdGenerator> KVTabletInteTest::MakeIdGenerator(const std::vector<segmentid_t>& segmentIdList,
                                                                   const std::vector<versionid_t>& versionIdList)
{
    auto mockIdGenerator = std::make_shared<MockIdGenerator>(IdMaskType::BUILD_PUBLIC);
    EXPECT_CALL(*mockIdGenerator, GetNextSegmentIdUnSafe())
        .Times(segmentIdList.size())
        .WillRepeatedly(testing::ReturnRoundRobin(segmentIdList));
    EXPECT_CALL(*mockIdGenerator, GetNextVersionIdUnSafe())
        .Times(versionIdList.size())
        .WillRepeatedly(testing::ReturnRoundRobin(versionIdList));
    return mockIdGenerator;
}
// test import, parallel num == 4, first import slave 0&2, second import slave 0&1&3
TEST_P(KVTabletInteTest, TestImport)
{
    framework::IndexRoot indexRoot = GetIndexRoot();
    std::shared_ptr<config::TabletOptions> offlineTabletOptions = CreateOfflineMultiShardOptions();

    // slave builder 0
    KVTableTestHelper helper0(/*autoCleanIndex=*/true, /*needDeploy=*/false);
    helper0.SetIdGenerator(MakeIdGenerator({536870912, 536870912, 536870916, 536870916}, {1073741824, 1073741828}));
    ASSERT_TRUE(helper0.Open(indexRoot, _tabletSchema, offlineTabletOptions, INVALID_VERSIONID).IsOK());
    helper0.GetTaskScheduler()->CleanTasks();

    ASSERT_TRUE(helper0.Build("cmd=add,string1=0_1,string2=a1,ts=1;", MakeLocator(0, 1, {{0, 16383, 1}})).IsOK());
    ASSERT_TRUE(helper0.Build("cmd=add,string1=0_2,string2=a2,ts=2;", MakeLocator(0, 2, {{0, 16383, 2}})).IsOK());
    ASSERT_TRUE(helper0.Flush().IsOK());
    ASSERT_TRUE(helper0.NeedCommit());
    auto [status1, versionId0] = helper0.Commit(framework::CommitOptions().SetNeedPublish(false));
    ASSERT_EQ(versionId0, 1073741824);
    ASSERT_TRUE(status1.IsOK());
    framework::Version version0 =
        framework::TabletTestAgent(helper0.GetTablet())
            .TEST_GetVersion(VersionCoord(
                versionId0, framework::TabletTestAgent(helper0.GetTablet()).TEST_GetFence().GetFenceName()));

    // slave builder 2
    KVTableTestHelper helper2(/*autoCleanIndex=*/true, /*needDeploy=*/false);
    helper2.SetIdGenerator(MakeIdGenerator({536870914, 536870914}, {1073741826}));
    ASSERT_TRUE(helper2.Open(indexRoot, _tabletSchema, offlineTabletOptions, INVALID_VERSIONID).IsOK());
    helper2.GetTaskScheduler()->CleanTasks();

    ASSERT_TRUE(helper2.Build("cmd=add,string1=2_1,string2=c3,ts=3;", MakeLocator(0, 3, {{32768, 49151, 3}})).IsOK());
    ASSERT_TRUE(helper2.Build("cmd=add,string1=2_2,string2=c4,ts=4;", MakeLocator(0, 4, {{32768, 49151, 4}})).IsOK());
    ASSERT_TRUE(helper2.Flush().IsOK());
    ASSERT_TRUE(helper2.NeedCommit());
    auto [status2, versionId2] = helper2.Commit(framework::CommitOptions().SetNeedPublish(false));
    ASSERT_EQ(versionId2, 1073741826);
    ASSERT_TRUE(status2.IsOK());
    framework::Version version2 =
        framework::TabletTestAgent(helper2.GetTablet())
            .TEST_GetVersion(VersionCoord(
                versionId2, framework::TabletTestAgent(helper2.GetTablet()).TEST_GetFence().GetFenceName()));

    // master builder && import first time
    KVTableTestHelper masterHelper(/*autoCleanIndex=*/true, /*needDeploy=*/false);
    ASSERT_TRUE(masterHelper.Open(indexRoot, _tabletSchema, offlineTabletOptions).IsOK());
    masterHelper.GetTaskScheduler()->CleanTasks();

    ASSERT_TRUE(masterHelper.GetTablet()->Import({version0, version2}, _importOptions).IsOK());
    ASSERT_TRUE(masterHelper.NeedCommit());
    auto [status3, importedVersionId1] = masterHelper.Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_EQ(importedVersionId1, 536870912);
    ASSERT_TRUE(status3.IsOK());
    framework::Version importedVersion1 =
        framework::TabletTestAgent(masterHelper.GetTablet()).TEST_GetVersion(importedVersionId1);
    ASSERT_EQ(importedVersion1.GetLocator().DebugString(), "0:2:[{0,16383,2,0}{32768,49151,4,0}]");
    ASSERT_TRUE(importedVersion1.HasSegment(536870912) && importedVersion1.HasSegment(536870914));
    ASSERT_EQ(importedVersion1.GetTimestamp(), 4);
    ASSERT_TRUE(masterHelper.Reopen(importedVersionId1).IsOK());
    ASSERT_TRUE(masterHelper.GetTablet()->Import({version0, version2}, _importOptions).IsOK());
    ASSERT_FALSE(masterHelper.NeedCommit());

    // slave builder 1
    KVTableTestHelper helper1(/*autoCleanIndex=*/true, /*needDeploy=*/false);
    helper1.SetIdGenerator(MakeIdGenerator({536870913, 536870913}, {1073741825}));
    ASSERT_TRUE(helper1.Open(indexRoot, _tabletSchema, offlineTabletOptions, INVALID_VERSIONID).IsOK());
    helper1.GetTaskScheduler()->CleanTasks();

    ASSERT_TRUE(helper1.Build("cmd=add,string1=1_1,string2=b5,ts=5;", MakeLocator(0, 5, {{16384, 32767, 5}})).IsOK());
    ASSERT_TRUE(helper1.Build("cmd=add,string1=1_2,string2=b6,ts=6;", MakeLocator(0, 6, {{16384, 32767, 6}})).IsOK());
    ASSERT_TRUE(helper1.Flush().IsOK());
    ASSERT_TRUE(helper1.NeedCommit());
    auto [status4, versionId1] = helper1.Commit(framework::CommitOptions().SetNeedPublish(false));
    ASSERT_EQ(versionId1, 1073741825);
    ASSERT_TRUE(status4.IsOK());
    framework::Version version1 =
        framework::TabletTestAgent(helper1.GetTablet())
            .TEST_GetVersion(VersionCoord(
                versionId1, framework::TabletTestAgent(helper1.GetTablet()).TEST_GetFence().GetFenceName()));

    // slave builder 3
    KVTableTestHelper helper3(/*autoCleanIndex=*/true, /*needDeploy=*/false);
    helper3.SetIdGenerator(MakeIdGenerator({536870915, 536870915}, {1073741827}));
    ASSERT_TRUE(helper3.Open(indexRoot, _tabletSchema, offlineTabletOptions, INVALID_VERSIONID).IsOK());
    helper3.GetTaskScheduler()->CleanTasks();

    ASSERT_TRUE(helper3.Build("cmd=add,string1=3_1,string2=d7,ts=7;", MakeLocator(0, 7, {{49152, 65535, 7}})).IsOK());
    ASSERT_TRUE(helper3.Build("cmd=add,string1=3_2,string2=d8,ts=8;", MakeLocator(0, 8, {{49152, 65535, 8}})).IsOK());
    ASSERT_TRUE(helper3.Flush().IsOK());
    ASSERT_TRUE(helper3.NeedCommit());
    auto [status5, versionId3] = helper3.Commit(framework::CommitOptions().SetNeedPublish(false));
    ASSERT_EQ(versionId3, 1073741827);
    ASSERT_TRUE(status5.IsOK());
    framework::Version version3 =
        framework::TabletTestAgent(helper3.GetTablet())
            .TEST_GetVersion(VersionCoord(
                versionId3, framework::TabletTestAgent(helper3.GetTablet()).TEST_GetFence().GetFenceName()));

    // slave builder 0 update && repeat add
    ASSERT_TRUE(
        helper0.Build("cmd=update_field,string1=0_1,string2=a9,ts=9;", MakeLocator(0, 9, {{0, 16383, 9}})).IsOK());
    ASSERT_TRUE(helper0.Build("cmd=add,string1=0_2,string2=a10,ts=10;", MakeLocator(0, 10, {{0, 16383, 10}})).IsOK());
    ASSERT_TRUE(helper0.Flush().IsOK());
    ASSERT_TRUE(helper0.NeedCommit());
    auto [status6, versionId4] = helper0.Commit(framework::CommitOptions().SetNeedPublish(false));
    ASSERT_EQ(versionId4, 1073741828);
    ASSERT_TRUE(status6.IsOK());
    framework::Version version4 =
        framework::TabletTestAgent(helper0.GetTablet())
            .TEST_GetVersion(VersionCoord(
                versionId4, framework::TabletTestAgent(helper0.GetTablet()).TEST_GetFence().GetFenceName()));

    // import second time
    ASSERT_TRUE(masterHelper.GetTablet()->Import({version4, version1, version2, version3}, _importOptions).IsOK());
    ASSERT_TRUE(masterHelper.NeedCommit());
    auto [status7, importedVersionId2] = masterHelper.Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_EQ(importedVersionId2, 536870913);
    ASSERT_TRUE(status7.IsOK());
    framework::Version importedVersion2 =
        framework::TabletTestAgent(masterHelper.GetTablet()).TEST_GetVersion(importedVersionId2);
    ASSERT_EQ(importedVersion2.GetLocator().DebugString(),
              "0:4:[{0,16383,10,0}{16384,32767,6,0}{32768,49151,4,0}{49152,65535,8,0}]");
    ASSERT_TRUE(importedVersion2.HasSegment(536870912) && importedVersion2.HasSegment(536870913) &&
                importedVersion2.HasSegment(536870914) && importedVersion2.HasSegment(536870915) &&
                importedVersion2.HasSegment(536870916));
    ASSERT_EQ(importedVersion2.GetTimestamp(), 10);
    ASSERT_TRUE(masterHelper.Reopen(importedVersionId2).IsOK());
    ASSERT_TRUE(masterHelper.GetTablet()->Import({version4, version1, version2, version3}, _importOptions).IsOK());
    ASSERT_FALSE(masterHelper.NeedCommit());

    // align version with versionId 536870915
    auto [status8, alignVersionId] =
        masterHelper.Commit(framework::CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870915));
    ASSERT_EQ(alignVersionId, 536870915);
    ASSERT_TRUE(status8.IsOK());

    // online search, need deploy
    KVTableTestHelper onlineHelper(/*autoCleanIndex=*/true, /*needDeploy=*/true);
    auto onlineTabletOptions = std::make_shared<config::TabletOptions>();
    onlineTabletOptions->SetFlushLocal(true);
    onlineTabletOptions->SetFlushRemote(false);
    onlineTabletOptions->TEST_GetOnlineConfig().TEST_SetNeedReadRemoteIndex(true);

    ASSERT_TRUE(onlineHelper.Open(indexRoot, _tabletSchema, onlineTabletOptions).IsOK());
    onlineHelper.GetTaskScheduler()->CleanTasks();
    ASSERT_TRUE(onlineHelper.Reopen(alignVersionId).IsOK());
    ASSERT_TRUE(onlineHelper.Query("kv", "string1", "0_1", "string2=a9"));
    ASSERT_TRUE(onlineHelper.Query("kv", "string1", "0_2", "string2=a10"));
    ASSERT_TRUE(onlineHelper.Query("kv", "string1", "1_1", "string2=b5"));
    ASSERT_TRUE(onlineHelper.Query("kv", "string1", "1_2", "string2=b6"));
    ASSERT_TRUE(onlineHelper.Query("kv", "string1", "2_1", "string2=c3"));
    ASSERT_TRUE(onlineHelper.Query("kv", "string1", "2_2", "string2=c4"));
    ASSERT_TRUE(onlineHelper.Query("kv", "string1", "3_1", "string2=d7"));
    ASSERT_TRUE(onlineHelper.Query("kv", "string1", "3_2", "string2=d8"));
}

TEST_P(KVTabletInteTest, TestBuildNoMem)
{
    TabletResource resource;
    auto memController = std::make_shared<MemoryQuotaController>("", /*totalQuota=*/128 * 1024 * 1024);
    memController->Allocate(128 * 1024 * 1024);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->SetFlushLocal(false);
    helper.SetMemoryQuotaController(memController, nullptr);
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, tabletOptions).IsOK());
    ASSERT_TRUE(helper.Build(docString1()).IsNoMem());
    ASSERT_TRUE(helper.Build(docString1()).IsNoMem());
    ASSERT_TRUE(helper.Build(docString1()).IsNoMem());
    ASSERT_TRUE(helper.Build(docString1()).IsNoMem());
    memController->Free(128 * 1024 * 1024);
    ASSERT_FALSE(helper.BuildSegment(docString1()).IsOK());
    helper.GetTablet()->ReportMetrics(helper.GetTablet()->GetTabletWriter());
    ASSERT_TRUE(helper.BuildSegment(docString1()).IsOK());
    auto version = helper.GetCurrentVersion();
    ASSERT_EQ(1u, version.GetSegmentCount());
    ASSERT_EQ(0 | framework::Segment::PUBLIC_SEGMENT_ID_MASK, version[0]);
}

TEST_P(KVTabletInteTest, TestBuildNoMemWithDump)
{
    TabletResource resource;
    auto memController = std::make_shared<MemoryQuotaController>("", /*totalQuota=*/128 * 1024 * 1024);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetMaxDocCount(4);
    helper.SetMemoryQuotaController(memController, nullptr);
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, tabletOptions).IsOK());
    ASSERT_TRUE(helper.Build(docString1()).IsOK()) << "quota: " << memController->GetFreeQuota();
    auto allocateMem = memController->GetFreeQuota() - 20;
    memController->Allocate(allocateMem);
    ASSERT_TRUE(helper.Build(docString2()).IsNoMem());
    while (!helper.NeedCommit()) {};
    ASSERT_TRUE(helper.NeedCommit());
    ASSERT_TRUE(helper.Build(docString2()).IsNoMem());
    ASSERT_TRUE(helper.Build(docString2()).IsNoMem());
    auto tablet = helper.GetTablet();
    auto dumper = framework::TabletTestAgent(tablet).TEST_GetTabletDumper();
    ASSERT_EQ(0u, dumper->GetDumpQueueSize());

    auto [status, version] = helper.Commit();
    ASSERT_TRUE(status.IsOK());
    memController->Free(allocateMem);
    ASSERT_TRUE(helper.Reopen(version).IsOK());
    memController->Allocate(allocateMem);
    while (memController->GetFreeQuota() <= 20) {}
    ASSERT_TRUE(memController->GetFreeQuota() > 20);
}

TEST_P(KVTabletInteTest, TestBuildingSegmentMemCheck)
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 16,
            "level_num" : 3
        }
    }
    } )";
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, tabletOptions).IsOK());
    ASSERT_TRUE(helper.BuildSegment(docString1()).IsOK());
    auto tablet = helper.GetTablet();
    auto memController = framework::TabletTestAgent(tablet).TEST_GetTabletMemoryQuotaController();
    auto allocateQuota = memController->GetAllocatedQuota();
    std::cout << "hello : " << GET_CASE_PARAM() << " " << allocateQuota << std::endl;
    ASSERT_LT(allocateQuota, 128 * 1024 * 1024);
    ASSERT_GT(allocateQuota, 50 * 1024 * 1024); // UNSTABLE, allocateQuota may be 0
    ASSERT_TRUE(helper.BuildSegment(docString2()).IsOK());
    ASSERT_LT(allocateQuota, 128 * 1024 * 1024); // no need refresh?
    ASSERT_GT(allocateQuota, 50 * 1024 * 1024);
}

TEST_P(KVTabletInteTest, TestBuildWithDiffSrcSegment)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    tabletOptions->SetIsOnline(true);
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions)).IsOK());
    auto docs = "cmd=add,string1=1,string2=1,ts=1,locator=0:1;"
                "cmd=add,string1=2,string2=2,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    docs = "cmd=add,string1=3,string2=3,ts=3,locator=1:3;"
           "cmd=add,string1=4,string2=4,ts=4,locator=1:4;";
    ASSERT_TRUE(helper.Build(docs).IsOK());
    ASSERT_TRUE(helper.Merge().IsOK());
    // when reopen with diff src drop rt
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=1"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "string2=2"));
    ASSERT_FALSE(helper.Query("kv", "string1", "3", "string2=3"));
    ASSERT_FALSE(helper.Query("kv", "string1", "4", "string2=4"));
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    docs = "cmd=add,string1=5,string2=5,ts=5,locator=1:5;"
           "cmd=add,string1=6,string2=6,ts=6,locator=1:6;";
    ASSERT_TRUE(helper.Build(docs).IsOK());
    ASSERT_TRUE(helper.Merge().IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=1"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "string2=2"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=3"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=4"));
    ASSERT_TRUE(helper.Query("kv", "string1", "5", "string2=5"));
    ASSERT_TRUE(helper.Query("kv", "string1", "6", "string2=6"));
}

TEST_P(KVTabletInteTest, TestOnlyBuildingSegment)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions)).IsOK());
    ASSERT_TRUE(helper.Build(docString1()).IsOK());
    // check result
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=abc"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "string2=cde"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=fgh"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=ijk"));

    // test dumping & building segment
    ASSERT_TRUE(helper.Flush().IsOK());
    ASSERT_TRUE(helper.Build(docString2()).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", ""));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "12", "string2=Acde"));
    ASSERT_TRUE(helper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "15", ""));
}

TEST_P(KVTabletInteTest, TestReopenCoverNoRt)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper mainHelper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(tabletOptions)).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docString1()).IsOK());
    auto tabletOptions1 = CreateOnlineMultiShardOptions();
    tabletOptions1->SetIsLeader(false);
    tabletOptions1->SetFlushLocal(true);
    tabletOptions1->SetFlushRemote(false);
    framework::IndexRoot indexRoot1(GET_TEMP_DATA_PATH() + "/private_dir", GET_TEMP_DATA_PATH());
    KVTableTestHelper followHelper;
    ASSERT_TRUE(followHelper.Open(indexRoot1, _tabletSchema, std::move(tabletOptions1)).IsOK());
    ASSERT_TRUE(followHelper.Build(docString2()).IsOK());
    ASSERT_TRUE(followHelper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "2", ""));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "4", ""));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "12", "string2=Acde"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "15", ""));

    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(followHelper.Reopen(version.GetVersionId()).IsOK());
    ASSERT_TRUE(followHelper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "2", ""));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "12", "string2=Acde"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "15", ""));
}

TEST_P(KVTabletInteTest, TestReopenCoverPartRt)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper mainHelper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(tabletOptions)).IsOK());
    std::string docs = "cmd=add,string1=1,string2=abc,ts=5,locator=0:5;"
                       "cmd=add,string1=4,string2=Aijk,ts=6,locator=0:6;"
                       "cmd=add,string1=2,string2=Aijk,ts=6,locator=0:11;";
    ASSERT_TRUE(mainHelper.BuildSegment(docs).IsOK());
    auto tabletOptions1 = CreateOnlineMultiShardOptions();
    tabletOptions1->SetIsLeader(false);
    tabletOptions1->SetFlushLocal(true);
    tabletOptions1->SetFlushRemote(false);
    framework::IndexRoot indexRoot1(GET_TEMP_DATA_PATH() + "/private_dir", GET_TEMP_DATA_PATH());
    KVTableTestHelper followHelper;
    ASSERT_TRUE(followHelper.Open(indexRoot1, _tabletSchema, std::move(tabletOptions1)).IsOK());
    ASSERT_TRUE(followHelper.BuildSegment(docString1()).IsOK());
    std::string docs2 = "cmd=add,string1=1,string2=ijk,ts=5,locator=0:5;"
                        "cmd=add,string1=12,string2=Acde,ts=6,locator=0:6;"
                        "cmd=add,string1=3,string2=Afgh,ts=7,locator=0:7;"
                        "cmd=add,string1=14,string2=Aijk,ts=8,locator=0:8;"
                        "cmd=add,string1=15,string2=15,ts=9,locator=0:9;"
                        "cmd=delete,string1=15,ts=10,locator=0:10;"
                        "cmd=delete,string1=2,ts=13,locator=0:12;";
    ASSERT_TRUE(followHelper.BuildSegment(docs2).IsOK());

    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(followHelper.Reopen(version.GetVersionId()).IsOK());
    ASSERT_TRUE(followHelper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "2", ""));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "4", "string2=Aijk"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "12", "string2=Acde"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "15", ""));
}

TEST_P(KVTabletInteTest, TestReopenCoverAllRt)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper mainHelper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(tabletOptions)).IsOK());
    std::string docs = "cmd=add,string1=1,string2=abc,ts=5,locator=0:10;"
                       "cmd=add,string1=4,string2=Aijk,ts=6,locator=0:11;"
                       "cmd=add,string1=2,string2=Aijk,ts=6,locator=0:12;";
    ASSERT_TRUE(mainHelper.BuildSegment(docs).IsOK());
    auto tabletOptions1 = CreateOnlineMultiShardOptions();
    tabletOptions1->SetIsLeader(false);
    tabletOptions1->SetFlushLocal(true);
    tabletOptions1->SetFlushRemote(false);
    framework::IndexRoot indexRoot1(GET_TEMP_DATA_PATH() + "/private_dir", GET_TEMP_DATA_PATH());
    KVTableTestHelper followHelper;
    ASSERT_TRUE(followHelper.Open(indexRoot1, _tabletSchema, std::move(tabletOptions1)).IsOK());
    ASSERT_TRUE(followHelper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(followHelper.BuildSegment(docString2()).IsOK());

    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(followHelper.Reopen(version.GetVersionId()).IsOK());
    ASSERT_TRUE(followHelper.Query("kv", "string1", "1", "string2=abc"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "2", "string2=Aijk"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "3", ""));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "4", "string2=Aijk"));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "12", ""));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "14", ""));
    ASSERT_TRUE(followHelper.Query("kv", "string1", "15", ""));
}

TEST_P(KVTabletInteTest, TestCalculateFileSize)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper mainHelper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(tabletOptions)).IsOK());
    std::string docs = "cmd=add,string1=1,string2=abc,ts=5,locator=0:10;"
                       "cmd=add,string1=4,string2=Aijk,ts=6,locator=0:11;"
                       "cmd=add,string1=2,string2=Aijk,ts=6,locator=0:12;";
    ASSERT_TRUE(mainHelper.BuildSegment(docs).IsOK());
    auto tabletOptions1 = CreateOnlineMultiShardOptions();
    tabletOptions1->SetIsLeader(false);
    tabletOptions1->SetFlushLocal(true);
    tabletOptions1->SetFlushRemote(false);
    framework::IndexRoot indexRoot1(GET_TEMP_DATA_PATH() + "/private_dir", GET_TEMP_DATA_PATH());
    KVTableTestHelper followHelper;
    ASSERT_TRUE(followHelper.Open(indexRoot1, _tabletSchema, std::move(tabletOptions1)).IsOK());
    ASSERT_TRUE(followHelper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(followHelper.BuildSegment(docString2()).IsOK());

    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(followHelper.Reopen(version.GetVersionId()).IsOK());

    auto fs = followHelper.GetTablet()->_fence._fileSystem;
    auto lfs = std::dynamic_pointer_cast<indexlib::file_system::LogicalFileSystem>(fs);

    std::string ip = autil::NetUtil::getBindIp(); // ip in version file
    auto fileSize = lfs->GetVersionFileSize(version.GetVersionId());
    if (GetParam() == "plain" || GetParam() == "impact") {
        ASSERT_EQ(339752 + ip.size() * 2, fileSize);
    } else {
        ASSERT_EQ(339778 + ip.size() * 2, fileSize);
    }
}

TEST_P(KVTabletInteTest, TestSimple)
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "level_num" : 3
        }
    }
    } )";
    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    // test init
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());
    // test build
    ASSERT_TRUE(helper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(helper.BuildSegment(docString2()).IsOK());
    // test level info
    auto tablet = helper.GetTablet();
    std::stringstream ss;
    ss << "0:" << framework::Segment::PRIVATE_SEGMENT_ID_MASK + 1 << "," << framework::Segment::PRIVATE_SEGMENT_ID_MASK;
    CheckLevelInfo(tablet, 3, 1, framework::topo_hash_mod, ss.str());

    // check result
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", ""));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "12", "string2=Acde"));
    ASSERT_TRUE(helper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "15", ""));
    // test building segment
    ASSERT_TRUE(helper.Build(docString1()).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=abc"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "string2=cde"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=fgh"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "12", "string2=Acde"));
    ASSERT_TRUE(helper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "15", ""));
    // test building & dumping segment
    ASSERT_TRUE(tablet->Flush().IsOK());
    ASSERT_TRUE(helper.Build(docString3()).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "string2=cde"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "12", "string2=12"));
    ASSERT_TRUE(helper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "15", "string2=15"));
}

TEST_P(KVTabletInteTest, TestMultiShardEmptyIndex)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(_tabletOptions)).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", ""));
}

TEST_P(KVTabletInteTest, TestMultiShardBuild)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    // test init
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(_tabletOptions)).IsOK());
    // test build
    ASSERT_TRUE(helper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(helper.BuildSegment(docString2()).IsOK());
    // test level info
    auto tablet = helper.GetTablet();
    std::stringstream ss;
    ss << "0:" << framework::Segment::PRIVATE_SEGMENT_ID_MASK + 1 << "," << framework::Segment::PRIVATE_SEGMENT_ID_MASK;
    CheckLevelInfo(tablet, 3, 2, framework::topo_hash_mod, ss.str());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", ""));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "12", "string2=Acde"));
    ASSERT_TRUE(helper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "15", ""));

    // test building segment
    ASSERT_TRUE(helper.Build(docString1()).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=abc"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "string2=cde"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=fgh"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "12", "string2=Acde"));
    ASSERT_TRUE(helper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "15", ""));
    // test building & dumping segment
    ASSERT_TRUE(tablet->Flush().IsOK());
    ASSERT_TRUE(helper.Build(docString3()).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "string2=cde"));
    ASSERT_TRUE(helper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "12", "string2=12"));
    ASSERT_TRUE(helper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "15", "string2=15"));
}

// levelSegments:levelIdx:segId,segId...;
void KVTabletInteTest::CheckLevelInfo(const std::shared_ptr<Tablet>& tablet, uint32_t levelCount, uint32_t shardCount,
                                      LevelTopology topology, std::string levelSegmentStr)
{
    auto version = framework::TabletTestAgent(tablet).TEST_GetTabletData()->GetOnDiskVersion();
    auto segDescs = version.GetSegmentDescriptions();
    auto levelInfo = segDescs->GetLevelInfo();
    ASSERT_TRUE(levelInfo);
    ASSERT_EQ(levelCount, levelInfo->GetLevelCount());
    ASSERT_EQ(shardCount, levelInfo->GetShardCount());
    ASSERT_EQ(topology, levelInfo->GetTopology());
    std::vector<std::string> levelSegments;
    autil::StringUtil::fromString(levelSegmentStr, levelSegments, ";");
    for (size_t i = 0; i < levelSegments.size(); i++) {
        std::vector<std::vector<uint32_t>> oneLevelInfos;
        autil::StringUtil::fromString(levelSegments[i], oneLevelInfos, ",", ":");
        uint32_t levelIdx = oneLevelInfos[0][0];
        auto segments = levelInfo->GetSegmentIds(levelIdx);
        auto& expectSegments = oneLevelInfos[1];
        ASSERT_EQ(expectSegments.size(), segments.size());
        for (size_t j = 0; j < segments.size(); j++) {
            ASSERT_EQ(expectSegments[j], segments[j]) << j;
        }
    }
}

TEST_P(KVTabletInteTest, TestOffsetLimitTriggerForceDump)
{
    // 10mb for each segment
    size_t maxValueFileSize = 500 * 1024;
    // short offset
    // totalDocSize > one segment size
    InnerTestBuildForceDump(false, true, maxValueFileSize, "dense", maxValueFileSize * 3, 3, "dir1");
    // totalDocSize <= one segment size
    InnerTestBuildForceDump(false, true, maxValueFileSize, "dense", maxValueFileSize, 1, "dir2");
    // totalDocSize > one segment size
    InnerTestBuildForceDump(false, true, maxValueFileSize, "cuckoo", maxValueFileSize * 3, 3, "dir3");
    // totalDocSize <= one segment size
    InnerTestBuildForceDump(false, true, maxValueFileSize, "cuckoo", maxValueFileSize, 1, "dir4");

    // normal offset
    InnerTestBuildForceDump(false, false, maxValueFileSize, "dense", maxValueFileSize * 2, 1, "dir5");
    InnerTestBuildForceDump(false, false, maxValueFileSize, "cuckoo", maxValueFileSize * 2, 1, "dir7");
}

void KVTabletInteTest::InnerTestBuildForceDump(bool enableCompactHashKey, bool enableShortOffset,
                                               size_t maxValueFileSize, std::string hashType, size_t totalDocSize,
                                               size_t segmentNum, std::string dirName)
{
    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit((int64_t)2 * 1024 * 1024 *
                                                                                            1024); // 2G
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetEnablePackageFile(false);  // 2G
    tabletOptions->TEST_GetOnlineConfig().TEST_SetNeedDeployIndex(false);
    tabletOptions->TEST_GetOnlineConfig().TEST_SetNeedReadRemoteIndex(true);
    tabletOptions->TEST_GetOnlineConfig().MutableLoadConfigList().SetLoadMode(
        indexlib::file_system::LoadConfig::LoadMode::REMOTE_ONLY);

    std::string field = "key:int32;value:string:false;";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "key", "value;key");
    auto indexConfigs = tabletSchema->GetIndexConfigs();
    assert(indexConfigs.size() == 1);
    std::shared_ptr<indexlibv2::config::KVIndexConfig> kvConfig =
        std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfigs[0]);
    indexlib::config::KVIndexPreference& kvIndexPreference = kvConfig->GetIndexPreference();
    const indexlib::config::KVIndexPreference::HashDictParam& dictParam = kvIndexPreference.GetHashDictParam();
    indexlib::config::KVIndexPreference::HashDictParam newDictParam(hashType, dictParam.GetOccupancyPct(),
                                                                    dictParam.UsePreciseCountWhenMerge(),
                                                                    enableCompactHashKey, enableShortOffset);
    newDictParam.SetMaxValueSizeForShortOffset(maxValueFileSize);
    kvConfig->GetIndexPreference().SetHashDictParam(newDictParam);
    // prepare doc
    size_t bufLen = maxValueFileSize / 1000;
    static const size_t VAR_NUM_ATTRIBUTE_MAX_COUNT = 65535;
    bufLen = bufLen > VAR_NUM_ATTRIBUTE_MAX_COUNT - 20 ? VAR_NUM_ATTRIBUTE_MAX_COUNT - 20 : bufLen;
    char* buffer = new char[bufLen];

    // size_t totalDocSize = segmentNum * maxValueFileSize;
    std::stringstream fullDocBuffer;
    memset(buffer, 'a', bufLen);
    std::string seperator = ";";
    int32_t i = 1;
    while (fullDocBuffer.str().size() < totalDocSize) {
        std::string docValue = "cmd=add,key=" + autil::StringUtil::toString(i);
        docValue += ",ts=1,value=";
        fullDocBuffer << docValue;
        fullDocBuffer << std::string(buffer, bufLen) << seperator;
        i++;
    }
    std::string fullDoc(fullDocBuffer.str().c_str(), fullDocBuffer.str().size());
    std::string rootDir = GET_TEMP_DATA_PATH() + "/" + dirName;
    framework::IndexRoot indexRoot(rootDir, rootDir);

    KVTableTestHelper helper;
    // todo: wait kv indexer support
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());
    ASSERT_TRUE(helper.Build(fullDoc).IsOK());
    auto tablet = helper.GetTablet();
    ASSERT_TRUE(tablet->Seal().IsOK());
    ASSERT_TRUE(tablet->NeedCommit());
    auto pair = tablet->Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(pair.first.IsOK());

    auto partDir = indexlib::file_system::Directory::GetPhysicalDirectory(
        PathUtil::JoinPath(rootDir, Fence::GetFenceName(RT_INDEX_PARTITION_DIR_NAME)));
    for (size_t i = 0; i < segmentNum; i++) {
        std::string segDirName =
            "segment_" + autil::StringUtil::toString(framework::Segment::PRIVATE_SEGMENT_ID_MASK + i) + "_level_0";
        ASSERT_TRUE(partDir->IsDir(segDirName)) << segDirName;
        if (enableShortOffset) {
            std::string filePath = segDirName + "/index/key/format_option";
            std::string formatStr;
            ASSERT_NO_THROW(partDir->Load(filePath, formatStr));

            index::KVFormatOptions kvOptions;
            kvOptions.FromString(formatStr);
            ASSERT_TRUE(kvOptions.IsShortOffset());
        }
    }
    ASSERT_FALSE(partDir->IsExist(
        "segment_" + autil::StringUtil::toString(framework::Segment::PRIVATE_SEGMENT_ID_MASK + segmentNum) +
        "_level_0"));
    delete[] buffer;
}

TEST_P(KVTabletInteTest, TestUpdate2Add4Offline)
{
    string docs;
    std::string field = "string1:string;string2:string;string3:string;"
                        "string4:string;int5:int32;float6:float;double7:double:true";
    auto tabletSchema =
        table::KVTabletSchemaMaker::Make(field, "string1", "string2;string3;string4;int5;float6;double7");
    framework::IndexRoot indexRoot = GetIndexRoot();
    auto offlineTabletOptions = CreateOfflineMultiShardOptions();
    KVTableTestHelper helper(/*autoCleanIndex*/ false, /*needDeploy*/ false);
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(offlineTabletOptions)).IsOK());
    auto onlineTabletOptions = CreateOnlineMultiShardOptions();
    KVTableTestHelper queryHelper(/*autoCleanIndex*/ false, /*needDeploy*/ true);
    ASSERT_TRUE(queryHelper.Open(indexRoot, tabletSchema, std::move(onlineTabletOptions)).IsOK());
    std::string fenceName = framework::TabletTestAgent(helper.GetTablet()).TEST_GetFence().GetFenceName();

    // add key=1
    docs = "cmd=add,string1=1,string2=abc,string3=cde,string4=pot,int5=12,float6=3.4,double7=1.2 2.3,ts=1,locator=0:1;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(
        queryHelper.Reopen(framework::VersionCoord(helper.GetCurrentVersion().GetVersionId(), fenceName)).IsOK());
    ASSERT_TRUE(queryHelper.Query("kv", "string1", "1",
                                  "string2=abc,string3=cde,string4=pot,int5=12,float6=3.4,double7=1.2 2.3"));

    // add and update key=2
    docs = "cmd=add,string1=2,string2=345,string3=678,string4=wsd,int5=34,float6=4.5,double7=2.3 3.4,ts=2,locator=0:2;"
           "cmd=update_field,string1=2,string2=789,string4=pstxyz,int5=67,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(
        queryHelper.Reopen(framework::VersionCoord(helper.GetCurrentVersion().GetVersionId(), fenceName)).IsOK());
    ASSERT_TRUE(queryHelper.Query("kv", "string1", "2",
                                  "string2=789,string3=678,string4=pstxyz,int5=67,float6=4.5,double7=2.3 3.4"));

    // update key=2 again
    docs =
        "cmd=update_field,string1=2,string2=789,string4=othx,int5=0,float6=0.0,double7=3.4 5.6 7.8,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(
        queryHelper.Reopen(framework::VersionCoord(helper.GetCurrentVersion().GetVersionId(), fenceName)).IsOK());
    ASSERT_TRUE(queryHelper.Query("kv", "string1", "2",
                                  "string2=789,string3=678,string4=othx,int5=0,float6=0,double7=3.4 5.6 7.8"));

    // update key=2 again from disk segment
    docs = "cmd=update_field,string1=2,string2=456,string4=opt,int5=34,float6=65.8,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(
        queryHelper.Reopen(framework::VersionCoord(helper.GetCurrentVersion().GetVersionId(), fenceName)).IsOK());
    ASSERT_TRUE(queryHelper.Query("kv", "string1", "2",
                                  "string2=456,string3=678,string4=opt,int5=34,float6=65.8,double7=3.4 5.6 7.8"));

    // add key2 again
    docs = "cmd=add,string1=2,string2=abc,string3=def,string4=wsd,int5=90,float6=124.5,double7=0.5,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(
        queryHelper.Reopen(framework::VersionCoord(helper.GetCurrentVersion().GetVersionId(), fenceName)).IsOK());
    ASSERT_TRUE(queryHelper.Query("kv", "string1", "2",
                                  "string2=abc,string3=def,string4=wsd,int5=90,float6=124.5,double7=0.5"));

    // delete key=2
    docs = "cmd=delete,string1=2,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(
        queryHelper.Reopen(framework::VersionCoord(helper.GetCurrentVersion().GetVersionId(), fenceName)).IsOK());
    ASSERT_TRUE(queryHelper.Query("kv", "string1", "2", ""));

    // update key=2 again, not exist key, update return true, it has been dropped
    docs = "cmd=update_field,string1=2,string2=789,string4=othx,int5=0,float6=0.0,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(
        queryHelper.Reopen(framework::VersionCoord(helper.GetCurrentVersion().GetVersionId(), fenceName)).IsOK());
    ASSERT_TRUE(queryHelper.Query("kv", "string1", "2", ""));

    // key=3, not exist key, update return true, it has been dropped
    docs = "cmd=update_field,string1=3,string2=789,string4=othx,int5=0,float6=7.8,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(
        queryHelper.Reopen(framework::VersionCoord(helper.GetCurrentVersion().GetVersionId(), fenceName)).IsOK());
    ASSERT_TRUE(queryHelper.Query("kv", "string1", "3", ""));
}

TEST_P(KVTabletInteTest, TestUpdate2Add4Online)
{
    string docs;
    std::string field = "string1:string;string2:string;string3:string;"
                        "string4:string;int5:int32;float6:float;double7:double:true";
    auto tabletSchema =
        table::KVTabletSchemaMaker::Make(field, "string1", "string2;string3;string4;int5;float6;double7");
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());

    // add key=1
    docs = "cmd=add,string1=1,string2=abc,string3=cde,string4=pot,int5=12,float6=3.4,double7=1.2 2.3,ts=1,locator=0:1;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/false).IsOK());
    ASSERT_TRUE(
        helper.Query("kv", "string1", "1", "string2=abc,string3=cde,string4=pot,int5=12,float6=3.4,double7=1.2 2.3"));

    // add and update key=2
    docs = "cmd=add,string1=2,string2=345,string3=678,string4=wsd,int5=34,float6=4.5,double7=2.3 3.4,ts=2,locator=0:2;"
           "cmd=update_field,string1=2,string2=789,string4=pstxyz,int5=67,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/false).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "2",
                             "string2=789,string3=678,string4=pstxyz,int5=67,float6=4.5,double7=2.3 3.4"));

    // update key=2 again
    docs =
        "cmd=update_field,string1=2,string2=789,string4=othx,int5=0,float6=0.0,double7=3.4 5.6 7.8,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(
        helper.Query("kv", "string1", "2", "string2=789,string3=678,string4=othx,int5=0,float6=0,double7=3.4 5.6 7.8"));

    // update key=2 again from disk segment
    docs = "cmd=update_field,string1=2,string2=456,string4=opt,int5=34,float6=65.8,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/false).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "2",
                             "string2=456,string3=678,string4=opt,int5=34,float6=65.8,double7=3.4 5.6 7.8"));

    // add key2 again
    docs = "cmd=add,string1=2,string2=abc,string3=def,string4=wsd,int5=90,float6=124.5,double7=0.5,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/false).IsOK());
    ASSERT_TRUE(
        helper.Query("kv", "string1", "2", "string2=abc,string3=def,string4=wsd,int5=90,float6=124.5,double7=0.5"));

    // delete key=2
    docs = "cmd=delete,string1=2,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/false).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "2", ""));

    // update key=2 again, not exist key, update return true, it has been dropped
    docs = "cmd=update_field,string1=2,string2=789,string4=othx,int5=0,float6=0.0,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/false).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "2", ""));

    // key=3, not exist key, update return true, it has been dropped
    docs = "cmd=update_field,string1=3,string2=789,string4=othx,int5=0,float6=7.8,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/false).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "3", ""));
}

TEST_P(KVTabletInteTest, TestUpdate2AddInSameBatch)
{
    string docs;
    std::string field =
        "string1:string;string2:string;string3:string;string4:string;int5:int32;float6:float;double7:double:true";
    auto tabletSchema =
        table::KVTabletSchemaMaker::Make(field, "string1", "string2;string3;string4;int5;float6;double7");
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());

    // add -> update key=1 in same batch
    docs = "cmd=add,string1=1,string2=345,string3=678,string4=wsd,int5=34,float6=4.5,double7=2.3 3.4,ts=2,locator=0:2;"
           "cmd=update_field,string1=1,string2=789,string4=pstxyz,int5=67,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/true).IsOK());
    // string3, float6 is old value.
    ASSERT_TRUE(helper.Query("kv", "string1", "1",
                             "string2=789,string3=678,string4=pstxyz,int5=67,float6=4.5,double7=2.3 3.4"));

    // add -> add -> update key=1 in same batch
    docs = "cmd=add,string1=1,string2=345,string3=678,string4=wsd,int5=34,float6=4.5,double7=4.5 6.7,ts=2,locator=0:2;"
           "cmd=add,string1=1,string2=abc,string3=ijk,string4=pstxyz,int5=56,float6=130.6,double7=7.8 "
           "8.9,ts=2,locator=0:2;"
           "cmd=update_field,string1=1,string2=bcde,string4=pstw,int5=0,double7=3.5 5.7 6.9,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/true).IsOK());
    // string3, float6 is old value
    ASSERT_TRUE(helper.Query("kv", "string1", "1",
                             "string2=bcde,string3=ijk,string4=pstw,int5=0,float6=130.6,double7=3.5 5.7 6.9"));

    // add -> delete -> update key=1 in same batch
    docs = "cmd=add,string1=1,string2=345,string3=678,string4=wsd,int5=34,float6=4.5,double7=1.4 2.5,ts=2,locator=0:2;"
           "cmd=delete,string1=1,ts=2,locator=0:2;"
           "cmd=update_field,string1=1,string2=789,string4=pst,int5=67,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, true).IsOK());
    // update document is dropped
    ASSERT_TRUE(helper.Query("kv", "string1", "1", ""));

    // add -> delete -> update -> update key=1 in same batch
    docs = "cmd=add,string1=1,string2=345,string3=678,string4=wsd,int5=34,float6=4.5,double7=1.5 2.6,ts=2,locator=0:2;"
           "cmd=delete,string1=1,ts=2,locator=0:2;"
           "cmd=update_field,string1=1,string2=567,string4=xyz,int5=34,ts=46,locator=0:2;"
           "cmd=update_field,string1=1,string2=789,string4=pst,int5=67,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/true).IsOK());
    // update document is dropped
    ASSERT_TRUE(helper.Query("kv", "string1", "1", ""));

    // add -> delete -> add -> update key=1 in same batch
    docs = "cmd=add,string1=1,string2=345,string3=678,string4=wsd,int5=34,float6=4.5,double7=1.6 2.7,ts=2,locator=0:2;"
           "cmd=delete,string1=1,ts=2,locator=0:2;"
           "cmd=add,string1=1,string2=abc,string3=ijk,string4=pst,int5=56,float6=130.6,ts=2,locator=0:2;"
           "cmd=update_field,string1=1,string2=bcde,string4=pstw,int5=0,double7=1.7 2.8,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/true).IsOK());
    // string3, float6 is old value
    ASSERT_TRUE(helper.Query("kv", "string1", "1",
                             "string2=bcde,string3=ijk,string4=pstw,int5=0,float6=130.6,double7=1.7 2.8"));

    // add -> delete -> add -> update -> update key=1 in same batch
    docs =
        "cmd=add,string1=1,string2=345,string3=678,string4=wsd,int5=34,float6=4.5,double7=1.4 2.5,ts=2,locator=0:2;"
        "cmd=delete,string1=1,ts=2,locator=0:2;"
        "cmd=add,string1=1,string2=abc,string3=ijk,string4=pst,int5=56,float6=130.6,double7=1.5 2.7,ts=2,locator=0:2;"
        "cmd=update_field,string1=1,string2=bcde,string4=pstw,int5=0,ts=2,locator=0:2;"
        "cmd=update_field,string1=1,string2=efg,string3=xyz,float6=89,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/true).IsOK());
    ASSERT_TRUE(
        helper.Query("kv", "string1", "1", "string2=efg,string3=xyz,string4=pstw,int5=0,float6=89,double7=1.5 2.7"));

    // key=1 add -> update, key=2 add -> update, in same batch, test mem pool
    docs =
        "cmd=add,string1=1,string2=345,string3=678,string4=wsd,int5=34,float6=4.5,double7=1.4 2.5,ts=2,locator=0:2;"
        "cmd=update_field,string1=1,string2=bcde,string4=rst,int5=0,ts=2,locator=0:2;"
        "cmd=add,string1=2,string2=abc,string3=ijk,string4=pst,int5=56,float6=130.6,double7=1.5 2.9,ts=2,locator=0:2;"
        "cmd=update_field,string1=2,string2=efg,string3=xyz,float6=89,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/true).IsOK());
    ASSERT_TRUE(
        helper.Query("kv", "string1", "1", "string2=bcde,string3=678,string4=rst,int5=0,float6=4.5,double7=1.4 2.5"));
    ASSERT_TRUE(
        helper.Query("kv", "string1", "2", "string2=efg,string3=xyz,string4=pst,int5=56,float6=89,double7=1.5 2.9"));
}

TEST_P(KVTabletInteTest, TestUpdateWithException)
{
    auto tabletFactoryCreator = framework::TabletFactoryCreator::GetInstance();
    auto originalKVTabletFactory = tabletFactoryCreator->_factorysMap["kv"];
    autil::ScopeGuard factoryGuard([&]() {
        delete tabletFactoryCreator->_factorysMap["kv"];
        tabletFactoryCreator->_factorysMap["kv"] = originalKVTabletFactory;
    });
    auto fakeKVTabletFactory = new FakeKVTabletFactory();
    tabletFactoryCreator->_factorysMap["kv"] = fakeKVTabletFactory;

    string docs;
    std::string field = "string1:string;string2:string;string3:string;string4:string;int5:int32;float6:float";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2;string3;string4;int5;float6");
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());

    // add key=1 succeeded
    docs = "cmd=add,string1=1,string2=123,string3=234,string4=xyz,int5=56,float6=78.3,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=123,string3=234,string4=xyz,int5=56,float6=78.3"));

    // add key=1 failed, filesystem return error
    docs = "cmd=add,string1=1,string2=345,string3=678,string4=wsd,int5=34,float6=4.5,ts=2,locator=0:2;";
    fslib::fs::FileSystem::_useMock = true;
    ExceptionTrigger::InitTrigger(/*normalIOCount=*/0, /*probabilityMode=*/true, /*triggerOnceMode=*/false);
    ASSERT_FALSE(helper.BuildSegment(docs).IsOK());
    fslib::fs::FileSystem::_useMock = false;
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=123,string3=234,string4=xyz,int5=56,float6=78.3"));

    // add key=1 succeeded
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=345,string3=678,string4=wsd,int5=34,float6=4.5"));

    // update key=1 failed, because of query failed, return io error
    docs = "cmd=update_field,string1=1,string2=789,string4=pst,int5=67,ts=2,locator=0:2;";
    FakeKVReader::setFail(true);
    auto st = helper.Build(docs);
    ASSERT_TRUE(st.IsIOError()) << st.ToString().c_str();
    FakeKVReader::setFail(false);
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=345,string3=678,string4=wsd,int5=34,float6=4.5"));

    // update key=1 succeeded
    ASSERT_TRUE(helper.Build(docs).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=789,string3=678,string4=pst,int5=67,float6=4.5"));
}

TEST_P(KVTabletInteTest, TestUpdate2AddFixedLenKV)
{
    string docs;
    std::string field = "string1:string;int2:int32";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "int2");
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());

    // add key=1
    docs = "cmd=add,string1=1,int2=35,ts=1,locator=0:1;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/false).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "int2=35"));

    // add and update key=2
    docs = "cmd=add,string1=2,int2=12,ts=2,locator=0:2;"
           "cmd=update_field,string1=2,int2=34,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/false).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "int2=34"));

    // update key=2 again
    docs = "cmd=update_field,string1=2,int2=56,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "int2=56"));

    // update key=2 again
    docs = "cmd=update_field,string1=2,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "int2=56"));

    // update key=3
    docs = "cmd=update_field,string1=3,int2=99,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "3", ""));
}

TEST_P(KVTabletInteTest, TestUpdate2AddFixedLenKVInSameBatch)
{
    string docs;
    std::string field = "string1:string;int2:int64";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "int2");
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());

    // add -> update key=1 in same batch
    docs = "cmd=add,string1=1,int2=12,ts=2,locator=0:2;"
           "cmd=update_field,string1=1,int2=34,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/true).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "int2=34"));

    // add -> add -> update key=1 in same batch
    docs = "cmd=add,string1=1,int2=56,ts=2,locator=0:2;"
           "cmd=add,string1=1,int2=78,ts=2,locator=0:2;"
           "cmd=update_field,string1=1,ts=2,locator=0:2;";
    ASSERT_TRUE(helper.Build(docs, /*oneBatch=*/true).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "int2=78"));
}

TEST_P(KVTabletInteTest, TestSortDump)
{
    std::string field = "key:int32;int2:int32;float3:float;string4:string";
    _tabletSchema = table::KVTabletSchemaMaker::Make(field, "key", "int2;float3;string4");
    std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(_tabletSchema->GetIndexConfig("kv", "key"))
        ->SetTTL(10);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    std::string jsonStr = R"(
        [
            {
                "sort_field" : "int2",
                "sort_pattern" : "desc"
            },
            {
                "sort_field" : "float3",
                "sort_pattern" : "desc"
            }
       ])";
    autil::legacy::Any sortDescription;
    FromJsonString(sortDescription, jsonStr);
    _tabletSchema->TEST_GetImpl()->SetRuntimeSetting("sort_descriptions", sortDescription, /*overwrite*/ false);
    jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "level_num" : 1
        }
    }
    } )";
    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions)).IsOK());
    auto docs = "cmd=add,key=1,int2=12,float3=3.4,string4=ab1,ts=1000000,locator=0:1;"
                "cmd=add,key=3,int2=32,float3=5.4,string4=abc,ts=2000000,locator=0:2;"
                "cmd=add,key=2,int2=34,float3=4.3,string4=ab2,ts=3000000,locator=0:3;"
                "cmd=add,key=4,int2=67,float3=4.7,string4=abz,ts=4000000,locator=0:4;"
                "cmd=add,key=3,int2=56,float3=4.1,string4=ab3,ts=5000000,locator=0:5;"
                "cmd=add,key=5,int2=34,float3=4.5,string4=ab4,ts=6000000,locator=0:6;"
                "cmd=delete,key=4,ts=7000000,locator=0:7;"
                "cmd=add,key=4,int2=34,float3=4.5,string4=ab4,ts=8000000,locator=0:8;"
                "cmd=delete,key=5,ts=9000000,locator=0:9;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());

    // prepare expected records
    vector<Record> expectedSortedRecords;
    vector<keytype_t> keyVec = {5, 3, 4, 2, 1};
    vector<bool> deleteVec = {true, false, false, false, false};
    vector<uint32_t> timStampVec = {9, 5, 8, 3, 1};
    for (size_t i = 0; i < keyVec.size(); i++) {
        Record record;
        record.key = keyVec[i];
        record.deleted = deleteVec[i];
        record.timestamp = timStampVec[i];
        expectedSortedRecords.push_back(record);
    }

    // check result
    auto tablet = helper.GetITablet();
    auto reader = tablet->GetTabletReader();
    auto kvTabletReader = std::dynamic_pointer_cast<table::KVTabletSessionReader>(reader);
    auto kvReader = kvTabletReader->GetIndexReader<KVReaderImpl>("kv", "key");
    auto shardReaders = kvReader->TEST_GetDiskShardReaders();
    ASSERT_EQ(size_t(1), shardReaders.size());
    ASSERT_EQ(size_t(1), shardReaders[0].size());
    auto iterator = shardReaders[0][0].first->CreateIterator();

    // check sort record order
    autil::mem_pool::Pool pool;
    size_t pos = 0;
    uint32_t docCount = 0;
    while (iterator->HasNext()) {
        Record record;
        ASSERT_TRUE(iterator->Next(&pool, record).IsOK());
        auto expectedRecord = expectedSortedRecords[pos];
        ASSERT_EQ(expectedRecord.key, record.key);
        ASSERT_EQ(expectedRecord.deleted, record.deleted);
        ASSERT_EQ(expectedRecord.timestamp, record.timestamp);
        pos++;
        docCount++;
    }
    ASSERT_EQ(docCount, 5);

    // check record value
    ASSERT_TRUE(helper.Query("kv", "key", "1", "int2=12,float3=3.4,string4=ab1"));
    ASSERT_TRUE(helper.Query("kv", "key", "2", "int2=34,float3=4.3,string4=ab2"));
    ASSERT_TRUE(helper.Query("kv", "key", "3", "int2=56,float3=4.1,string4=ab3"));
    ASSERT_TRUE(helper.Query("kv", "key", "4", "int2=34,float3=4.5,string4=ab4"));
}

TEST_P(KVTabletInteTest, TestFollowReopen)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH() + "/local", GET_TEMP_DATA_PATH() + "/remote");
    KVTableTestHelper helper;
    auto tabletOptions = CreateOnlineMultiShardOptions();
    tabletOptions->SetIsLeader(false);
    tabletOptions->SetFlushLocal(true);
    tabletOptions->SetFlushRemote(false);
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions)).IsOK());
    ASSERT_EQ(INVALID_VERSIONID, helper.GetCurrentVersion().GetVersionId());
    std::string docs = "cmd=add,string1=1,string2=abc,ts=1,locator=0:2;"
                       "cmd=add,string1=4,string2=Aijk,ts=2,locator=0:3;"
                       "cmd=add,string1=2,string2=Aijk,ts=3,locator=0:4;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_EQ(framework::Version::PRIVATE_VERSION_ID_MASK | 0, helper.GetCurrentVersion().GetVersionId());
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=abc"));
    ASSERT_TRUE(helper.Query("kv", "string1", "2", "string2=Aijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "4", "string2=Aijk"));
    docs = "cmd=add,string1=7,string2=abc,ts=4,locator=0:5;"
           "cmd=add,string1=8,string2=Aijk,ts=5,locator=0:6;"
           "cmd=add,string1=9,string2=Aijk,ts=6,locator=0:7;";
    ASSERT_TRUE(helper.Build(docs).IsOK());
    ASSERT_TRUE(helper.Query("kv", "string1", "7", "string2=abc"));
    ASSERT_TRUE(helper.Query("kv", "string1", "8", "string2=Aijk"));
    ASSERT_TRUE(helper.Query("kv", "string1", "9", "string2=Aijk"));
    auto tablet = std::dynamic_pointer_cast<framework::Tablet>(helper.GetITablet());
    ASSERT_TRUE(tablet);
    auto tabletData = tablet->GetTabletData();
    ASSERT_EQ(1, tabletData->GetIncSegmentCount());
    ASSERT_EQ(2, tabletData->GetSegmentCount());
    auto segment = tabletData->GetSegment(framework::Segment::PRIVATE_SEGMENT_ID_MASK | 0);
    ASSERT_TRUE(segment);
    ASSERT_TRUE(std::dynamic_pointer_cast<plain::MultiShardDiskSegment>(segment));
}

TEST_P(KVTabletInteTest, TestRecoverLocalIndexSimple)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH() + "/local", GET_TEMP_DATA_PATH() + "/remote");

    auto followerTabletOptions = CreateOnlineMultiShardOptions();
    followerTabletOptions->SetIsLeader(false);
    followerTabletOptions->SetFlushLocal(true);
    followerTabletOptions->SetFlushRemote(false);

    // open follower
    auto followerHelper1 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/false);
    ASSERT_TRUE(followerHelper1->Open(indexRoot, _tabletSchema, followerTabletOptions).IsOK());
    followerHelper1->GetTaskScheduler()->CleanTasks();
    ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk1,string2=aaa,ts=1,locator=0:1;").IsOK());
    ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk2,string2=bbb,ts=2,locator=0:2;").IsOK());
    ASSERT_TRUE(followerHelper1->BuildSegment("cmd=update_field,string1=pk2,string2=ccc,ts=3,locator=0:3;").IsOK());

    // follower failover, load_remain_flush_realtime_index == false, should not recover rt index
    followerTabletOptions->TEST_GetOnlineConfig().TEST_SetLoadRemainFlushRealtimeIndex(false);
    auto followerHelper2 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/false);
    ASSERT_TRUE(followerHelper2->Open(indexRoot, _tabletSchema, followerTabletOptions).IsOK());
    followerHelper2->GetTaskScheduler()->CleanTasks();
    ASSERT_EQ(followerHelper2->GetCurrentVersion().GetVersionId(), INVALID_VERSIONID);
    ASSERT_EQ("0:-1:[]", followerHelper2->GetITablet()->GetTabletInfos()->GetLatestLocator().DebugString());
    ASSERT_FALSE(followerHelper2->Query("kv", "string1", "pk1", "string2=aaa"));
    ASSERT_FALSE(followerHelper2->Query("kv", "string1", "pk2", "string2=ccc"));

    // follower failover, load_remain_flush_realtime_index == true, should recover rt index
    followerTabletOptions->TEST_GetOnlineConfig().TEST_SetLoadRemainFlushRealtimeIndex(true);
    auto followerHelper3 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/false);
    ASSERT_TRUE(followerHelper3->Open(indexRoot, _tabletSchema, followerTabletOptions).IsOK());
    followerHelper3->GetTaskScheduler()->CleanTasks();
    ASSERT_EQ(followerHelper3->GetCurrentVersion().GetVersionId(), GetPrivateVersionId(2));
    ASSERT_EQ("0:3:[{0,65535,3,0}]", followerHelper3->GetITablet()->GetTabletInfos()->GetLatestLocator().DebugString());
    ASSERT_TRUE(followerHelper3->Query("kv", "string1", "pk1", "string2=aaa"));
    ASSERT_TRUE(followerHelper3->Query("kv", "string1", "pk2", "string2=ccc"));
    ASSERT_TRUE(followerHelper3
                    ->Build("cmd=add,string1=pk3,string2=ddd,ts=4,locator=0:4;"
                            "cmd=delete,string1=pk1,ts=5,locator=0:5;"
                            "cmd=update_field,string1=pk2,string2=eee,ts=6,locator=0:6;")
                    .IsOK());
    ASSERT_FALSE(followerHelper3->Query("kv", "string1", "pk1", "string2=aaa"));
    ASSERT_TRUE(followerHelper3->Query("kv", "string1", "pk2", "string2=eee"));
    ASSERT_TRUE(followerHelper3->Query("kv", "string1", "pk3", "string2=ddd"));
}

TEST_P(KVTabletInteTest, TestRecoverLocalIndexWithPublicVersion)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH() + "/local", GET_TEMP_DATA_PATH() + "/remote");

    auto leaderTabletOptions = CreateOnlineMultiShardOptions();
    leaderTabletOptions->SetIsLeader(true);
    leaderTabletOptions->SetFlushLocal(false);
    leaderTabletOptions->SetFlushRemote(true);
    leaderTabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(10);

    // open leader
    auto leaderHelper = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
    ASSERT_TRUE(leaderHelper->Open(indexRoot, _tabletSchema, leaderTabletOptions).IsOK());
    leaderHelper->GetTaskScheduler()->CleanTasks();
    ASSERT_TRUE(leaderHelper->BuildSegment("cmd=add,string1=pk1,string2=aaa,ts=1,locator=0:1;").IsOK());
    ASSERT_TRUE(leaderHelper->BuildSegment("cmd=add,string1=pk2,string2=bbb,ts=2,locator=0:2;").IsOK());
    ASSERT_TRUE(leaderHelper->Merge().IsOK());
    auto publicVersion1Id = leaderHelper->GetCurrentVersion().GetVersionId(); // base version
    ASSERT_TRUE(leaderHelper->BuildSegment("cmd=add,string1=pk3,string2=ccc,ts=3,locator=0:3;").IsOK());
    ASSERT_TRUE(leaderHelper->BuildSegment("cmd=add,string1=pk4,string2=ddd,ts=4,locator=0:4;").IsOK());
    ASSERT_TRUE(leaderHelper->Merge().IsOK());
    auto publicVersion2Id = leaderHelper->GetCurrentVersion().GetVersionId();

    {
        // follower build faster than leader
        auto followerTabletOptions = CreateOnlineMultiShardOptions();
        followerTabletOptions->SetIsLeader(false);
        followerTabletOptions->SetFlushLocal(true);
        followerTabletOptions->SetFlushRemote(false);

        auto followerHelper1 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
        ASSERT_TRUE(followerHelper1->Open(indexRoot, _tabletSchema, followerTabletOptions).IsOK());
        followerHelper1->GetTaskScheduler()->CleanTasks();
        ASSERT_TRUE(followerHelper1->Reopen(publicVersion1Id).IsOK());
        ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk3,string2=ccc,ts=3,locator=0:3;").IsOK());
        ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk4,string2=ddd,ts=4,locator=0:4;").IsOK());
        ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk5,string2=eee,ts=5,locator=0:5;").IsOK());

        // follower failover, should recover rt index which faster than publicVersion2
        followerTabletOptions->TEST_GetOnlineConfig().TEST_SetLoadRemainFlushRealtimeIndex(true);
        auto followerHelper2 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
        ASSERT_TRUE(followerHelper2->Open(indexRoot, _tabletSchema, followerTabletOptions, publicVersion2Id).IsOK());
        followerHelper2->GetTaskScheduler()->CleanTasks();
        ASSERT_EQ(followerHelper2->GetCurrentVersion().GetVersionId(), GetPublicVersionId(5));
        ASSERT_EQ("0:5:[{0,65535,5,0}]",
                  followerHelper2->GetITablet()->GetTabletInfos()->GetLatestLocator().DebugString());
        ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk1", "string2=aaa"));
        ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk2", "string2=bbb"));
        ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk3", "string2=ccc"));
        ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk4", "string2=ddd"));
        ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk5", "string2=eee"));
    }
    {
        // follower build slower than leader
        auto followerTabletOptions = CreateOnlineMultiShardOptions();
        followerTabletOptions->SetIsLeader(false);
        followerTabletOptions->SetFlushLocal(true);
        followerTabletOptions->SetFlushRemote(false);

        auto followerHelper1 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
        ASSERT_TRUE(followerHelper1->Open(indexRoot, _tabletSchema, followerTabletOptions).IsOK());
        followerHelper1->GetTaskScheduler()->CleanTasks();
        ASSERT_TRUE(followerHelper1->Reopen(publicVersion1Id).IsOK());
        ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk0,string2=ccc,ts=3,locator=0:3;").IsOK());
        ASSERT_TRUE(followerHelper1->Query("kv", "string1", "pk0", "string2=ccc"));

        // follower failover, recover rt index though slower than publicVersion2
        followerTabletOptions->TEST_GetOnlineConfig().TEST_SetLoadRemainFlushRealtimeIndex(true);
        auto followerHelper2 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
        ASSERT_TRUE(followerHelper2->Open(indexRoot, _tabletSchema, followerTabletOptions, publicVersion2Id).IsOK());
        followerHelper2->GetTaskScheduler()->CleanTasks();
        ASSERT_EQ(followerHelper2->GetCurrentVersion().GetVersionId(), GetPublicVersionId(5));
        ASSERT_EQ("0:4:[{0,65535,4,0}]",
                  followerHelper2->GetITablet()->GetTabletInfos()->GetLatestLocator().DebugString());
        ASSERT_FALSE(followerHelper2->Query("kv", "string1", "pk0", "string2=ccc"));
        ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk1", "string2=aaa"));
        ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk2", "string2=bbb"));
        ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk3", "string2=ccc"));
        ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk4", "string2=ddd"));
    }
}

TEST_P(KVTabletInteTest, TestRecoverLocalIndexPublicVersionIncompatible)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH() + "/local", GET_TEMP_DATA_PATH() + "/remote");

    auto leaderTabletOptions = CreateOnlineMultiShardOptions();
    leaderTabletOptions->SetIsLeader(true);
    leaderTabletOptions->SetFlushLocal(false);
    leaderTabletOptions->SetFlushRemote(true);
    leaderTabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(10);

    // open leader
    auto leaderHelper = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
    ASSERT_TRUE(leaderHelper->Open(indexRoot, _tabletSchema, leaderTabletOptions).IsOK());
    leaderHelper->GetTaskScheduler()->CleanTasks();
    ASSERT_TRUE(leaderHelper->BuildSegment("cmd=add,string1=pk1,string2=aaa,ts=1,locator=0:1;").IsOK());
    ASSERT_TRUE(leaderHelper->BuildSegment("cmd=add,string1=pk2,string2=bbb,ts=2,locator=0:2;").IsOK());
    ASSERT_TRUE(leaderHelper->Merge().IsOK());
    auto publicVersionId = leaderHelper->GetCurrentVersion().GetVersionId();

    auto leaderHelper2 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
    ASSERT_TRUE(leaderHelper2->Open(indexRoot, _tabletSchema, leaderTabletOptions).IsOK());
    leaderHelper2->GetTaskScheduler()->CleanTasks();
    ASSERT_TRUE(leaderHelper2->BuildSegment("cmd=add,string1=pk1,string2=aaa,ts=1,locator=0:1;").IsOK());
    ASSERT_TRUE(leaderHelper2->BuildSegment("cmd=add,string1=pk2,string2=bbb,ts=2,locator=0:2;").IsOK());
    ASSERT_TRUE(leaderHelper2->Merge().IsOK());
    auto publicVersionId2 = leaderHelper2->GetCurrentVersion().GetVersionId();

    // follower build faster than leader
    auto followerTabletOptions = CreateOnlineMultiShardOptions();
    followerTabletOptions->SetIsLeader(false);
    followerTabletOptions->SetFlushLocal(true);
    followerTabletOptions->SetFlushRemote(false);

    auto followerHelper1 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
    ASSERT_TRUE(followerHelper1->Open(indexRoot, _tabletSchema, followerTabletOptions, publicVersionId).IsOK());
    followerHelper1->GetTaskScheduler()->CleanTasks();
    ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk1,string2=aaa,ts=1,locator=0:3;").IsOK());
    ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk2,string2=bbb,ts=2,locator=0:4;").IsOK());
    ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk3,string2=ccc,ts=3,locator=0:5;").IsOK());
    auto [s1, isExist1] = IsFenceExist(indexRoot.GetLocalRoot(), followerHelper1.get());
    ASSERT_TRUE(s1.IsOK());
    ASSERT_TRUE(isExist1);
    // follower failover, try recover rt index but incompatible, quit recover
    followerTabletOptions->TEST_GetOnlineConfig().TEST_SetLoadRemainFlushRealtimeIndex(true);
    auto followerHelper2 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
    ASSERT_FALSE(followerHelper2->Open(indexRoot, _tabletSchema, followerTabletOptions, publicVersionId2).IsOK());
    auto [s2, isExist2] = IsFenceExist(indexRoot.GetLocalRoot(), followerHelper2.get());
    ASSERT_TRUE(s2.IsOK());
    ASSERT_FALSE(isExist2);
    ASSERT_TRUE(followerHelper2->Open(indexRoot, _tabletSchema, followerTabletOptions, publicVersionId2).IsOK());
    followerHelper2->GetTaskScheduler()->CleanTasks();
    ASSERT_EQ(followerHelper2->GetCurrentVersion().GetVersionId(), GetPublicVersionId(5));
    ASSERT_EQ("0:2:[{0,65535,2,0}]", followerHelper2->GetITablet()->GetTabletInfos()->GetLatestLocator().DebugString());
    ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk1", "string2=aaa"));
    ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk2", "string2=bbb"));
    ASSERT_FALSE(followerHelper2->Query("kv", "string1", "pk3", "string2=ccc"));
}

TEST_P(KVTabletInteTest, TestRecoverLocalIndexFail)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH() + "/local", GET_TEMP_DATA_PATH() + "/remote");

    auto leaderTabletOptions = CreateOnlineMultiShardOptions();
    leaderTabletOptions->SetIsLeader(true);
    leaderTabletOptions->SetFlushLocal(false);
    leaderTabletOptions->SetFlushRemote(true);
    leaderTabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(10);

    // open leader
    auto leaderHelper = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
    ASSERT_TRUE(leaderHelper->Open(indexRoot, _tabletSchema, leaderTabletOptions).IsOK());
    leaderHelper->GetTaskScheduler()->CleanTasks();
    ASSERT_TRUE(leaderHelper->BuildSegment("cmd=add,string1=pk1,string2=aaa,ts=1,locator=0:1;").IsOK());
    ASSERT_TRUE(leaderHelper->BuildSegment("cmd=add,string1=pk2,string2=bbb,ts=2,locator=0:2;").IsOK());
    ASSERT_TRUE(leaderHelper->Merge().IsOK());
    auto publicVersionId = leaderHelper->GetCurrentVersion().GetVersionId();

    // follower build faster than leader
    auto followerTabletOptions = CreateOnlineMultiShardOptions();
    followerTabletOptions->SetIsLeader(false);
    followerTabletOptions->SetFlushLocal(true);
    followerTabletOptions->SetFlushRemote(false);

    auto followerHelper1 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
    ASSERT_TRUE(followerHelper1->Open(indexRoot, _tabletSchema, followerTabletOptions).IsOK());
    followerHelper1->GetTaskScheduler()->CleanTasks();
    ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk1,string2=aaa,ts=1,locator=0:1;").IsOK());
    ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk2,string2=bbb,ts=2,locator=0:2;").IsOK());
    ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk3,string2=ccc,ts=3,locator=0:3;").IsOK());
    ASSERT_TRUE(followerHelper1->BuildSegment("cmd=add,string1=pk4,string2=ddd,ts=4,locator=0:4;").IsOK());
    // remove segment (3 | PRIVATE_SEGMENT_ID_MASK)
    auto directory = indexlib::file_system::IDirectory::GetPhysicalDirectory(GET_TEMP_DATA_PATH() + "/local");
    indexlib::file_system::RemoveOption removeOption;
    std::string segmentPath = framework::Fence::GetFenceName(RT_INDEX_PARTITION_DIR_NAME) + "/segment_" +
                              std::to_string(3 | framework::Segment::PRIVATE_SEGMENT_ID_MASK) + "_level_0";
    ASSERT_TRUE(directory->RemoveDirectory(segmentPath, removeOption).Status().IsOK());

    // follower failover, try recover rt index but fail, quit recover
    followerTabletOptions->TEST_GetOnlineConfig().TEST_SetLoadRemainFlushRealtimeIndex(true);
    auto followerHelper2 = std::make_shared<KVTableTestHelper>(/*autoCleanIndex=*/true, /*needDeploy=*/true);
    auto [s1, isExist1] = IsFenceExist(indexRoot.GetLocalRoot(), followerHelper1.get());
    ASSERT_TRUE(s1.IsOK());
    ASSERT_TRUE(isExist1);
    ASSERT_FALSE(followerHelper2->Open(indexRoot, _tabletSchema, followerTabletOptions, publicVersionId).IsOK());
    auto [s2, isExist2] = IsFenceExist(indexRoot.GetLocalRoot(), followerHelper2.get());
    ASSERT_TRUE(s2.IsOK());
    ASSERT_FALSE(isExist2);
    ASSERT_TRUE(followerHelper2->Open(indexRoot, _tabletSchema, followerTabletOptions, publicVersionId).IsOK());
    followerHelper2->GetTaskScheduler()->CleanTasks();
    ASSERT_EQ(followerHelper2->GetCurrentVersion().GetVersionId(), GetPublicVersionId(2));
    ASSERT_EQ("0:2:[{0,65535,2,0}]", followerHelper2->GetITablet()->GetTabletInfos()->GetLatestLocator().DebugString());
    ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk1", "string2=aaa"));
    ASSERT_TRUE(followerHelper2->Query("kv", "string1", "pk2", "string2=bbb"));
    ASSERT_FALSE(followerHelper2->Query("kv", "string1", "pk3", "string2=ccc"));
    ASSERT_FALSE(followerHelper2->Query("kv", "string1", "pk4", "string2=ddd"));
}

TEST_P(KVTabletInteTest, TestBuildingSegmentMemReclaim)
{
    RETURN_IF_ENABLE_TSAN();
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 1,
            "level_num" : 2,
            "enable_kv_memory_reclaim": true
        }
    }
    } )";
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper helper;
    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(1 * 1024 * 1024);
    tabletOptions->TEST_GetOnlineConfig().TEST_SetMaxRealtimeMemoryUse(2 * 1024 * 1024);
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, tabletOptions).IsOK());
    auto tablet = helper.GetTablet();
    for (size_t i = 0; i < 100; ++i) {
        string docs = string("cmd=add,string1=1,string2=") + string(1024 * 32, '2') + ";";
        ASSERT_TRUE(helper.Build(docs).IsOK());
        tablet->MemoryReclaim();
    }
}

TEST_P(KVTabletInteTest, TestInconsistentTableOptions)
{
    framework::IndexRoot indexRoot = GetIndexRoot();

    std::string offlineTableOptionsStr = R"( {
        "offline_index_config": {
            "build_config": {
                "sharding_column_num" : 2,
                "level_num" : 2
            }
        }
    } )";
    auto offlineTabletOptions = CreateOfflineMultiShardOptions(offlineTableOptionsStr);

    KVTableTestHelper helper0;
    ASSERT_TRUE(helper0.Open(indexRoot, _tabletSchema, offlineTabletOptions, INVALID_VERSIONID).IsOK());

    ASSERT_TRUE(helper0.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(helper0.Merge().IsOK());
    auto [status, versionId] = helper0.Commit();
    ASSERT_NE(-1, versionId);
    ASSERT_TRUE(status.IsOK());
    auto version = helper0.GetCurrentVersion();
    {
        auto levelInfo = version.GetSegmentDescriptions()->GetLevelInfo();
        ASSERT_EQ(2, levelInfo->GetLevelCount());
        ASSERT_EQ(2, levelInfo->GetShardCount());
        ASSERT_EQ(vector<segmentid_t>({}), levelInfo->levelMetas[0].segments);
        ASSERT_EQ(vector<segmentid_t>({0, 1}), levelInfo->levelMetas[1].segments);
    }
    helper0.Close();

    std::string onlineTableOptionsStr = R"( {
        "online_index_config": {
            "build_config": {
                "sharding_column_num" : 8,
                "level_num" : 3
            }
        }
    } )";
    auto onlineTabletOptions = CreateOnlineMultiShardOptions(onlineTableOptionsStr);

    onlineTabletOptions->SetIsOnline(true);
    onlineTabletOptions->SetFlushLocal(true);
    KVTableTestHelper helper1;
    ASSERT_TRUE(helper1.Open(indexRoot, _tabletSchema, onlineTabletOptions, versionId).IsOK());
    ASSERT_TRUE(helper1.BuildSegment(docString2()).IsOK());
    ASSERT_TRUE(helper1.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(helper1.Query("kv", "string1", "12", "string2=Acde"));
    ASSERT_TRUE(helper1.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(helper1.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(helper1.Query("kv", "string1", "15", ""));
    ASSERT_TRUE(helper1.Query("kv", "string1", "2", ""));
    auto [status1, versionId1] = helper1.Commit();
    ASSERT_TRUE(status1.IsOK());
    auto version1 = helper1.GetCurrentVersion();
    {
        auto levelInfo = version1.GetSegmentDescriptions()->GetLevelInfo();
        ASSERT_NE(nullptr, levelInfo);
        ASSERT_EQ(2, levelInfo->GetLevelCount());
        ASSERT_EQ(2, levelInfo->GetShardCount());
        ASSERT_EQ(vector<segmentid_t>({1073741824}), levelInfo->levelMetas[0].segments);
        ASSERT_EQ(vector<segmentid_t>({0, 1}), levelInfo->levelMetas[1].segments);
        ASSERT_EQ(vector<segmentid_t>({1073741824, 0}), levelInfo->GetSegmentIds(0));
        ASSERT_EQ(vector<segmentid_t>({1073741824, 1}), levelInfo->GetSegmentIds(1));
    }
}

TEST_P(KVTabletInteTest, TestCloseWithReaderHeld)
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "level_num" : 3
        }
    }
    } )";
    std::shared_ptr<config::TabletOptions> tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    // test init
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _tabletSchema, std::move(tabletOptions), INVALID_VERSIONID).IsOK());
    // test build
    ASSERT_TRUE(helper.BuildSegment(docString1()).IsOK());
    // check result
    ASSERT_TRUE(helper.Query("kv", "string1", "1", "string2=abc"));
    auto reader = helper.GetTabletReader();
    helper.Close();
    ASSERT_TRUE(KVTableTestHelper::DoQuery(reader, "kv", "string1", "1", "string2=abc"));
}

}} // namespace indexlibv2::table
