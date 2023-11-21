#include "indexlib/framework/TabletCommitter.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/framework/CommitOptions.h"
#include "indexlib/framework/IdGenerator.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/index_task/Constant.h"
#include "indexlib/framework/mock/MockMemSegment.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class TabletCommitterTest : public TESTBASE
{
public:
    TabletCommitterTest() = default;
    ~TabletCommitterTest() = default;

    void setUp() override {}
    void tearDown() override {}

private:
    bool AddOneSegment(std::shared_ptr<TabletData>& tabletData, std::shared_ptr<TabletCommitter> tabletCommitter,
                       segmentid_t segmentId, int64_t ts, std::shared_ptr<SegmentStatistics> segmentStatistics);
    Fence CreateFence(const std::string& rootDir, const std::string& fenceName,
                      const indexlib::file_system::FileSystemOptions& fsOptions);
    std::shared_ptr<TabletCommitter> CreateEmptyTabletCommiter(const std::string& tableName);

private:
    const std::string _tabletName = "test_committer";
};

Fence TabletCommitterTest::CreateFence(const std::string& rootDir, const std::string& fenceName,
                                       const indexlib::file_system::FileSystemOptions& fsOptions)
{
    std::string fencePath = indexlib::util::PathUtil::JoinPath(rootDir, fenceName);
    Fence fence(rootDir, fenceName,
                indexlib::file_system::FileSystemCreator::Create(/*name=*/"", fencePath).GetOrThrow());
    return fence;
}

bool TabletCommitterTest::AddOneSegment(std::shared_ptr<TabletData>& tabletData,
                                        std::shared_ptr<TabletCommitter> tabletCommitter, segmentid_t segmentId,
                                        int64_t ts, std::shared_ptr<SegmentStatistics> segmentStatistics)
{
    SegmentMeta segmentMeta(segmentId);
    auto segmentInfo = std::make_shared<SegmentInfo>();
    segmentInfo->timestamp = ts;
    if (segmentStatistics) {
        segmentInfo->AddDescription(SegmentStatistics::JSON_KEY, autil::legacy::ToJsonString(segmentStatistics));
    }
    segmentMeta.segmentInfo = segmentInfo;
    segmentMeta.schema = std::make_shared<config::TabletSchema>();
    auto memSegment = std::make_shared<MockMemSegment>(segmentMeta, Segment::SegmentStatus::ST_DUMPING);
    tabletData->TEST_PushSegment(memSegment);
    if (tabletCommitter != nullptr) {
        tabletCommitter->Push(segmentId);
        return tabletCommitter->NeedCommit();
    }
    return true;
}

std::shared_ptr<TabletCommitter> TabletCommitterTest::CreateEmptyTabletCommiter(const std::string& tableName)
{
    Version v;
    v.SetSchemaId(0);
    v.SetReadSchemaId(0);
    auto tabletData = std::make_shared<TabletData>(tableName);
    auto resourceMap = std::make_shared<ResourceMap>();
    auto status = tabletData->Init(v, {}, resourceMap);
    assert(status.IsOK());
    auto tabletCommitter = std::make_shared<TabletCommitter>(tableName);
    tabletCommitter->Init(nullptr, tabletData);
    return tabletCommitter;
}

TEST_F(TabletCommitterTest, testNeedCommit)
{
    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    ASSERT_FALSE(tabletCommitter->NeedCommit());

    const std::string fenceName("test_committer");
    Fence fence("", fenceName, nullptr);
    std::vector<std::string> filterDirs;
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    {
        auto [status, version] = tabletCommitter->GenerateVersionWithoutId(tabletData, fence, nullptr, filterDirs);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(version.GetLastSegmentId(), INVALID_SEGMENTID);
    }

    {
        segmentid_t segmentId = 0;
        int64_t ts = 1;
        ASSERT_TRUE(AddOneSegment(tabletData, tabletCommitter, segmentId, ts, nullptr));
        auto [status, version] = tabletCommitter->GenerateVersionWithoutId(tabletData, fence, nullptr, filterDirs);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(version.GetLastSegmentId(), 0);
        ASSERT_EQ(version.GetTimestamp(), 1);
        ASSERT_EQ(version.GetFenceName(), fenceName);
    }
    {
        // segmentId = 2 is not in commitQ, so also not exist in version
        segmentid_t segmentId = 2;
        int64_t ts = 2;
        ASSERT_TRUE(AddOneSegment(tabletData, /*committer=*/nullptr, segmentId, ts, nullptr));
        tabletCommitter->Push(0);
        ASSERT_TRUE(tabletCommitter->NeedCommit());

        auto [status, version] = tabletCommitter->GenerateVersionWithoutId(tabletData, fence, nullptr, filterDirs);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(version.GetLastSegmentId(), 0);
        ASSERT_EQ(version.GetTimestamp(), 1);
        ASSERT_EQ(version.GetFenceName(), fenceName);
    }
    {
        // total 10 segments in commitQ, but 11 segments in tabletData, so there are 10 segments in version
        size_t segmentCntInCommitQ = 10;
        for (size_t i = 0; i < segmentCntInCommitQ; ++i) {
            segmentid_t segmentId = i;
            ASSERT_TRUE(AddOneSegment(tabletData, tabletCommitter, segmentId, (int64_t)i, nullptr));
        }
        ASSERT_TRUE(AddOneSegment(tabletData, /*committer=*/nullptr, (segmentid_t)(segmentCntInCommitQ),
                                  (int64_t)(segmentCntInCommitQ), nullptr));

        auto [status, version] = tabletCommitter->GenerateVersionWithoutId(tabletData, fence, nullptr, filterDirs);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(version.GetLastSegmentId(), segmentCntInCommitQ - 1);
        ASSERT_EQ(version.GetTimestamp(), segmentCntInCommitQ - 1);
    }
    ASSERT_FALSE(tabletCommitter->NeedCommit());
    tabletCommitter->SetDumpError(Status::IOError());
    ASSERT_TRUE(tabletCommitter->GetDumpError().IsIOError());
    ASSERT_TRUE(tabletCommitter->NeedCommit());
}

TEST_F(TabletCommitterTest, testDumpError)
{
    const std::string rootDir = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;
    auto fence = CreateFence(rootDir, "fence", fsOptions);

    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    ASSERT_FALSE(tabletCommitter->NeedCommit());

    tabletCommitter->SetDumpError(Status::IOError());
    ASSERT_TRUE(tabletCommitter->GetDumpError().IsIOError());
    ASSERT_TRUE(tabletCommitter->NeedCommit());

    auto tabletData = std::make_shared<TabletData>(_tabletName);
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    auto [status, version] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/3, &idGenerator, CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(status.IsIOError());
    ASSERT_FALSE(version.IsValid());
}

TEST_F(TabletCommitterTest, testCommit)
{
    const std::string rootDir = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;

    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    for (size_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(AddOneSegment(tabletData, tabletCommitter, (segmentid_t)(i), (int64_t)i, nullptr));
    }
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    size_t retryCnt = 3;
    for (size_t i = 0; i < 2 * retryCnt; ++i) {
        auto fence = CreateFence(rootDir, "fence", fsOptions);
        auto [status, version] = tabletCommitter->Commit(tabletData, fence, /*retry=*/retryCnt, &idGenerator,
                                                         CommitOptions().SetNeedPublish(true));
        ASSERT_TRUE(status.IsOK());
        versionid_t expectedVersionId = Version::PUBLIC_VERSION_ID_MASK | (i << 1);
        ASSERT_EQ(expectedVersionId, version.GetVersionId());

        auto [status1, version1] = tabletCommitter->Commit(tabletData, fence, /*retry=*/retryCnt, &idGenerator,
                                                           CommitOptions().SetNeedPublish(true));
        ASSERT_TRUE(status1.IsOK());
        ++expectedVersionId;
        ASSERT_EQ(expectedVersionId, version1.GetVersionId());
        ASSERT_EQ(version._segments, version1._segments);
        ASSERT_TRUE(version.GetCommitTime() > 0);
        ASSERT_TRUE(version1.GetCommitTime() > 0);
        ASSERT_TRUE(version1.GetCommitTime() > version.GetCommitTime());
    }
}

TEST_F(TabletCommitterTest, testSegmentWithDescription)
{
    const std::string rootDir = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;

    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    for (size_t i = 0; i < 10; ++i) {
        std::map<std::string, std::string> strMap;
        std::map<std::string, std::pair<int64_t, int64_t>> intMap(
            {{"demo_key", std::make_pair<int64_t, int64_t>(i, i)}});
        auto segmentStatics = std::make_shared<SegmentStatistics>(i, intMap, strMap);
        ASSERT_TRUE(AddOneSegment(tabletData, tabletCommitter, (segmentid_t)(i), (int64_t)i, segmentStatics));
    }
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    size_t retryCnt = 3;
    auto fence = CreateFence(rootDir, "fence", fsOptions);
    auto commitOptions = CommitOptions().SetNeedPublish(true);
    commitOptions.AddVersionDescription("generation", "123456789");
    commitOptions.AddVersionDescription("udf_key", "udf_value");
    auto [status, version] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/retryCnt, &idGenerator, commitOptions);
    ASSERT_TRUE(status.IsOK());
    versionid_t expectedVersionId = Version::PUBLIC_VERSION_ID_MASK;
    ASSERT_EQ(expectedVersionId, version.GetVersionId());

    std::string value;
    ASSERT_TRUE(version.GetDescription("generation", value));
    ASSERT_EQ(std::string("123456789"), value);
    ASSERT_TRUE(version.GetDescription("udf_key", value));
    ASSERT_EQ(std::string("udf_value"), value);

    auto segmentDescriptions = version.GetSegmentDescriptions();
    ASSERT_TRUE(segmentDescriptions);
    auto segmentStatistics = segmentDescriptions->GetSegmentStatisticsVector();
    ASSERT_EQ(10, segmentStatistics.size());
    for (size_t i = 0; i < 10; ++i) {
        std::map<std::string, std::string> strMap;
        std::map<std::string, std::pair<int64_t, int64_t>> intMap(
            {{"demo_key", std::make_pair<int64_t, int64_t>(i, i)}});
        auto expectedSegmentStatics = std::make_shared<SegmentStatistics>(i, intMap, strMap);
        ASSERT_EQ(*expectedSegmentStatics, segmentStatistics[i]);
    }
}

TEST_F(TabletCommitterTest, testCommitWithCertainVersionId)
{
    const std::string rootDir = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;
    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    for (size_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(AddOneSegment(tabletData, tabletCommitter, (segmentid_t)(i), (int64_t)i, nullptr));
    }
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    int32_t retryCnt = 3;
    auto fence = CreateFence(rootDir, "fence", fsOptions);
    auto [status, version] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/retryCnt, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870915));
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(536870915, version.GetVersionId());

    auto [status1, version1] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/retryCnt, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870918));
    ASSERT_TRUE(status1.IsOK());
    ASSERT_EQ(536870918, version1.GetVersionId());
    ASSERT_EQ(version._segments, version1._segments);
    ASSERT_TRUE(version.GetCommitTime() > 0);
    ASSERT_TRUE(version1.GetCommitTime() > 0);
    ASSERT_TRUE(version1.GetCommitTime() > version.GetCommitTime());

    auto [status2, version2] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/retryCnt, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870918));
    ASSERT_FALSE(status2.IsOK());
}

TEST_F(TabletCommitterTest, testVersionLine)
{
    const std::string rootDir = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;
    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    for (size_t i = 0; i < 5; ++i) {
        ASSERT_TRUE(AddOneSegment(tabletData, tabletCommitter, (segmentid_t)(i), (int64_t)i, nullptr));
    }
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    int32_t retryCnt = 3;
    auto fence = CreateFence(rootDir, "fence", fsOptions);
    auto [status, version] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/retryCnt, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870915));
    ASSERT_TRUE(status.IsOK());
    for (size_t i = 5; i < 10; ++i) {
        ASSERT_TRUE(AddOneSegment(tabletData, tabletCommitter, (segmentid_t)(i), (int64_t)i, nullptr));
    }

    auto [status1, version1] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/retryCnt, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870918));
    ASSERT_TRUE(status1.IsOK());

    VersionCoord versionCoord(version.GetVersionId(), version.GetFenceName());
    ASSERT_TRUE(version1.CanFastFowardFrom(versionCoord, /*hasBuildingSegment*/ false));
    ASSERT_EQ(version.GetVersionId(), version1.GetVersionLine().GetParentVersion().GetVersionId());
}

TEST_F(TabletCommitterTest, testIsExist)
{
    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    std::map<std::string, std::string> params1;
    IndexTaskMetaCreator creator;
    auto meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params1).Create();
    tabletCommitter->HandleIndexTask(meta, Action::ADD);
    ASSERT_TRUE(tabletCommitter->HasIndexTask(BULKLOAD_TASK_TYPE, /*taskName=*/"123"));
}

TEST_F(TabletCommitterTest, testAddIndexTasks)
{
    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    std::map<std::string, std::string> params1;
    IndexTaskMetaCreator creator1;
    auto meta1 = creator1.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params1).Create();
    tabletCommitter->HandleIndexTask(meta1, Action::ADD);
    std::map<std::string, std::string> params2;
    params2["key1"] = "value1";
    IndexTaskMetaCreator creator2;
    auto meta2 = creator2.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("456").Params(params2).Create();
    tabletCommitter->HandleIndexTask(meta2, Action::ADD);

    ASSERT_TRUE(tabletCommitter->NeedCommit());

    indexlib::file_system::FileSystemOptions fsOptions;
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    auto fence = CreateFence(GET_TEMP_DATA_PATH(), "fence", fsOptions);
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    auto [status, version] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/1, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870912));
    ASSERT_TRUE(status.IsOK());
    auto task1 = version.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, /*taskName=*/"123");
    ASSERT_TRUE(task1);
    ASSERT_EQ(task1->GetState(), IndexTaskMeta::PENDING);
    auto task2 = version.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, /*taskName=*/"456");
    ASSERT_TRUE(task2);
    ASSERT_EQ(task2->GetState(), IndexTaskMeta::PENDING);
}

TEST_F(TabletCommitterTest, testSuspendIndexTasks)
{
    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    IndexTaskMetaCreator creator;

    std::map<std::string, std::string> params1;
    auto meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params1).Create();
    tabletCommitter->HandleIndexTask(meta, Action::ADD);

    std::map<std::string, std::string> params2;
    params2["key2"] = "value2";
    meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params2).Create();
    tabletCommitter->HandleIndexTask(meta, Action::SUSPEND);

    std::map<std::string, std::string> params3;
    params3["key3"] = "value3";
    meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("456").Params(params3).Create();
    tabletCommitter->HandleIndexTask(meta, Action::SUSPEND);

    ASSERT_TRUE(tabletCommitter->NeedCommit());

    indexlib::file_system::FileSystemOptions fsOptions;
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    auto fence = CreateFence(GET_TEMP_DATA_PATH(), "fence", fsOptions);
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    auto [status, version] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/1, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870912));
    ASSERT_TRUE(status.IsOK());
    auto task1 = version.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, /*taskName=*/"123");
    ASSERT_TRUE(task1);
    ASSERT_EQ(task1->GetState(), IndexTaskMeta::SUSPENDED);
    ASSERT_EQ(task1->GetParams(), params1);
    auto task2 = version.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, /*taskName=*/"456");
    ASSERT_TRUE(task2);
    ASSERT_EQ(task2->GetState(), IndexTaskMeta::SUSPENDED);
    ASSERT_EQ(task2->GetParams(), params3);
}

TEST_F(TabletCommitterTest, testAbortIndexTasks)
{
    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    IndexTaskMetaCreator creator;

    std::map<std::string, std::string> params1;
    auto meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params1).Create();
    tabletCommitter->HandleIndexTask(meta, Action::ADD);

    std::map<std::string, std::string> params2;
    params2["key2"] = "value2";
    meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params2).Create();
    tabletCommitter->HandleIndexTask(meta, Action::ABORT);

    std::map<std::string, std::string> params3;
    params3["key3"] = "value3";
    meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("456").Params(params3).Create();
    tabletCommitter->HandleIndexTask(meta, Action::ABORT);

    ASSERT_TRUE(tabletCommitter->NeedCommit());

    indexlib::file_system::FileSystemOptions fsOptions;
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    auto fence = CreateFence(GET_TEMP_DATA_PATH(), "fence", fsOptions);
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    auto [status, version] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/1, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870912));
    ASSERT_TRUE(status.IsOK());
    auto task1 = version.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, /*taskName=*/"123");
    ASSERT_TRUE(task1);
    ASSERT_EQ(task1->GetState(), IndexTaskMeta::ABORTED);
    ASSERT_EQ(task1->GetParams(), params1);
    auto task2 = version.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, /*taskName=*/"456");
    // try to abort non-exist task, action abort will be ignored
    ASSERT_FALSE(task2);
}

TEST_F(TabletCommitterTest, testOverwriteIndexTasks_0)
{
    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    IndexTaskMetaCreator creator;

    std::map<std::string, std::string> params1;
    params1[PARAM_LAST_SEQUENCE_NUMBER] = "111";
    params1["key"] = "value111";
    auto meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params1).Create();
    tabletCommitter->HandleIndexTask(meta, Action::ADD);

    std::map<std::string, std::string> params2;
    params2["key"] = "value222";
    meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params2).Create();
    tabletCommitter->HandleIndexTask(meta, Action::OVERWRITE);

    std::map<std::string, std::string> params3;
    params3["key3"] = "value3";
    meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("456").Params(params3).Create();
    tabletCommitter->HandleIndexTask(meta, Action::OVERWRITE);

    ASSERT_TRUE(tabletCommitter->NeedCommit());

    indexlib::file_system::FileSystemOptions fsOptions;
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    auto fence = CreateFence(GET_TEMP_DATA_PATH(), "fence", fsOptions);
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    auto [status, version] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/1, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870912));
    ASSERT_TRUE(status.IsOK());
    auto task1 = version.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, /*taskName=*/"123");
    ASSERT_TRUE(task1);
    ASSERT_EQ(task1->GetState(), IndexTaskMeta::PENDING);
    std::map<std::string, std::string> params;
    params[PARAM_LAST_SEQUENCE_NUMBER] = "111";
    params["key"] = "value222";
    ASSERT_EQ(task1->GetParams(), params);

    auto task2 = version.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, /*taskName=*/"456");
    // try to overwrite non-exist task, action abort will be ignored
    ASSERT_FALSE(task2);
}

TEST_F(TabletCommitterTest, testOverwriteIndexTasks_1)
{
    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    IndexTaskMetaCreator creator;

    std::map<std::string, std::string> params1;
    params1[PARAM_LAST_SEQUENCE_NUMBER] = "111";
    params1["key"] = "value111";
    auto meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params1).Create();
    tabletCommitter->HandleIndexTask(meta, Action::ADD);

    std::map<std::string, std::string> params2;
    params2["key"] = "value222";
    meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params2).Create();
    tabletCommitter->HandleIndexTask(meta, Action::ABORT);

    std::map<std::string, std::string> params3;
    params3["key3"] = "value3";
    meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params3).Create();
    tabletCommitter->HandleIndexTask(meta, Action::OVERWRITE);

    ASSERT_TRUE(tabletCommitter->NeedCommit());

    indexlib::file_system::FileSystemOptions fsOptions;
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    auto fence = CreateFence(GET_TEMP_DATA_PATH(), "fence", fsOptions);
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    auto [status, version] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/1, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870912));
    ASSERT_TRUE(status.IsOK());
    auto task1 = version.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, /*taskName=*/"123");
    ASSERT_TRUE(task1);
    // aborted task will not be overwritten
    ASSERT_EQ(task1->GetState(), IndexTaskMeta::ABORTED);
    ASSERT_TRUE(task1->GetParams().empty());
}

TEST_F(TabletCommitterTest, testInvalidAction)
{
    auto tabletCommitter = CreateEmptyTabletCommiter(_tabletName);
    IndexTaskMetaCreator creator;

    std::map<std::string, std::string> params1;
    params1[PARAM_LAST_SEQUENCE_NUMBER] = "111";
    params1["key"] = "value111";
    auto meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId("123").Params(params1).Create();
    tabletCommitter->HandleIndexTask(meta, Action::UNKNOWN);

    ASSERT_TRUE(tabletCommitter->NeedCommit());

    indexlib::file_system::FileSystemOptions fsOptions;
    auto tabletData = std::make_shared<TabletData>(_tabletName);
    auto fence = CreateFence(GET_TEMP_DATA_PATH(), "fence", fsOptions);
    IdGenerator idGenerator(IdMaskType::BUILD_PUBLIC);
    auto [status, version] =
        tabletCommitter->Commit(tabletData, fence, /*retry=*/1, &idGenerator,
                                CommitOptions().SetNeedPublish(true).SetTargetVersionId(536870912));
    ASSERT_TRUE(status.IsOK());
    // invalid action task will be ignored
    auto task1 = version.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, /*taskName=*/"123");
    ASSERT_FALSE(task1);
}

} // namespace indexlibv2::framework
