#include "indexlib/framework/Version.h"

#include <chrono>
#include <string>

#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/VersionCommitter.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::framework {

class VersionTest : public TESTBASE
{
public:
    VersionTest() {}
    ~VersionTest() {}

    void setUp() override {}
    void tearDown() override {}

private:
    void CheckEqual(const Version& v1, const Version& v2);
};

TEST_F(VersionTest, FromString)
{
    string str = "{\"segments\":[1,3,12],\"segment_branch_names\":[\"d1\",\"d3\",\"d12\"],\"versionid\":10}";
    Version v1;
    ASSERT_TRUE(v1.FromString(str).IsOK());

    Version v2;
    v2.SetFenceName("d1");
    v2.AddSegment(1);
    v2.SetFenceName("d3");
    v2.AddSegment(3);
    v2.SetFenceName("d12");
    v2.AddSegment(12);
    v2.SetVersionId(10);

    CheckEqual(v1, v2);
}

TEST_F(VersionTest, FromStringWithTimestamp)
{
    string str = "{\"segments\":[1,3,12],\"segment_branch_names\":[\"d1\",\"d3\",\"d12\"],\"versionid\":10,"
                 "\"timestamp\":123}";
    Version v1;
    ASSERT_TRUE(v1.FromString(str).IsOK());

    Version v2;
    v2.SetFenceName("d1");
    v2.AddSegment(1);
    v2.SetFenceName("d3");
    v2.AddSegment(3);
    v2.SetFenceName("d12");
    v2.AddSegment(12);
    v2.SetVersionId(10);
    v2.SetTimestamp(123);

    CheckEqual(v1, v2);
}

TEST_F(VersionTest, FromStringWithCommitTime)
{
    string str = "{\"segments\":[1,3,12],\"segment_branch_names\":[\"d1\",\"d3\",\"d12\"],\"versionid\":10,"
                 "\"commit_time\":1234}";
    Version v1;
    ASSERT_TRUE(v1.FromString(str).IsOK());

    Version v2;
    v2.SetFenceName("d1");
    v2.AddSegment(1);
    v2.SetFenceName("d3");
    v2.AddSegment(3);
    v2.SetFenceName("d12");
    v2.AddSegment(12);
    v2.SetVersionId(10);
    v2.SetCommitTime(1234);

    CheckEqual(v1, v2);
}

TEST_F(VersionTest, FromStringWithBadStr)
{
    string str = "{\"segments\":[1,3,12] \"versionid\":10}";
    Version v1;
    auto status = v1.FromString(str);
    ASSERT_TRUE(status.IsConfigError());
}

TEST_F(VersionTest, SchemaRoadMap)
{
    string str = "{\"segments\":[1,3,12], \"versionid\":10}";
    Version v1;
    auto status = v1.FromString(str);
    vector<schemaid_t> targetSchemaRoadMap = {0};
    ASSERT_EQ(targetSchemaRoadMap, v1.GetSchemaVersionRoadMap());
    ASSERT_TRUE(v1.IsInSchemaVersionRoadMap(0));
    ASSERT_FALSE(v1.IsInSchemaVersionRoadMap(1));

    v1.SetSchemaId(1);
    v1.SetSchemaId(3);
    targetSchemaRoadMap = {0, 1, 3};
    ASSERT_EQ(targetSchemaRoadMap, v1.GetSchemaVersionRoadMap());
    ASSERT_TRUE(v1.IsInSchemaVersionRoadMap(1));
    ASSERT_TRUE(v1.IsInSchemaVersionRoadMap(3));
    ASSERT_FALSE(v1.IsInSchemaVersionRoadMap(4));

    str = v1.ToString();
    Version v2;
    status = v2.FromString(str);
    ASSERT_EQ(targetSchemaRoadMap, v2.GetSchemaVersionRoadMap());
    ASSERT_TRUE(v2.IsInSchemaVersionRoadMap(1));
    ASSERT_TRUE(v2.IsInSchemaVersionRoadMap(3));
    ASSERT_FALSE(v2.IsInSchemaVersionRoadMap(4));
}

TEST_F(VersionTest, ToString)
{
    Version v1;
    v1.SetFenceName("d1");
    v1.AddSegment(1);
    v1.SetFenceName("d3");
    v1.AddSegment(3);
    v1.SetFenceName("d12");
    v1.AddSegment(12);
    v1.SetVersionId(10);
    v1.SetTimestamp(123);
    v1.SetCommitTime(1234);

    auto buf = v1.ToString();

    Version v2;
    auto status = v2.FromString(buf);
    ASSERT_TRUE(status.IsOK());

    CheckEqual(v1, v2);
}

void VersionTest::CheckEqual(const Version& v1, const Version& v2)
{
    ASSERT_EQ(v1.GetVersionId(), v2.GetVersionId());
    ASSERT_EQ(v1.GetSegmentCount(), v2.GetSegmentCount());
    ASSERT_EQ(v1.GetTimestamp(), v2.GetTimestamp());
    ASSERT_EQ(v1.GetCommitTime(), v2.GetCommitTime());
    auto iter1 = v1.begin();
    auto iter2 = v2.begin();
    while (iter1 != v1.end() && iter2 != v2.end()) {
        ASSERT_EQ(*iter1, *iter2) << "v1: " << iter1->segmentId << "; v2: " << iter2->segmentId << ";";
        iter1++;
        iter2++;
    }
}

TEST_F(VersionTest, TestClone)
{
    {
        Version orgVersion;
        Version copyVersion;
        copyVersion = orgVersion.Clone(std::string(orgVersion.GetFenceName()));
        CheckEqual(orgVersion, copyVersion);
    }
    {
        Version orgVersion(2, 100);
        orgVersion.AddSegment(1);

        Version copyVersion;
        copyVersion = orgVersion.Clone(std::string(orgVersion.GetFenceName()));
        CheckEqual(orgVersion, copyVersion);
    }
    {
        Version orgVersion;
        Version copyVersion;
        orgVersion._locator.SetLegacyLocator();
        copyVersion = orgVersion.Clone(std::string(orgVersion.GetFenceName()));
        CheckEqual(orgVersion, copyVersion);
        ASSERT_EQ(orgVersion.GetLocator(), copyVersion.GetLocator());
        ASSERT_EQ(orgVersion.GetLocator().IsLegacyLocator(), copyVersion.GetLocator().IsLegacyLocator());
    }
    {
        Version orgVersion(2, 100);
        orgVersion.AddSegment(1);
        orgVersion._locator.SetLegacyLocator();

        Version copyVersion;
        copyVersion = orgVersion.Clone(std::string(orgVersion.GetFenceName()));
        CheckEqual(orgVersion, copyVersion);
        ASSERT_EQ(orgVersion.GetLocator(), copyVersion.GetLocator());
        ASSERT_EQ(orgVersion.GetLocator().IsLegacyLocator(), copyVersion.GetLocator().IsLegacyLocator());
    }
}

TEST_F(VersionTest, TestSetLegacyLocator)
{
    std::string jsonStr = R"(
    {
        "segments": [1,2],
        "versionid": 10,
        "format_version" : 2,
        "segment_descriptions": {
            "segment_statistics": [
                {
                    "segment_id": 1,
                    "integer_stats": {
                        "eventtime": [100, 201]
                    }
                },
                {
                    "segment_id": 2,
                    "integer_stats": {
                        "eventtime": [300, 400]
                    }
                }
            ]
        }
    })";
    Version v1;
    FromJsonString(v1, jsonStr);
    ASSERT_EQ(3, v1.GetFormatVersion());
    ASSERT_TRUE(v1.GetLocator().IsLegacyLocator());
}

TEST_F(VersionTest, AddSegmentError)
{
    // TODO(tianwei) Add this UT.
}

TEST_F(VersionTest, TestValidateSegmentIds)
{
    // TODO(tianwei) Add this UT.
}

TEST_F(VersionTest, TestStore)
{
    const std::string rootDir = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string fenceRoot = PathUtil::JoinPath(rootDir, "fence");
    auto fs = indexlib::file_system::FileSystemCreator::Create(/*name*/ "", fenceRoot, fsOptions).GetOrThrow();
    Fence fence(rootDir, "fence", fs);
    Version version;
    auto status = VersionCommitter::CommitAndPublish(version, fence, {});
    ASSERT_TRUE(status.IsInvalidArgs());
    version.SetVersionId(0);
    version.SetFenceName("d1");
    version.AddSegment(1);

    status = VersionCommitter::CommitAndPublish(version, fence, {});
    ASSERT_TRUE(status.IsOK());

    std::string versionFile("version.0");
    auto checkExist = [&versionFile](const std::shared_ptr<indexlib::file_system::Directory>& dir,
                                     bool isMasterDir) -> bool {
        bool exist = dir->IsExist(versionFile);
        if (!exist && !isMasterDir) {
            std::cout << "file not in logic fs" << std::endl;
            return false;
        }
        auto physicalVersionFile = PathUtil::JoinPath(dir->GetOutputPath(), versionFile);
        auto ec = indexlib::file_system::FslibWrapper::IsExist(physicalVersionFile, exist);
        if (ec == indexlib::file_system::ErrorCode::FSEC_OK && exist) {
            return true;
        }
        std::cout << "file not in physical fs" << std::endl;
        return false;
    };
    ASSERT_TRUE(checkExist(indexlib::file_system::Directory::Get(fs), /*isMasterDir*/ false));
    ASSERT_TRUE(checkExist(indexlib::file_system::Directory::GetPhysicalDirectory(rootDir),
                           /*isMasterDir*/ true));
}

TEST_F(VersionTest, AddSegment)
{
    Version v;
    v.SetFenceName("d1");
    v.AddSegment(1);
    v.AddSegment(3);
    v.SetVersionId(10);

    vector<segmentid_t> expected = {1, 3};
    ASSERT_EQ(expected.size(), v.end() - v.begin());
    size_t idx = 0;
    for (auto [seg, _] : v) {
        EXPECT_EQ(expected[idx++], seg);
    }
}

TEST_F(VersionTest, RemoveSegment)
{
    // TODO(tianwei) Add this UT.
}

TEST_F(VersionTest, TestAddSegment)
{
    Version version;
    version.AddSegment(1);
    version.SetLastSegmentId(500);
    version.AddSegment(10);
    ASSERT_EQ(500, version.GetLastSegmentId());
}

TEST_F(VersionTest, testSchemaId)
{
    string jsonStr = R"({
        "versionid": 1,
        "segments": [1],
        "schema_version": 100
    })";

    Version version;
    ASSERT_TRUE(version.FromString(jsonStr).IsOK());
    ASSERT_EQ(100, version.GetSchemaId());
    auto iter = version.begin();
    ASSERT_EQ(1, iter->segmentId);
    ASSERT_EQ(100, iter->schemaId);
    ASSERT_EQ(100, version.GetReadSchemaId());

    version.SetSchemaId(101);
    version.AddSegment(2);
    version.SetReadSchemaId(50);

    std::string content = version.ToString();
    Version version2;
    ASSERT_TRUE(version2.FromString(content).IsOK());
    ASSERT_EQ(101, version2.GetSchemaId());
    iter = version2.begin();
    ASSERT_EQ(1, iter->segmentId);
    ASSERT_EQ(100, iter->schemaId);
    ++iter;
    ASSERT_EQ(2, iter->segmentId);
    ASSERT_EQ(101, iter->schemaId);
    ASSERT_EQ(50, version.GetReadSchemaId());
}

TEST_F(VersionTest, TestSegmentStatistics)
{
    string str = R"(
    {
        "segments": [1,2],
        "versionid": 10,
        "segment_descriptions": {
            "segment_statistics": [
                {
                    "segment_id": 1,
                    "integer_stats": {
                        "eventtime": [100, 200]
                    }
                },
                {
                    "segment_id": 2,
                    "integer_stats": {
                        "eventtime": [300, 400]
                    }
                }
            ]
        }
    })";
    Version v1;
    FromJsonString(v1, str);

    auto checkSegmentStats = [](const Version& version) {
        auto segDescriptions = version.GetSegmentDescriptions();
        ASSERT_TRUE(segDescriptions != nullptr);
        const std::vector<indexlibv2::framework::SegmentStatistics>& segStatsVec =
            segDescriptions->GetSegmentStatisticsVector();
        ASSERT_EQ(2, segStatsVec.size());
        auto integerStats = segStatsVec[0].GetIntegerStats();
        ASSERT_EQ(1, segStatsVec[0].GetSegmentId());
        ASSERT_EQ(1, integerStats.size());
        auto it = integerStats.find("eventtime");
        ASSERT_TRUE(it != integerStats.end());
        ASSERT_EQ((std::make_pair<int64_t, int64_t>(100, 200)), it->second);

        integerStats = segStatsVec[1].GetIntegerStats();
        ASSERT_EQ(2, segStatsVec[1].GetSegmentId());
        ASSERT_EQ(1, integerStats.size());
        it = integerStats.find("eventtime");
        ASSERT_TRUE(it != integerStats.end());
        ASSERT_EQ((std::make_pair<int64_t, int64_t>(300, 400)), it->second);
    };

    checkSegmentStats(v1);

    Version v2;
    string v1Str = ToJsonString(v1);
    FromJsonString(v2, v1Str);
    checkSegmentStats(v2);

    ASSERT_EQ(v1Str, ToJsonString(v2));

    Version v3;
    v3 = v2;
    checkSegmentStats(v3);
    ASSERT_EQ(v1Str, ToJsonString(v3));

    Version v4(v2);
    checkSegmentStats(v4);
    ASSERT_EQ(v1Str, ToJsonString(v4));

    string str2 = R"(
    {
        "segments": [1,2],
        "versionid": 10,
        "segment_descriptions": {
            "segment_statistics": [
                {
                    "segment_id": 1,
                    "integer_stats": {
                        "eventtime": [100, 201]
                    }
                },
                {
                    "segment_id": 2,
                    "integer_stats": {
                        "eventtime": [300, 400]
                    }
                }
            ]
        }
    })";
    Version v5;
    FromJsonString(v5, str2);
    ASSERT_NE(v1Str, ToJsonString(v5));
}

TEST_F(VersionTest, TestSegmentStatisticsCompatible)
{
    // legacy
    string legacyJsonStr = R"({
        "versionid": 1,
        "segments": [1],
        "segment_temperature_metas": [{"segment_id":1, "segment_temperature": "HOT", "segment_temperature_detail": "HOT:1"}],
        "segment_statistics": [{"segment_id":1, "integer_stats": {"event_time" : [-100, 0]}}]
    })";

    Version version1;
    ASSERT_TRUE(version1.FromString(legacyJsonStr).IsOK());
    auto segStats = version1.GetSegmentDescriptions()->GetSegmentStatisticsVector();
    ASSERT_EQ(1, segStats.size());
    ASSERT_EQ(1, segStats[0].GetSegmentId());
    using IntegerRangeType = SegmentStatistics::IntegerRangeType;
    IntegerRangeType integerRange;
    ASSERT_FALSE(segStats[0].GetStatistic("non-exist-key", integerRange));
    ASSERT_TRUE(segStats[0].GetStatistic("event_time", integerRange));
    ASSERT_EQ(IntegerRangeType(-100, 0), integerRange);
}

TEST_F(VersionTest, TestVersionLine)
{
    VersionLine emptyVersionLine;
    Version version1;
    version1.SetFenceName("fence_a");
    version1.SetVersionId(100 | Version::PUBLIC_VERSION_ID_MASK);
    version1.SetVersionLine(emptyVersionLine);
    version1.Finalize();

    Version version2;
    version2.SetFenceName("fence_3");
    version2.SetVersionId(101 | Version::PUBLIC_VERSION_ID_MASK);
    version2.SetVersionLine(version1.GetVersionLine());
    version2.Finalize();

    auto& newVersionLine = version2.GetVersionLine();
    ASSERT_EQ(100 | Version::PUBLIC_VERSION_ID_MASK, newVersionLine.GetParentVersion().GetVersionId());
    VersionCoord version1Coord(version1.GetVersionId(), version1.GetFenceName());
    ASSERT_TRUE(version2.CanFastFowardFrom(version1Coord, /*hasBuildingSegment*/ false));

    Version version3;
    FromJsonString(version3, ToJsonString(version2));
    ASSERT_EQ(version2.GetVersionLine(), version3.GetVersionLine());
}

TEST_F(VersionTest, TestIndexTaskStateTransfer)
{
    Version version;
    ASSERT_TRUE(version.ValidateStateTransfer(IndexTaskMeta::DONE, IndexTaskMeta::DONE));
    ASSERT_FALSE(version.ValidateStateTransfer(IndexTaskMeta::DONE, IndexTaskMeta::READY));
    ASSERT_FALSE(version.ValidateStateTransfer(IndexTaskMeta::DONE, IndexTaskMeta::SUSPENDED));
    ASSERT_FALSE(version.ValidateStateTransfer(IndexTaskMeta::DONE, IndexTaskMeta::ABORTED));

    ASSERT_TRUE(version.ValidateStateTransfer(IndexTaskMeta::READY, IndexTaskMeta::READY));
    ASSERT_TRUE(version.ValidateStateTransfer(IndexTaskMeta::READY, IndexTaskMeta::SUSPENDED));
    ASSERT_TRUE(version.ValidateStateTransfer(IndexTaskMeta::READY, IndexTaskMeta::ABORTED));
    ASSERT_TRUE(version.ValidateStateTransfer(IndexTaskMeta::READY, IndexTaskMeta::DONE));

    ASSERT_TRUE(version.ValidateStateTransfer(IndexTaskMeta::SUSPENDED, IndexTaskMeta::SUSPENDED));
    ASSERT_TRUE(version.ValidateStateTransfer(IndexTaskMeta::SUSPENDED, IndexTaskMeta::READY));
    ASSERT_TRUE(version.ValidateStateTransfer(IndexTaskMeta::SUSPENDED, IndexTaskMeta::ABORTED));
    ASSERT_FALSE(version.ValidateStateTransfer(IndexTaskMeta::SUSPENDED, IndexTaskMeta::DONE));

    ASSERT_TRUE(version.ValidateStateTransfer(IndexTaskMeta::ABORTED, IndexTaskMeta::ABORTED));
    ASSERT_FALSE(version.ValidateStateTransfer(IndexTaskMeta::ABORTED, IndexTaskMeta::READY));
    ASSERT_FALSE(version.ValidateStateTransfer(IndexTaskMeta::ABORTED, IndexTaskMeta::SUSPENDED));
    ASSERT_FALSE(version.ValidateStateTransfer(IndexTaskMeta::ABORTED, IndexTaskMeta::DONE));
}

TEST_F(VersionTest, TestUpdateIndexTaskState)
{
    Version version;
    std::string taskType1 = "type1";
    std::string taskName1 = "name1";
    std::map<std::string, std::string> params1;
    params1["key1"] = "value1";

    version.UpdateIndexTaskState(taskType1, taskName1, IndexTaskMeta::DONE);
    auto task = version.GetIndexTask(taskType1, taskName1);
    ASSERT_FALSE(task);

    version.AddIndexTask(taskType1, taskName1, params1);
    task = version.GetIndexTask(taskType1, taskName1);
    ASSERT_TRUE(task);
    ASSERT_EQ(task->state, IndexTaskMeta::READY);

    version.UpdateIndexTaskState(taskType1, taskName1, IndexTaskMeta::DONE);
    ASSERT_EQ(task->state, IndexTaskMeta::DONE);
    ASSERT_TRUE(task->params.empty());
    ASSERT_NE(task->endTimeInSecs, indexlib::INVALID_TIMESTAMP);
}

TEST_F(VersionTest, TestOverwriteIndexTask)
{
    Version version;
    std::string taskType1 = "type1";
    std::string taskName1 = "name1";
    std::map<std::string, std::string> params1;
    params1["key1"] = "value1";
    std::map<std::string, std::string> params2;
    params2["key2"] = "value2";

    version.OverwriteIndexTask(taskType1, taskName1, params1);
    auto task = version.GetIndexTask(taskType1, taskName1);
    ASSERT_FALSE(task);

    version.AddIndexTask(taskType1, taskName1, params1);
    task = version.GetIndexTask(taskType1, taskName1);
    ASSERT_TRUE(task);
    ASSERT_EQ(task->state, IndexTaskMeta::READY);
    auto firstBeginTime = task->beginTimeInSecs;
    ASSERT_NE(firstBeginTime, indexlib::INVALID_TIMESTAMP);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    version.OverwriteIndexTask(taskType1, taskName1, params2);
    task = version.GetIndexTask(taskType1, taskName1);
    ASSERT_TRUE(task);
    ASSERT_EQ(task->state, IndexTaskMeta::READY);
    auto secondBeginTime = task->beginTimeInSecs;
    ASSERT_NE(secondBeginTime, indexlib::INVALID_TIMESTAMP);
    ASSERT_NE(firstBeginTime, secondBeginTime);
    ASSERT_EQ(task->params, params2);
}

TEST_F(VersionTest, TestAddIndexTask)
{
    Version version;
    std::string taskType1 = "type1";
    std::string taskName1 = "name1";
    std::map<std::string, std::string> params1;
    params1["key1"] = "value1";
    std::map<std::string, std::string> params2;
    params2["key2"] = "value2";

    version.AddIndexTask(taskType1, taskName1, params1);
    auto task = version.GetIndexTask(taskType1, taskName1);
    ASSERT_TRUE(task);
    ASSERT_EQ(task->state, IndexTaskMeta::READY);
    ASSERT_EQ(task->params, params1);

    version.AddIndexTask(taskType1, taskName1, params2);
    task = version.GetIndexTask(taskType1, taskName1);
    ASSERT_TRUE(task);
    ASSERT_EQ(task->state, IndexTaskMeta::READY);
    ASSERT_EQ(task->params, params1);
}

TEST_F(VersionTest, TestSimpleIndexTasks)
{
    // task to be done
    std::string taskType1 = "type1";
    std::string taskName1 = "name1";
    std::map<std::string, std::string> params1;
    params1["key1"] = "value1";
    // task to be overwritten
    std::string taskType2 = "type2";
    std::string taskName2 = "name2";
    std::map<std::string, std::string> params2;
    params2["key2"] = "wrong value";
    std::string comment2 = "invalid value";
    // task to be abort
    std::string taskType3 = "type3";
    std::string taskName3 = "name3";
    std::map<std::string, std::string> params3;
    params3["key3"] = "value3";

    Version version;
    version.AddIndexTask(taskType1, taskName1, params1);
    version.AddIndexTask(taskType2, taskName2, params2, IndexTaskMeta::SUSPENDED, comment2);
    version.AddIndexTask(taskType3, taskName3, params3);

    auto tasks = version.GetIndexTasks();
    ASSERT_EQ(tasks.size(), 3u);
    ASSERT_EQ(tasks[0]->taskType, "type1");
    ASSERT_EQ(tasks[0]->taskName, "name1");
    ASSERT_EQ(tasks[0]->state, IndexTaskMeta::READY);
    ASSERT_EQ(tasks[0]->params, params1);

    ASSERT_EQ(tasks[1]->taskType, "type2");
    ASSERT_EQ(tasks[1]->taskName, "name2");
    ASSERT_EQ(tasks[1]->state, IndexTaskMeta::SUSPENDED);
    ASSERT_EQ(tasks[1]->params, params2);
    ASSERT_EQ(tasks[1]->comment, comment2);

    ASSERT_EQ(tasks[2]->taskType, "type3");
    ASSERT_EQ(tasks[2]->taskName, "name3");
    ASSERT_EQ(tasks[2]->state, IndexTaskMeta::READY);
    ASSERT_EQ(tasks[2]->params, params3);
    // update state of task1
    version.UpdateIndexTaskState("type1", "name1", IndexTaskMeta::DONE);
    // overwrite task2
    params2["key2"] = "value2";
    version.OverwriteIndexTask("type2", "name2", params2);
    // cancel task 3
    version.UpdateIndexTaskState("type3", "name3", IndexTaskMeta::ABORTED, "cancel task3");
    // check task status
    auto indexTask1 = version.GetIndexTask("type1", "name1");
    ASSERT_TRUE(indexTask1);
    ASSERT_EQ(indexTask1->state, IndexTaskMeta::DONE);

    auto indexTask2 = version.GetIndexTask("type2", "name2");
    ASSERT_TRUE(indexTask2);
    ASSERT_EQ(indexTask2->state, IndexTaskMeta::READY);
    ASSERT_EQ(indexTask2->params, params2);
    ASSERT_EQ(indexTask2->comment, "");

    auto indexTask3 = version.GetIndexTask("type3", "name3");
    ASSERT_TRUE(indexTask3);
    ASSERT_EQ(indexTask3->state, IndexTaskMeta::ABORTED);
    ASSERT_EQ(indexTask3->comment, "cancel task3");
}

TEST_F(VersionTest, TestIndexTaskJsonize)
{
    string jsonStr = R"({
    "versionid":1,
    "segments":[
        1
    ],
    "index_task_queue":[
            {
                "params":{
                    "key1":"value1"
                },
                "state": "DONE",
                "task_name":"name1",
                "task_type":"type1"
            },
            {
                "params":{
                    "key2":"value2"
                },
                "state": "READY",
                "task_name":"name2",
                "task_type":"type2"
            }
    ]})";

    Version version;
    ASSERT_TRUE(version.FromString(jsonStr).IsOK());
    auto tasks = version.GetIndexTasks();
    ASSERT_EQ(tasks.size(), 2);
    {
        auto task = version.GetIndexTask("type1", "name1");
        ASSERT_TRUE(task);
        ASSERT_EQ(task->taskName, "name1");
        ASSERT_EQ(task->taskType, "type1");
        ASSERT_EQ(task->state, IndexTaskMeta::DONE);
        std::map<std::string, std::string> expectParams;
        expectParams["key1"] = "value1";
        ASSERT_EQ(task->params, expectParams);
    }
    {
        auto task = version.GetIndexTask("type2", "name2");
        ASSERT_TRUE(task);
        ASSERT_EQ(task->taskName, "name2");
        ASSERT_EQ(task->taskType, "type2");
        ASSERT_EQ(task->state, IndexTaskMeta::READY);
        std::map<std::string, std::string> expectParams;
        expectParams["key2"] = "value2";
        ASSERT_EQ(task->params, expectParams);
    }

    std::string content = version.ToString();
    Version version2;
    ASSERT_TRUE(version2.FromString(content).IsOK());
    {
        auto task = version2.GetIndexTask("type1", "name1");
        ASSERT_TRUE(task);
        ASSERT_EQ(task->taskName, "name1");
        ASSERT_EQ(task->taskType, "type1");
        ASSERT_EQ(task->state, IndexTaskMeta::DONE);
        std::map<std::string, std::string> expectParams;
        expectParams["key1"] = "value1";
        ASSERT_EQ(task->params, expectParams);
    }
    {
        auto task = version2.GetIndexTask("type2", "name2");
        ASSERT_TRUE(task);
        ASSERT_EQ(task->taskName, "name2");
        ASSERT_EQ(task->taskType, "type2");
        ASSERT_EQ(task->state, IndexTaskMeta::READY);
        std::map<std::string, std::string> expectParams;
        expectParams["key2"] = "value2";
        ASSERT_EQ(task->params, expectParams);
    }
}

TEST_F(VersionTest, TestIndexTaskJsonizeWithInvalidJson)
{
    string jsonStr = R"({
    "versionid":1,
    "segments":[
        1
    ],
    "index_task_queue":[
            {
                "params":{
                    "key1": 1
                },
                "state": "DONE",
                "task_name":"name",
                "task_type":"type"
            }
    ]})";

    Version version;
    ASSERT_FALSE(version.FromString(jsonStr).IsOK());
}

} // namespace indexlibv2::framework
