#include "indexlib/index_base/segment/test/online_segment_directory_unittest.h"

#include "indexlib/file_system/DiskStorage.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/lifecycle_strategy.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, OnlineSegmentDirectoryTest);

OnlineSegmentDirectoryTest::OnlineSegmentDirectoryTest() {}

OnlineSegmentDirectoryTest::~OnlineSegmentDirectoryTest() {}

void OnlineSegmentDirectoryTest::CaseSetUp() {}

void OnlineSegmentDirectoryTest::CaseTearDown() {}

void OnlineSegmentDirectoryTest::TestConvertToRtSegmentId()
{
    segmentid_t incId = 1;
    ASSERT_FALSE(RealtimeSegmentDirectory::IsRtSegmentId(incId));

    segmentid_t rtId = RealtimeSegmentDirectory::ConvertToRtSegmentId(incId);
    segmentid_t rtExpectId = 0x40000001;
    INDEXLIB_TEST_EQUAL(rtExpectId, rtId);
    ASSERT_TRUE(RealtimeSegmentDirectory::IsRtSegmentId(rtId));
}

void OnlineSegmentDirectoryTest::TestConvertToJoinSegmentId()
{
    segmentid_t segId = 1;
    ASSERT_FALSE(JoinSegmentDirectory::IsJoinSegmentId(segId));

    segmentid_t joinSegId = JoinSegmentDirectory::ConvertToJoinSegmentId(segId);
    INDEXLIB_TEST_EQUAL((segmentid_t)0x20000001, joinSegId);
    ASSERT_TRUE(JoinSegmentDirectory::IsJoinSegmentId(joinSegId));
}

void OnlineSegmentDirectoryTest::TestInitTwice()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
    OnlineSegmentDirectory segDir;
    segDir.Init(GET_PARTITION_DIRECTORY());

    Version version = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 0, "1", "3", "2");

    OnlineSegmentDirectory segDir2;
    segDir2.Init(GET_PARTITION_DIRECTORY());

    Version expect = VersionMaker::Make(0, "1", "3", "2");

    INDEXLIB_TEST_EQUAL(expect, segDir2.GetVersion())
        << ToJsonString(expect) << "||" << ToJsonString(segDir2.GetVersion());
}

void OnlineSegmentDirectoryTest::TestReopen()
{
    Version expectedVersion, actualVersion;
    {
        SegmentDirectoryPtr segDir(new OnlineSegmentDirectory());
        file_system::FileSystemOptions fsOptions;
        fsOptions.outputStorage = file_system::FSST_MEM;
        RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
        Version version =
            VersionMaker::Make(GET_PARTITION_DIRECTORY()->MakeDirectory("expected"), 1, "4,5,6", "1,2", "1");
        segDir->Init(GET_PARTITION_DIRECTORY()->GetDirectory("expected", true));
        expectedVersion = segDir->GetVersion();
    }
    {
        SegmentDirectoryPtr segDir(new OnlineSegmentDirectory());
        file_system::FileSystemOptions fsOptions;
        fsOptions.outputStorage = file_system::FSST_MEM;
        RESET_FILE_SYSTEM("ut2", true /* auto mount */, fsOptions);
        Version version =
            VersionMaker::Make(GET_PARTITION_DIRECTORY()->MakeDirectory("actual"), 0, "1,2,3", "1,2", "1");
        segDir->Init(GET_PARTITION_DIRECTORY()->GetDirectory("actual", true));
        Version newVersion =
            VersionMaker::Make(GET_PARTITION_DIRECTORY()->MakeDirectory("actual"), 1, "4,5,6", "1,2,3,4,5", "1,2");
        Version incVersion = VersionMaker::Make(1, "4,5,6", "", "");
        segDir->Reopen(incVersion);
        actualVersion = segDir->GetVersion();
    }
    ASSERT_EQ(expectedVersion, actualVersion);
}

void OnlineSegmentDirectoryTest::TestAddSegment()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion = VersionMaker::Make(partDirectory, 0, "0,1", "", "");
    OnlineSegmentDirectory segDir;
    segDir.Init(partDirectory, onDiskVersion);

    VersionMaker::MakeRealtimeSegment(partDirectory, 0);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(0), INVALID_TIMESTAMP);
    ASSERT_EQ(VersionMaker::Make(0, "0,1", "0", ""), segDir.GetVersion());

    VersionMaker::MakeRealtimeSegment(partDirectory, 1);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(1), INVALID_TIMESTAMP);
    ASSERT_EQ(VersionMaker::Make(0, "0,1", "0,1", ""), segDir.GetVersion());

    VersionMaker::MakeJoinSegment(partDirectory, 0);
    segDir.AddSegment(JoinSegmentDirectory::ConvertToJoinSegmentId(0), INVALID_TIMESTAMP);
    ASSERT_EQ(VersionMaker::Make(0, "0,1", "0,1", "0"), segDir.GetVersion());

    VersionMaker::MakeJoinSegment(partDirectory, 1);
    segDir.AddSegment(JoinSegmentDirectory::ConvertToJoinSegmentId(1), INVALID_TIMESTAMP);
    ASSERT_EQ(VersionMaker::Make(0, "0,1", "0,1", "1"), segDir.GetVersion());
}

void OnlineSegmentDirectoryTest::TestRemoveSegments()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion = VersionMaker::Make(partDirectory, 0, "0,1", "", "");
    OnlineSegmentDirectory segDir;
    segDir.Init(partDirectory, onDiskVersion);

    VersionMaker::MakeRealtimeSegment(partDirectory, 0);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(0), INVALID_TIMESTAMP);
    VersionMaker::MakeRealtimeSegment(partDirectory, 1);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(1), INVALID_TIMESTAMP);
    VersionMaker::MakeJoinSegment(partDirectory, 0);
    segDir.AddSegment(JoinSegmentDirectory::ConvertToJoinSegmentId(0), INVALID_TIMESTAMP);
    VersionMaker::MakeJoinSegment(partDirectory, 1);
    segDir.AddSegment(JoinSegmentDirectory::ConvertToJoinSegmentId(1), INVALID_TIMESTAMP);

    vector<segmentid_t> segIds;
    segIds.push_back(RealtimeSegmentDirectory::ConvertToRtSegmentId(0));
    segIds.push_back(JoinSegmentDirectory::ConvertToJoinSegmentId(1));
    segDir.RemoveSegments(segIds);
    ASSERT_EQ(VersionMaker::Make(0, "0,1", "1", ""), segDir.GetVersion());
}

void OnlineSegmentDirectoryTest::TestRefreshVersion()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion = VersionMaker::Make(rootDirectory, 0, "0,1", "", "");
    OnlineSegmentDirectory segDir;
    segDir.Init(GET_PARTITION_DIRECTORY(), onDiskVersion);

    Version rtVersion = VersionMaker::Make(rootDirectory, 0, "", "10,11", "");
    RealtimeSegmentDirectoryPtr rtSegDir(new RealtimeSegmentDirectory);
    rtSegDir->Init(segDir.mRtSegDir->GetRootDirectory(), rtVersion);

    Version joinVersion = VersionMaker::Make(rootDirectory, 0, "", "", "10");
    JoinSegmentDirectoryPtr joinSegDir(new JoinSegmentDirectory);
    joinSegDir->Init(segDir.mJoinSegDir->GetRootDirectory(), joinVersion);

    segDir.mRtSegDir = rtSegDir;
    segDir.mJoinSegDir = joinSegDir;

    segDir.RefreshVersion();

    Version expectVersion = VersionMaker::Make(0, "0,1", "10,11", "10");
    ASSERT_EQ(expectVersion, segDir.GetVersion());

    Version joinVersion2 = VersionMaker::Make(rootDirectory, 1, "", "", "20");
    joinSegDir->Init(segDir.mJoinSegDir->GetRootDirectory(), joinVersion2);

    segDir.RefreshVersion();

    Version expectVersion2 = VersionMaker::Make(0, "0,1", "10,11", "20");
    ASSERT_EQ(expectVersion2, segDir.GetVersion());
}

void OnlineSegmentDirectoryTest::TestInitSub()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion = VersionMaker::Make(partDirectory, 0, "0,1", "3", "2", 0, true);
    OnlineSegmentDirectory segDir;
    segDir.Init(partDirectory, index_base::Version(INVALID_VERSIONID), true);

    SegmentDirectoryPtr subSegDir = segDir.GetSubSegmentDirectory();
    ASSERT_TRUE(subSegDir);

    OnlineSegmentDirectoryPtr subOnlineSegDir = DYNAMIC_POINTER_CAST(OnlineSegmentDirectory, subSegDir);
    ASSERT_TRUE(subOnlineSegDir);
    ASSERT_TRUE(subOnlineSegDir->IsSubSegmentDirectory());

    ASSERT_EQ(onDiskVersion, subOnlineSegDir->GetVersion());

    SegmentDirectoryPtr subRtSegDir = subOnlineSegDir->GetRtSegmentDirectory();
    ASSERT_TRUE(subRtSegDir);
    ASSERT_TRUE(subRtSegDir->IsSubSegmentDirectory());

    SegmentDirectoryPtr subJoinSegDir = subOnlineSegDir->GetJoinSegmentDirectory();
    ASSERT_TRUE(subJoinSegDir);
    ASSERT_TRUE(subJoinSegDir->IsSubSegmentDirectory());
}

void OnlineSegmentDirectoryTest::TestInitSubForEmptyVersion()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    OnlineSegmentDirectory segDir;
    segDir.Init(partDirectory, index_base::Version(INVALID_VERSIONID), true);

    SegmentDirectoryPtr subSegDir = segDir.GetSubSegmentDirectory();
    ASSERT_TRUE(subSegDir);
}

void OnlineSegmentDirectoryTest::TestSwitchToLinkDirectoryForRtSegments()
{
    InnerTestSwitchToLinkDirectoryForRtSegments(false);
    InnerTestSwitchToLinkDirectoryForRtSegments(true);
}

void OnlineSegmentDirectoryTest::InnerTestSwitchToLinkDirectoryForRtSegments(bool flushRt)
{
    tearDown();
    setUp();

    RESET_FILE_SYSTEM(LoadConfigList(), false, true, true, flushRt);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion = VersionMaker::Make(partDirectory, 0, "0,1", "", "");
    OnlineSegmentDirectory segDir;
    segDir.Init(partDirectory, onDiskVersion);

    VersionMaker::MakeRealtimeSegment(partDirectory, 0);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(0), INVALID_TIMESTAMP);

    VersionMaker::MakeRealtimeSegment(partDirectory, 1);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(1), INVALID_TIMESTAMP);

    VersionMaker::MakeRealtimeSegment(partDirectory, 2);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(2), INVALID_TIMESTAMP);

    DirectoryPtr rtDirectory = partDirectory->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, true);
    rtDirectory->Sync(true);
    stringstream ss;
    ss << "segment_" << RealtimeSegmentDirectory::ConvertToRtSegmentId(2) << "_level_0/segment_info";
    rtDirectory->RemoveFile(ss.str());

    segmentid_t lastLinkRtSegId = INVALID_SEGMENTID;
    if (!flushRt) {
        lastLinkRtSegId = segDir.GetLastValidRtSegmentInLinkDirectory();
        ASSERT_EQ(INVALID_SEGMENTID, lastLinkRtSegId);
        ASSERT_FALSE(segDir.SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId));
    } else {
        lastLinkRtSegId = segDir.GetLastValidRtSegmentInLinkDirectory();
        ASSERT_EQ(RealtimeSegmentDirectory::ConvertToRtSegmentId(1), lastLinkRtSegId);
        ASSERT_TRUE(segDir.SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId));
    }
}

void OnlineSegmentDirectoryTest::TestMergeVersionWithSegmentStatistics()
{
    OnlineSegmentDirectory onlineSegmentDirectory;
    Version version, joinSegDirVersion, rtSegDirVersion;
    for (segmentid_t segmentId = 0; segmentId < 9; segmentId++) {
        indexlibv2::framework::SegmentStatistics segStats;
        segStats.SetSegmentId(segmentId);
        std::pair<int, int> stat(segmentId, segmentId + 1);
        segStats.AddStatistic("key", stat);
        if (segmentId < 3) {
            version.AddSegment(segmentId);
            version.AddSegmentStatistics(segStats);
        } else if (segmentId < 6) {
            joinSegDirVersion.AddSegment(segmentId);
            joinSegDirVersion.AddSegmentStatistics(segStats);
        } else {
            rtSegDirVersion.AddSegment(segmentId);
            rtSegDirVersion.AddSegmentStatistics(segStats);
        }
    }
    onlineSegmentDirectory.MergeVersion(version);
    onlineSegmentDirectory.MergeVersion(joinSegDirVersion);
    onlineSegmentDirectory.MergeVersion(rtSegDirVersion);
    const auto& newVersion = onlineSegmentDirectory.GetVersion();
    ASSERT_EQ(9, newVersion.GetSegmentCount());
    ASSERT_EQ(9, newVersion.GetSegmentStatisticsVector().size());
    for (segmentid_t segmentId = 0; segmentId < 9; segmentId++) {
        ASSERT_EQ(segmentId, newVersion[segmentId]);
        indexlibv2::framework::SegmentStatistics segStats;
        ASSERT_TRUE(newVersion.GetSegmentStatistics(segmentId, &segStats));
        ASSERT_EQ(segmentId, segStats.GetSegmentId());
        const auto& stats = segStats.GetIntegerStats();
        ASSERT_EQ(1, stats.size());
        auto it = stats.find("key");
        ASSERT_TRUE(it != stats.end());
        ASSERT_EQ((std::pair<int64_t, int64_t>(segmentId, segmentId + 1)), it->second);
    }
}

class FakeOnlineSegmentDirectory : public OnlineSegmentDirectory
{
public:
    FakeOnlineSegmentDirectory(bool enableRecover, std::shared_ptr<file_system::LifecycleConfig> lifecycleConfig,
                               int64_t* timeStamp)
        : OnlineSegmentDirectory(enableRecover, lifecycleConfig)
        , _timeStamp(timeStamp)
    {
    }

    void UpdateLifecycleStrategy() override
    {
        (*_timeStamp)++;
        if ((mLifecycleStrategy == nullptr) ||
            (mLifecycleConfig->GetStrategy() == file_system::LifecycleConfig::STATIC_STRATEGY)) {
            return;
        }
        auto newLifecycleStrategy = LifecycleStrategyFactory::CreateStrategy(
            *mLifecycleConfig, {{LifecyclePatternBase::CURRENT_TIME, std::to_string(*_timeStamp)}});

        if (newLifecycleStrategy == nullptr) {
            mLifecycleStrategy.reset();
        } else {
            mLifecycleStrategy.reset(newLifecycleStrategy.release());
        }
    }

private:
    int64_t* _timeStamp;
};

void OnlineSegmentDirectoryTest::TestDirectoryLifecycle()
{
    std::string lifecycleConfigStr = R"({
        "strategy" : "dynamic",
        "patterns": [
            {"statistic_field" : "eventtime", "statistic_type": "integer", "offset_base" : "CURRENT_TIME", "lifecycle":"hot", "range":[-2, 0], "is_offset": true},
            {"statistic_field" : "eventtime", "statistic_type": "integer", "offset_base" : "CURRENT_TIME","lifecycle":"warm", "range":[-4, -2], "is_offset": true},
            {"statistic_field" : "eventtime", "statistic_type": "integer", "offset_base" : "CURRENT_TIME","lifecycle":"cold", "range":[-100000, -4], "is_offset": true},
            {"statistic_field" : "eventtime", "statistic_type": "integer", "offset_base" : "CURRENT_TIME","lifecycle":"hot", "range":[10000, 10005], "is_offset": false},
            {"statistic_field" : "eventtime", "statistic_type": "integer", "offset_base" : "CURRENT_TIME","lifecycle":"cold", "range":[10005, 10007], "is_offset": false}
        ]
    })";
    string loadConfigListJson = R"({
        "load_config": [{"file_patterns": [".*"],"load_strategy": "mmap","deploy": true,"lifecycle": "hot"}]
    })";
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, loadConfigListJson);
    auto lifecycleConfig = std::make_shared<LifecycleConfig>();
    FromJsonString(*lifecycleConfig, lifecycleConfigStr);

    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    fsOptions.loadConfigList = loadConfigList;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion = VersionMaker::Make(partDirectory, 0, "0", "", "");
    onDiskVersion.AddSegmentStatistics({0, {{"eventtime", {10000, 10005}}}, {}});

    int64_t timeStamp = -1;
    std::shared_ptr<OnlineSegmentDirectory> segDir =
        std::make_shared<FakeOnlineSegmentDirectory>(false, lifecycleConfig, &timeStamp);
    segDir->Init(partDirectory, onDiskVersion);

    auto checkLifecycle = [&segDir, &partDirectory](segmentid_t segId, const std::string& segDataLifecycle,
                                                    const std::string& fileSystemLifecycle, bool isRt = false) {
        auto segData = segDir->GetSegmentData(segId);
        ASSERT_EQ(segId, segData.GetSegmentId());
        ASSERT_EQ(segDataLifecycle, segData.GetLifecycle());
        auto fs = partDirectory->GetFileSystem();
        auto inputStorage = fs->TEST_GetInputStorage();
        auto diskStorage = dynamic_pointer_cast<indexlib::file_system::DiskStorage>(inputStorage);
        auto lifecycleTable = diskStorage->_lifecycleTable;
        std::string segmentDirName = "segment_" + std::to_string(segId) + "_level_0/1";
        if (isRt) {
            segmentDirName = util::PathUtil::JoinPath("rt_index_partition", segmentDirName);
        }
        ASSERT_EQ(fileSystemLifecycle, lifecycleTable.GetLifecycle(segmentDirName));
    };
    auto addRtSegment = [&segDir, &partDirectory](segmentid_t segmentId,
                                                  std::map<std::string, std::pair<int64_t, int64_t>> stats) {
        VersionMaker::MakeRealtimeSegment(partDirectory, segmentId);
        auto rtSegId = RealtimeSegmentDirectory::ConvertToRtSegmentId(segmentId);
        auto stringStats = indexlibv2::framework::SegmentStatistics::StringStatsMapType();
        auto segmentStats = std::make_shared<indexlibv2::framework::SegmentStatistics>(rtSegId, stats, stringStats);
        segDir->AddSegment(rtSegId, INVALID_TIMESTAMP, segmentStats);
    };

    checkLifecycle(0, "hot", "hot");
    Version onDiskVersion2 = VersionMaker::Make(partDirectory, 1, "0,1", "", "");
    onDiskVersion2.AddSegmentStatistics({0, {{"eventtime", {10000, 10005}}}, {}});
    onDiskVersion2.AddSegmentStatistics({1, {{"eventtime", {10005, 10007}}}, {}});
    segDir->Reopen(onDiskVersion2);
    checkLifecycle(0, "hot", "hot");
    checkLifecycle(1, "cold", "cold");

    timeStamp = 10;
    auto rtSegId0 = RealtimeSegmentDirectory::ConvertToRtSegmentId(0);
    addRtSegment(0, {{"eventtime", {8, 10}}});
    checkLifecycle(rtSegId0, "hot", "hot", true);

    timeStamp = 12;
    auto rtSegId1 = RealtimeSegmentDirectory::ConvertToRtSegmentId(1);
    addRtSegment(1, {{"eventtime", {10, 12}}});
    checkLifecycle(rtSegId0, "warm", "warm", true);
    checkLifecycle(rtSegId1, "hot", "hot", true);

    timeStamp = 14;
    auto rtSegId2 = RealtimeSegmentDirectory::ConvertToRtSegmentId(2);
    addRtSegment(2, {{"eventtime", {12, 14}}});
    checkLifecycle(rtSegId0, "cold", "cold", true);
    checkLifecycle(rtSegId1, "warm", "warm", true);
    checkLifecycle(rtSegId2, "hot", "hot", true);
}

void OnlineSegmentDirectoryTest::TestFlushRtSegment()
{
    // todo: test after segment_info is flushed,  segmentData's lifecycle will be updated by LifecycleStrategy
}

}} // namespace indexlib::index_base
