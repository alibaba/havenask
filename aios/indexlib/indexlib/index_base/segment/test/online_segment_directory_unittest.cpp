#include "indexlib/index_base/segment/test/online_segment_directory_unittest.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/join_segment_directory.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, OnlineSegmentDirectoryTest);

OnlineSegmentDirectoryTest::OnlineSegmentDirectoryTest()
{
}

OnlineSegmentDirectoryTest::~OnlineSegmentDirectoryTest()
{
}

void OnlineSegmentDirectoryTest::CaseSetUp()
{
}

void OnlineSegmentDirectoryTest::CaseTearDown()
{
}

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
    OnlineSegmentDirectory segDir;
    segDir.Init(GET_PARTITION_DIRECTORY());
    
    Version version = VersionMaker::Make(GET_PARTITION_DIRECTORY(),
            0, "1", "3", "2");

    OnlineSegmentDirectory segDir2;
    segDir2.Init(GET_PARTITION_DIRECTORY());

    Version expect = VersionMaker::Make(0, "1", "3", "2");

    INDEXLIB_TEST_EQUAL(expect, segDir2.GetVersion());
}

void OnlineSegmentDirectoryTest::TestAddSegment()
{
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion = VersionMaker::Make(partDirectory, 
            0, "0,1", "", "");
    OnlineSegmentDirectory segDir;
    segDir.Init(partDirectory, onDiskVersion);

    VersionMaker::MakeRealtimeSegment(partDirectory, 0);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(0), 
                      INVALID_TIMESTAMP);
    ASSERT_EQ(VersionMaker::Make(0, "0,1", "0", ""), segDir.GetVersion());

    VersionMaker::MakeRealtimeSegment(partDirectory, 1);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(1),
                      INVALID_TIMESTAMP);
    ASSERT_EQ(VersionMaker::Make(0, "0,1", "0,1", ""), segDir.GetVersion());

    VersionMaker::MakeJoinSegment(partDirectory, 0);
    segDir.AddSegment(JoinSegmentDirectory::ConvertToJoinSegmentId(0),
                      INVALID_TIMESTAMP);
    ASSERT_EQ(VersionMaker::Make(0, "0,1", "0,1", "0"), segDir.GetVersion());

    VersionMaker::MakeJoinSegment(partDirectory, 1);
    segDir.AddSegment(JoinSegmentDirectory::ConvertToJoinSegmentId(1),
                      INVALID_TIMESTAMP);
    ASSERT_EQ(VersionMaker::Make(0, "0,1", "0,1", "1"), segDir.GetVersion());
}

void OnlineSegmentDirectoryTest::TestRemoveSegments()
{
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion = VersionMaker::Make(partDirectory, 0, "0,1", "", "");
    OnlineSegmentDirectory segDir;
    segDir.Init(partDirectory, onDiskVersion);

    VersionMaker::MakeRealtimeSegment(partDirectory, 0);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(0), 
                      INVALID_TIMESTAMP);
    VersionMaker::MakeRealtimeSegment(partDirectory, 1);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(1),
                      INVALID_TIMESTAMP);
    VersionMaker::MakeJoinSegment(partDirectory, 0);
    segDir.AddSegment(JoinSegmentDirectory::ConvertToJoinSegmentId(0),
                      INVALID_TIMESTAMP);
    VersionMaker::MakeJoinSegment(partDirectory, 1);
    segDir.AddSegment(JoinSegmentDirectory::ConvertToJoinSegmentId(1),
                      INVALID_TIMESTAMP);

    vector<segmentid_t> segIds;
    segIds.push_back(RealtimeSegmentDirectory::ConvertToRtSegmentId(0));
    segIds.push_back(JoinSegmentDirectory::ConvertToJoinSegmentId(1));
    segDir.RemoveSegments(segIds);
    ASSERT_EQ(VersionMaker::Make(0, "0,1", "1", ""), segDir.GetVersion());
}

void OnlineSegmentDirectoryTest::TestRefreshVersion()
{
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion  = VersionMaker::Make(rootDirectory,
            0, "0,1", "", "");
    OnlineSegmentDirectory segDir;
    segDir.Init(GET_PARTITION_DIRECTORY(), onDiskVersion);

    Version rtVersion = VersionMaker::Make(rootDirectory,
            0, "", "10,11", "");
    RealtimeSegmentDirectoryPtr rtSegDir(new RealtimeSegmentDirectory);
    rtSegDir->Init(segDir.mRtSegDir->GetRootDirectory(), rtVersion);

    Version joinVersion = VersionMaker::Make(rootDirectory,
            0, "", "", "10");
    JoinSegmentDirectoryPtr joinSegDir(new JoinSegmentDirectory);
    joinSegDir->Init(segDir.mJoinSegDir->GetRootDirectory(), joinVersion);

    segDir.mRtSegDir = rtSegDir;
    segDir.mJoinSegDir = joinSegDir;

    segDir.RefreshVersion();

    Version expectVersion = VersionMaker::Make(0, "0,1", "10,11", "10");
    ASSERT_EQ(expectVersion, segDir.GetVersion());

    Version joinVersion2 = VersionMaker::Make(rootDirectory,
            1, "", "", "20");
    joinSegDir->Init(segDir.mJoinSegDir->GetRootDirectory(), joinVersion2);

    segDir.RefreshVersion();

    Version expectVersion2 = VersionMaker::Make(0, "0,1", "10,11", "20");
    ASSERT_EQ(expectVersion2, segDir.GetVersion());
}

void OnlineSegmentDirectoryTest::TestInitSub()
{
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion  = VersionMaker::Make(partDirectory,
            0, "0,1", "3", "2", 0, true);
    OnlineSegmentDirectory segDir;
    segDir.Init(partDirectory, INVALID_VERSION, true);

    SegmentDirectoryPtr subSegDir = segDir.GetSubSegmentDirectory();
    ASSERT_TRUE(subSegDir);

    OnlineSegmentDirectoryPtr subOnlineSegDir = DYNAMIC_POINTER_CAST(
            OnlineSegmentDirectory, subSegDir);
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
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    OnlineSegmentDirectory segDir;
    segDir.Init(partDirectory, INVALID_VERSION, true);

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
    TearDown();
    SetUp();
    
    RESET_FILE_SYSTEM(LoadConfigList(), false, true, true, flushRt);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    Version onDiskVersion = VersionMaker::Make(partDirectory, 
            0, "0,1", "", "");
    OnlineSegmentDirectory segDir;
    segDir.Init(partDirectory, onDiskVersion);

    VersionMaker::MakeRealtimeSegment(partDirectory, 0);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(0), 
                      INVALID_TIMESTAMP);

    VersionMaker::MakeRealtimeSegment(partDirectory, 1);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(1),
                      INVALID_TIMESTAMP);

    VersionMaker::MakeRealtimeSegment(partDirectory, 2);
    segDir.AddSegment(RealtimeSegmentDirectory::ConvertToRtSegmentId(2),
                      INVALID_TIMESTAMP);
    
    DirectoryPtr rtDirectory = partDirectory->GetDirectory(
            RT_INDEX_PARTITION_DIR_NAME, true);
    rtDirectory->Sync(true);
    stringstream ss;
    ss << "segment_" << RealtimeSegmentDirectory::ConvertToRtSegmentId(2) << "_level_0/segment_info";
    rtDirectory->RemoveFile(ss.str());

    segmentid_t lastLinkRtSegId = INVALID_SEGMENTID;
    if (!flushRt)
    {
        lastLinkRtSegId = segDir.GetLastValidRtSegmentInLinkDirectory();
        ASSERT_EQ(INVALID_SEGMENTID, lastLinkRtSegId);
        ASSERT_FALSE(segDir.SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId));
    }
    else
    {
        lastLinkRtSegId = segDir.GetLastValidRtSegmentInLinkDirectory();
        ASSERT_EQ(RealtimeSegmentDirectory::ConvertToRtSegmentId(1), lastLinkRtSegId);
        ASSERT_TRUE(segDir.SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId));
    }
}

IE_NAMESPACE_END(index_base);

