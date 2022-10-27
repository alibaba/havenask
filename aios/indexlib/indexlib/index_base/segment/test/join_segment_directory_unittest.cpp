#include "indexlib/index_base/segment/test/join_segment_directory_unittest.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index_base/index_meta/segment_info.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, JoinSegmentDirectoryTest);

JoinSegmentDirectoryTest::JoinSegmentDirectoryTest()
{
}

JoinSegmentDirectoryTest::~JoinSegmentDirectoryTest()
{
}

void JoinSegmentDirectoryTest::CaseSetUp()
{
}

void JoinSegmentDirectoryTest::CaseTearDown()
{
}

void JoinSegmentDirectoryTest::TestAddSegment()
{
    SCOPED_TRACE("Failed");
    JoinSegmentDirectory segDir;
    segDir.Init(GET_PARTITION_DIRECTORY());
    segmentid_t segId0 = segDir.FormatSegmentId(0);
    DirectoryPtr dir0 = MAKE_SEGMENT_DIRECTORY(segId0);
    SegmentInfo segInfo0;
    segInfo0.Store(dir0);

    segDir.AddSegment(segId0, 0);
    ASSERT_EQ((size_t)1, segDir.GetVersion().GetSegmentCount());
    ASSERT_EQ(segDir.FormatSegmentId(0), segDir.GetVersion()[0]);

    segmentid_t segId1 = segDir.FormatSegmentId(1);
    DirectoryPtr dir1 = MAKE_SEGMENT_DIRECTORY(segId1);
    SegmentInfo segInfo1;
    segInfo1.Store(dir1);

    segDir.AddSegment(segId1, 1);
    ASSERT_EQ((size_t)1, segDir.GetVersion().GetSegmentCount());
    ASSERT_EQ(segDir.FormatSegmentId(1), segDir.GetVersion()[0]);
}

void JoinSegmentDirectoryTest::TestClone()
{
    Version onDiskVersion = VersionMaker::Make(
            GET_PARTITION_DIRECTORY(), 1, "0,2,4", "", "", 0, true);

    SegmentDirectoryPtr segDir(new JoinSegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), INVALID_VERSION, true);

    SegmentDirectoryPtr cloneSegDir(segDir->Clone());
    // sub SegmentDirectory will clone 
    ASSERT_TRUE(segDir->GetSubSegmentDirectory().get() != 
                cloneSegDir->GetSubSegmentDirectory().get());

    JoinSegmentDirectoryPtr mainSegDir = DYNAMIC_POINTER_CAST(
            JoinSegmentDirectory, cloneSegDir);
    ASSERT_TRUE(mainSegDir);

    JoinSegmentDirectoryPtr subSegDir = DYNAMIC_POINTER_CAST(
            JoinSegmentDirectory, cloneSegDir->GetSubSegmentDirectory());
    ASSERT_TRUE(subSegDir);
}

IE_NAMESPACE_END(index_base);

