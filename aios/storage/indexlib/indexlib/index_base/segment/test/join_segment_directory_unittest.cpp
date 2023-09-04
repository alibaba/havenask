#include "indexlib/index_base/segment/test/join_segment_directory_unittest.h"

#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/test/version_maker.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, JoinSegmentDirectoryTest);

JoinSegmentDirectoryTest::JoinSegmentDirectoryTest() {}

JoinSegmentDirectoryTest::~JoinSegmentDirectoryTest() {}

void JoinSegmentDirectoryTest::CaseSetUp() {}

void JoinSegmentDirectoryTest::CaseTearDown() {}

void JoinSegmentDirectoryTest::TestAddSegment()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);

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
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);

    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 1, "0,2,4", "", "", 0, true);

    SegmentDirectoryPtr segDir(new JoinSegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION), true);

    SegmentDirectoryPtr cloneSegDir(segDir->Clone());
    // sub SegmentDirectory will clone
    ASSERT_TRUE(segDir->GetSubSegmentDirectory().get() != cloneSegDir->GetSubSegmentDirectory().get());

    JoinSegmentDirectoryPtr mainSegDir = DYNAMIC_POINTER_CAST(JoinSegmentDirectory, cloneSegDir);
    ASSERT_TRUE(mainSegDir);

    JoinSegmentDirectoryPtr subSegDir =
        DYNAMIC_POINTER_CAST(JoinSegmentDirectory, cloneSegDir->GetSubSegmentDirectory());
    ASSERT_TRUE(subSegDir);
}
}} // namespace indexlib::index_base
