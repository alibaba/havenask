#include "indexlib/index_base/segment/test/multi_part_segment_directory_unittest.h"

#include "indexlib/test/version_maker.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, MultiPartSegmentDirectoryTest);

MultiPartSegmentDirectoryTest::MultiPartSegmentDirectoryTest() {}

MultiPartSegmentDirectoryTest::~MultiPartSegmentDirectoryTest() {}

void MultiPartSegmentDirectoryTest::CaseSetUp() {}

void MultiPartSegmentDirectoryTest::CaseTearDown() {}

void MultiPartSegmentDirectoryTest::TestSimpleProcess()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
    DirectoryPtr caseRoot = GET_PARTITION_DIRECTORY();
    DirectoryPtr partRoot1 = caseRoot->MakeDirectory("test1");
    DirectoryPtr partRoot2 = caseRoot->MakeDirectory("test2");

    Version v1 = VersionMaker::Make(partRoot1, 0, "1,2");
    Version v2 = VersionMaker::Make(partRoot2, 1, "1,2");

    DirectoryVector dirs;
    dirs.push_back(partRoot1);
    dirs.push_back(partRoot2);

    MultiPartSegmentDirectory segDir;
    segDir.Init(dirs);

    DirectoryPtr segDir1 = segDir.GetSegmentFsDirectory(0);
    ASSERT_TRUE(segDir1);
    ASSERT_EQ("test1/segment_1_level_0", segDir1->GetLogicalPath());

    DirectoryPtr segDir3 = segDir.GetSegmentFsDirectory(3);
    ASSERT_TRUE(segDir3);
    ASSERT_EQ("test2/segment_2_level_0", segDir3->GetLogicalPath());
}

void MultiPartSegmentDirectoryTest::TestEncodeToVirtualSegmentId()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
    DirectoryPtr caseRoot = GET_PARTITION_DIRECTORY();
    DirectoryPtr partRoot1 = caseRoot->MakeDirectory("test1");
    DirectoryPtr partRoot2 = caseRoot->MakeDirectory("test2");

    Version v1 = VersionMaker::Make(partRoot1, 0, "1,2,4");
    Version v2 = VersionMaker::Make(partRoot2, 1, "2,3,5");

    DirectoryVector dirs;
    dirs.push_back(partRoot1);
    dirs.push_back(partRoot2);

    MultiPartSegmentDirectory segDir;
    segDir.Init(dirs);

    segmentid_t encodedSegId = segDir.EncodeToVirtualSegmentId(0, 3);
    ASSERT_EQ(INVALID_SEGMENTID, encodedSegId);
    encodedSegId = segDir.EncodeToVirtualSegmentId(0, 2);
    ASSERT_EQ((segmentid_t)1, encodedSegId);
    encodedSegId = segDir.EncodeToVirtualSegmentId(0, 5);
    ASSERT_EQ(INVALID_SEGMENTID, encodedSegId);
    encodedSegId = segDir.EncodeToVirtualSegmentId(1, 1);
    ASSERT_EQ(INVALID_SEGMENTID, encodedSegId);
    encodedSegId = segDir.EncodeToVirtualSegmentId(1, 2);
    ASSERT_EQ((segmentid_t)3, encodedSegId);
    encodedSegId = segDir.EncodeToVirtualSegmentId(1, 5);
    ASSERT_EQ((segmentid_t)5, encodedSegId);
    encodedSegId = segDir.EncodeToVirtualSegmentId(1, 6);
    ASSERT_EQ(INVALID_SEGMENTID, encodedSegId);
    encodedSegId = segDir.EncodeToVirtualSegmentId(2, 0);
    ASSERT_EQ(INVALID_SEGMENTID, encodedSegId);
}
}} // namespace indexlib::index_base
