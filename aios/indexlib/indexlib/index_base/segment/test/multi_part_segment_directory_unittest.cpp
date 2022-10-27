#include "indexlib/index_base/segment/test/multi_part_segment_directory_unittest.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index_base/index_meta/segment_file_meta.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, MultiPartSegmentDirectoryTest);

MultiPartSegmentDirectoryTest::MultiPartSegmentDirectoryTest()
{
}

MultiPartSegmentDirectoryTest::~MultiPartSegmentDirectoryTest()
{
}

void MultiPartSegmentDirectoryTest::CaseSetUp()
{
}

void MultiPartSegmentDirectoryTest::CaseTearDown()
{
}

void MultiPartSegmentDirectoryTest::TestSimpleProcess()
{
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

    SegmentFileMetaPtr segmentFileMeta(new SegmentFileMeta);
    DirectoryPtr segDir1 = segDir.GetSegmentFsDirectory(0, segmentFileMeta);
    ASSERT_TRUE(segmentFileMeta);
    ASSERT_TRUE(segDir1);
    ASSERT_EQ(PathUtil::JoinPath(GET_TEST_DATA_PATH(), "test1/segment_1_level_0"), 
              segDir1->GetPath());

    segmentFileMeta.reset(new SegmentFileMeta);
    DirectoryPtr segDir3 = segDir.GetSegmentFsDirectory(3, segmentFileMeta);
    ASSERT_TRUE(segmentFileMeta);
    ASSERT_TRUE(segDir3);
    ASSERT_EQ(PathUtil::JoinPath(GET_TEST_DATA_PATH(), "test2/segment_2_level_0"), 
              segDir3->GetPath());
}

void MultiPartSegmentDirectoryTest::TestEncodeToVirtualSegmentId()
{
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

IE_NAMESPACE_END(index_base);

