#include "indexlib/merger/test/segment_directory_unittest.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/test/segment_directory_creator.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::util;

using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {

void SegmentDirectoryTest::CaseSetUp() { mRootDir = GET_TEMP_DATA_PATH(); }

void SegmentDirectoryTest::CaseTearDown() {}

void SegmentDirectoryTest::TestCaseForCreateSubSegmentDir()
{
    string str = "{\"segments\":[1],\"versionid\":10}";
    Version version;
    version.FromString(str);

    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1");
    SegmentDirectory segmentDirectory(GET_PARTITION_DIRECTORY(), version);
    SegmentDirectoryPtr segmentDir = segmentDirectory.CreateSubSegDir(false);
    INDEXLIB_TEST_EQUAL(segmentDir->GetSegmentDirectory(1)->GetLogicalPath(),
                        version.GetSegmentDirName(1) + "/sub_segment");
}

void SegmentDirectoryTest::TestCaseForIterator()
{
    string str = "{\"segments\":[1,3,12],\"versionid\":10}";
    Version version;

    version.FromString(str);

    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1");
    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_3");
    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_12");
    SegmentDirectory segmentDirectory(GET_PARTITION_DIRECTORY(), version);
    SegmentDirectory::Iterator iter = segmentDirectory.CreateIterator();
    auto path = iter.Next();
    auto segDir1 = GET_PARTITION_DIRECTORY()->GetDirectory(version.GetSegmentDirName(1), false);
    ASSERT_NE(segDir1, nullptr);
    INDEXLIB_TEST_EQUAL(path->GetLogicalPath(), segDir1->GetLogicalPath());
    path = iter.Next();
    segDir1 = GET_PARTITION_DIRECTORY()->GetDirectory(version.GetSegmentDirName(3), false);
    ASSERT_NE(segDir1, nullptr);
    INDEXLIB_TEST_EQUAL(path->GetLogicalPath(), version.GetSegmentDirName(3));
    path = iter.Next();
    segDir1 = GET_PARTITION_DIRECTORY()->GetDirectory(version.GetSegmentDirName(12), false);
    ASSERT_NE(segDir1, nullptr);
    INDEXLIB_TEST_EQUAL(path->GetLogicalPath(), version.GetSegmentDirName(12));
    INDEXLIB_TEST_TRUE(!iter.HasNext());
}

void SegmentDirectoryTest::TestGetBaseDocIds()
{
    Version version = SegmentDirectoryCreator::MakeVersion("1:-1:2,3,10", GET_PARTITION_DIRECTORY());
    SegmentDirectoryPtr segDir(new SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    vector<exdocid_t> baseDocIds;
    segDir->GetBaseDocIds(baseDocIds);

    INDEXLIB_TEST_EQUAL((size_t)3, baseDocIds.size());
    INDEXLIB_TEST_EQUAL((exdocid_t)0, baseDocIds[0]);
    INDEXLIB_TEST_EQUAL((exdocid_t)2, baseDocIds[1]);
    INDEXLIB_TEST_EQUAL((exdocid_t)5, baseDocIds[2]);
    INDEXLIB_TEST_EQUAL((uint64_t)15, segDir->GetTotalDocCount());

    SegmentInfo segInfo;
    INDEXLIB_TEST_TRUE(segDir->GetSegmentInfo(2, segInfo));
    INDEXLIB_TEST_TRUE(segDir->GetSegmentInfo(3, segInfo));
    INDEXLIB_TEST_TRUE(segDir->GetSegmentInfo(10, segInfo));
}

void SegmentDirectoryTest::TestGetBaseDocIdsForEmpty()
{
    Version version = SegmentDirectoryCreator::MakeVersion("2:100:", GET_PARTITION_DIRECTORY());
    SegmentDirectoryPtr segDir(new SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    vector<exdocid_t> baseDocIds;
    segDir->GetBaseDocIds(baseDocIds);

    SegmentInfo segInfo;
    INDEXLIB_TEST_EQUAL((size_t)0, baseDocIds.size());
    INDEXLIB_TEST_TRUE(!segDir->GetSegmentInfo(2, segInfo));
}

void SegmentDirectoryTest::TestLoadSegmentInfo()
{
    SegmentInfo inMemSegInfo = MakeSegmentInfo(100, 50);
    SegmentInfo onDiskSegInfo = MakeSegmentInfo(100, 200);

    SegmentInfo segInfo;
    SegmentDirectory segDir(GET_PARTITION_DIRECTORY(), index_base::Version(0));
    ASSERT_FALSE(segDir.LoadSegmentInfo(GET_PARTITION_DIRECTORY(), segInfo));

    onDiskSegInfo.Store(GET_PARTITION_DIRECTORY());
    ASSERT_TRUE(segDir.LoadSegmentInfo(GET_PARTITION_DIRECTORY(), segInfo));
    ASSERT_EQ(onDiskSegInfo, segInfo);
}

SegmentInfo SegmentDirectoryTest::MakeSegmentInfo(uint32_t docCount, int64_t timestamp)
{
    SegmentInfo segInfo;
    segInfo.docCount = docCount;
    segInfo.timestamp = timestamp;

    return segInfo;
}

void SegmentDirectoryTest::TestRemoveUselessSegment()
{
    Version version = SegmentDirectoryCreator::MakeVersion("2:100:2", GET_PARTITION_DIRECTORY());
    GET_PARTITION_DIRECTORY()->Store(version.GetVersionFileName(), version.ToString(), WriterOption());
    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0");
    GET_PARTITION_DIRECTORY()->MakeDirectory("segment_3");
    GET_PARTITION_DIRECTORY()->MakeDirectory("ttt");
    SegmentDirectory::RemoveUselessSegment(GET_PARTITION_DIRECTORY());
    ASSERT_TRUE(FslibWrapper::IsExist(GET_TEMP_DATA_PATH("segment_0")).GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(GET_TEMP_DATA_PATH("segment_3")).GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(GET_TEMP_DATA_PATH(+"ttt")).GetOrThrow());
}
}} // namespace indexlib::merger
