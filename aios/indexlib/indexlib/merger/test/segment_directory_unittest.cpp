#include "indexlib/merger/test/segment_directory_unittest.h"
#include "indexlib/merger/test/segment_directory_creator.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/util/path_util.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);

void SegmentDirectoryTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void SegmentDirectoryTest::CaseTearDown()
{
}

void SegmentDirectoryTest::TestCaseForCreateSubSegmentDir()
{
    string str = "{\"segments\":[1],\"versionid\":10}";
    Version version;

    version.FromString(str);

    SegmentDirectory segmentDirectory(mRootDir, version);
    SegmentDirectoryPtr segmentDir = segmentDirectory.CreateSubSegDir(false);
    INDEXLIB_TEST_EQUAL(segmentDir->GetSegmentPath(1), PathUtil::JoinPath(mRootDir,
                    version.GetSegmentDirName(1) + "/sub_segment/"));
}

void SegmentDirectoryTest::TestCaseForIterator()
{
    string str = "{\"segments\":[1,3,12],\"versionid\":10}";
    Version version;

    version.FromString(str);
        
    SegmentDirectory segmentDirectory(mRootDir, version);
    SegmentDirectory::Iterator iter = segmentDirectory.CreateIterator();
    string path;
    path = iter.Next();
    INDEXLIB_TEST_EQUAL(path, mRootDir + version.GetSegmentDirName(1) + "/");
    path = iter.Next();
    INDEXLIB_TEST_EQUAL(path, mRootDir + version.GetSegmentDirName(3) + "/");
    path = iter.Next();
    INDEXLIB_TEST_EQUAL(path, mRootDir + version.GetSegmentDirName(12) + "/");
    INDEXLIB_TEST_TRUE(!iter.HasNext());        
}

void SegmentDirectoryTest::TestGetBaseDocIds()
{
    Version version = SegmentDirectoryCreator::MakeVersion(
            "1:-1:2,3,10", mRootDir);
    SegmentDirectoryPtr segDir(new SegmentDirectory(mRootDir, version));
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
    Version version = SegmentDirectoryCreator::MakeVersion("2:100:", mRootDir);
    SegmentDirectoryPtr segDir(new SegmentDirectory(mRootDir, version));
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
    SegmentDirectory segDir;
    ASSERT_FALSE(segDir.LoadSegmentInfo(mRootDir, segInfo));

    onDiskSegInfo.Store(mRootDir + SEGMENT_INFO_FILE_NAME);
    ASSERT_TRUE(segDir.LoadSegmentInfo(mRootDir, segInfo));
    ASSERT_EQ(onDiskSegInfo, segInfo);
}

SegmentInfo SegmentDirectoryTest::MakeSegmentInfo(
        uint32_t docCount, int64_t timestamp)
{
    SegmentInfo segInfo;
    segInfo.docCount = docCount;
    segInfo.timestamp = timestamp;

    return segInfo;
}

void SegmentDirectoryTest::TestRemoveUselessSegment()
{
    Version version = SegmentDirectoryCreator::MakeVersion("2:100:2", mRootDir);
    version.Store(mRootDir, false);
    FileSystemWrapper::MkDir(mRootDir + "segment_0");
    FileSystemWrapper::MkDir(mRootDir + "segment_3");
    FileSystemWrapper::MkDir(mRootDir + "ttt");
    SegmentDirectory::RemoveUselessSegment(mRootDir);
    ASSERT_TRUE(FileSystemWrapper::IsExist(mRootDir + "segment_0"));
    ASSERT_FALSE(FileSystemWrapper::IsExist(mRootDir + "segment_3"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(mRootDir + "ttt"));
}

IE_NAMESPACE_END(merger);
