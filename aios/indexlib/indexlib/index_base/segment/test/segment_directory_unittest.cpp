#include "indexlib/index_base/segment/test/segment_directory_unittest.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/test/mock_directory.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/index_meta/segment_file_meta.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentDirectoryTest);

SegmentDirectoryTest::SegmentDirectoryTest()
{
}

SegmentDirectoryTest::~SegmentDirectoryTest()
{
}

void SegmentDirectoryTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    Version version(0);
    version.Store(mRootDir, false);
    mDirectory.reset(new MockDirectory(mRootDir));
}

void SegmentDirectoryTest::CaseTearDown()
{
}

void SegmentDirectoryTest::TestExtractSegmentId()
{
    SegmentDirectory segDir;
    segDir.Init(GET_PARTITION_DIRECTORY());
    //test no segmentid
    ASSERT_EQ(INVALID_SEGMENTID, segDir.ExtractSegmentId(
                    PathUtil::GetParentDirPath(mRootDir) + "/segment_1"));
    ASSERT_EQ(INVALID_SEGMENTID, segDir.ExtractSegmentId(mRootDir));
    ASSERT_EQ(INVALID_SEGMENTID, segDir.ExtractSegmentId("abc/segment_1"));
    ASSERT_EQ(INVALID_SEGMENTID, segDir.ExtractSegmentId(
                    mRootDir + "/abc/segment_1"));

    ASSERT_EQ(INVALID_SEGMENTID, segDir.ExtractSegmentId(
                    mRootDir + "/abc/segment_1_segment_2"));

    ASSERT_EQ(INVALID_SEGMENTID, segDir.ExtractSegmentId(
                    string("//") + mRootDir + "//////segment_1"));

    //test has segmentid
    ASSERT_EQ(1, segDir.ExtractSegmentId(mRootDir + "segment_1"));
    ASSERT_EQ(1, segDir.ExtractSegmentId(mRootDir + "//////segment_1"));
    ASSERT_EQ(1, segDir.ExtractSegmentId(mRootDir + "/segment_1/index"));
}

void SegmentDirectoryTest::TestGetFsDirectoryForSub()
{
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 
            1, "0,2,4", "", "", 0, true);
    SegmentDirectory segDir;
    segDir.Init(GET_PARTITION_DIRECTORY(), INVALID_VERSION, true);
    
    SegmentDirectoryPtr subSegDir = segDir.GetSubSegmentDirectory();
    INDEXLIB_TEST_TRUE(subSegDir);

    SegmentFileMetaPtr segmentFileMeta(new SegmentFileMeta);
    DirectoryPtr dir = subSegDir->GetSegmentFsDirectory(2, segmentFileMeta);
    ASSERT_TRUE(segmentFileMeta);
    INDEXLIB_TEST_TRUE(dir);
    INDEXLIB_TEST_TRUE(dir->GetPath().find("sub_segment") != string::npos);
}

void SegmentDirectoryTest::TestInitSegmentDataForSub()
{
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 
            1, "0,2,4", "", "", 0, true);
    SegmentDirectory segDir;
    segDir.Init(GET_PARTITION_DIRECTORY(), INVALID_VERSION, true);
    
    SegmentDirectoryPtr subSegDir = segDir.GetSubSegmentDirectory();
    INDEXLIB_TEST_TRUE(subSegDir);
    INDEXLIB_TEST_TRUE(subSegDir->IsSubSegmentDirectory());

    SegmentDataVector segDatas = subSegDir->GetSegmentDatas();
    INDEXLIB_TEST_EQUAL((size_t)3, segDatas.size());
    string path = segDatas[0].GetDirectory()->GetPath();
    INDEXLIB_TEST_TRUE(path.find(SUB_SEGMENT_DIR_NAME) != string::npos);
}

void SegmentDirectoryTest::TestInitIndexFormatVersion()
{
    {
        // non-exist format version        
        SegmentDirectory segDir;
        segDir.Init(GET_PARTITION_DIRECTORY(), INVALID_VERSION, true);
        const IndexFormatVersion& formatVersion =
            segDir.GetIndexFormatVersion();
        IndexFormatVersion expectFormatVersion(INDEX_FORMAT_VERSION);
        ASSERT_TRUE(expectFormatVersion == formatVersion);
    }

    {
        // existed format version
        INIT_FORMAT_VERSION_FILE("1.8.0");

        SegmentDirectory segDir;
        segDir.Init(GET_PARTITION_DIRECTORY(), INVALID_VERSION, true);

        IndexFormatVersion expectFormatVersion("1.8.0");
        IndexFormatVersion loadFormatVersion = segDir.GetIndexFormatVersion();
        ASSERT_TRUE(expectFormatVersion == loadFormatVersion);

        SegmentDirectoryPtr clonedSegDir(segDir.Clone());    
        loadFormatVersion = clonedSegDir->GetIndexFormatVersion();
        ASSERT_TRUE(expectFormatVersion == loadFormatVersion);
    }
}

void SegmentDirectoryTest::TestClone()
{
    Version onDiskVersion = VersionMaker::Make(
            GET_PARTITION_DIRECTORY(), 1, "0,2,4", "", "", 0, true);

    SegmentDirectoryPtr segDir(new SegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), INVALID_VERSION, true);

    SegmentDirectoryPtr cloneSegDir(segDir->Clone());

    ASSERT_EQ(segDir->mVersion, cloneSegDir->mVersion);
    ASSERT_EQ(segDir->mOnDiskVersion, cloneSegDir->mOnDiskVersion);
    ASSERT_EQ(segDir->mRootDir, cloneSegDir->mRootDir);
    ASSERT_EQ(segDir->mIsSubSegDir, cloneSegDir->mIsSubSegDir);
    ASSERT_EQ(segDir->mSegmentDatas.size(), 
              cloneSegDir->mSegmentDatas.size());
    for (size_t i = 0; i < segDir->mSegmentDatas.size(); i++)
    {
        ASSERT_EQ(segDir->mSegmentDatas[i].GetSegmentInfo(),
                  cloneSegDir->mSegmentDatas[i].GetSegmentInfo());
        ASSERT_EQ(segDir->mSegmentDatas[i].GetBaseDocId(),
                  cloneSegDir->mSegmentDatas[i].GetBaseDocId());
        ASSERT_EQ(segDir->mSegmentDatas[i].GetSegmentId(),
                  cloneSegDir->mSegmentDatas[i].GetSegmentId());
        
    }

    ASSERT_TRUE(segDir->mRootDirectory.get() == cloneSegDir->mRootDirectory.get());
    ASSERT_TRUE(segDir->mIndexFormatVersion == cloneSegDir->mIndexFormatVersion);

    // sub SegmentDirectory will clone 
    ASSERT_TRUE(segDir->mSubSegmentDirectory.get() != 
                cloneSegDir->mSubSegmentDirectory.get());
}


SegmentDirectoryTest::ExistStatus
SegmentDirectoryTest::CheckVersionExists(const Version& version)
{
    Version currentVersion = version;
    fslib::FileList versionList;
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    VersionLoader::ListVersion(rootDir, versionList);
    bool currentVersionExist = false;
    for (size_t i = 0; i < versionList.size(); ++i)
    {
        Version version;
        version.Load(rootDir, versionList[i]);
        if (currentVersion.GetVersionId() == version.GetVersionId())
        {
            currentVersionExist = true;
        }
    }

    fslib::FileList segments;
    VersionLoader::ListSegments(rootDir, segments);
    size_t segCount = currentVersion.GetSegmentCount();
    for (size_t i = 0; i < segments.size(); ++i)
    {
        segmentid_t currentSegment =
            Version::GetSegmentIdByDirName(segments[i]);
        currentVersion.RemoveSegment(currentSegment);
    }
    if (currentVersion.GetSegmentCount() == 0 && currentVersionExist)
    {
        return FULL_EXISTS;
    }
    if (currentVersion.GetSegmentCount() == segCount && !currentVersionExist)
    {
        return NONE_EXISTS;
    }
    return SOME_EXISTS;
}

void SegmentDirectoryTest::TestRollbackToCurrentVersion()
{
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    Version version1 = VersionMaker::Make(
            rootDir, 1, "1,2", "", "", 0, true);
    Version version2 = VersionMaker::Make(
            rootDir, 2, "2,3,4", "", "", 100, true);
    Version version3 = VersionMaker::Make(
            GET_PARTITION_DIRECTORY(), 3, "4,6,7", "", "", 200, true);

    SegmentDirectoryPtr segDir(new SegmentDirectory);
    segDir->Init(rootDir, version2, true);

    ASSERT_EQ(FULL_EXISTS, CheckVersionExists(version3));
    segDir->RollbackToCurrentVersion();
    ASSERT_EQ(FULL_EXISTS, CheckVersionExists(version1));
    ASSERT_EQ(FULL_EXISTS, CheckVersionExists(version2));
    ASSERT_EQ(NONE_EXISTS, CheckVersionExists(version3 - version2));

    segDir->RollbackToCurrentVersion();
    ASSERT_EQ(FULL_EXISTS, CheckVersionExists(version1));
    ASSERT_EQ(FULL_EXISTS, CheckVersionExists(version2));
}

IE_NAMESPACE_END(index_base);

