#include "indexlib/index_base/segment/test/segment_directory_unittest.h"

#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/test/version_maker.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SegmentDirectoryTest);

SegmentDirectoryTest::SegmentDirectoryTest() {}

SegmentDirectoryTest::~SegmentDirectoryTest() {}

void SegmentDirectoryTest::CaseSetUp()
{
    Version version(0);
    version.Store(GET_PARTITION_DIRECTORY(), true);
}

void SegmentDirectoryTest::CaseTearDown() {}

void SegmentDirectoryTest::TestGetFsDirectoryForSub()
{
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 1, "0,2,4", "", "", 0, true);
    SegmentDirectory segDir;
    segDir.Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION), true);

    SegmentDirectoryPtr subSegDir = segDir.GetSubSegmentDirectory();
    INDEXLIB_TEST_TRUE(subSegDir);

    DirectoryPtr dir = subSegDir->GetSegmentFsDirectory(2);
    INDEXLIB_TEST_TRUE(dir);
    INDEXLIB_TEST_TRUE(dir->GetLogicalPath().find("sub_segment") != string::npos);
}

void SegmentDirectoryTest::TestInitSegmentDataForSub()
{
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 1, "0,2,4", "", "", 0, true);
    SegmentDirectory segDir;
    segDir.Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION), true);

    SegmentDirectoryPtr subSegDir = segDir.GetSubSegmentDirectory();
    INDEXLIB_TEST_TRUE(subSegDir);
    INDEXLIB_TEST_TRUE(subSegDir->IsSubSegmentDirectory());

    SegmentDataVector segDatas = subSegDir->GetSegmentDatas();
    INDEXLIB_TEST_EQUAL((size_t)3, segDatas.size());
    string path = segDatas[0].GetDirectory()->GetLogicalPath();
    INDEXLIB_TEST_TRUE(path.find(SUB_SEGMENT_DIR_NAME) != string::npos);
}

void SegmentDirectoryTest::TestInitIndexFormatVersion()
{
    {
        // non-exist format version
        SegmentDirectory segDir;
        segDir.Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION), true);
        const IndexFormatVersion& formatVersion = segDir.GetIndexFormatVersion();
        IndexFormatVersion expectFormatVersion(INDEX_FORMAT_VERSION);
        ASSERT_TRUE(expectFormatVersion == formatVersion);
    }

    {
        // existed format version
        indexlib::file_system::DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
        assert(rootDirectory);
        if (rootDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME)) {
            rootDirectory->RemoveFile(INDEX_FORMAT_VERSION_FILE_NAME);
        }

        indexlib::index_base::IndexFormatVersion indexFormatVersion("1.8.0");
        indexFormatVersion.Store(rootDirectory);

        SegmentDirectory segDir;
        segDir.Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION), true);

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
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 1, "0,2,4", "", "", 0, true);

    SegmentDirectoryPtr segDir(new SegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION), true);

    auto dpDesc = std::make_shared<indexlibv2::framework::VersionDeployDescription>();
    dpDesc->rawPath = "/rawPath";
    dpDesc->remotePath = "/remotePath";
    dpDesc->configPath = "/configPath";
    segDir->SetVersionDeployDescription(dpDesc);

    SegmentDirectoryPtr cloneSegDir(segDir->Clone());

    ASSERT_EQ(segDir->mVersion, cloneSegDir->mVersion);
    ASSERT_EQ(segDir->mOnDiskVersion, cloneSegDir->mOnDiskVersion);
    ASSERT_EQ(segDir->mRootDirectory, cloneSegDir->mRootDirectory);
    ASSERT_EQ(segDir->mIsSubSegDir, cloneSegDir->mIsSubSegDir);
    ASSERT_EQ(segDir->mSegmentDatas.size(), cloneSegDir->mSegmentDatas.size());
    ASSERT_EQ(dpDesc.get(), cloneSegDir->GetVersionDeployDescription().get());

    for (size_t i = 0; i < segDir->mSegmentDatas.size(); i++) {
        ASSERT_EQ(*segDir->mSegmentDatas[i].GetSegmentInfo(), *cloneSegDir->mSegmentDatas[i].GetSegmentInfo());
        ASSERT_EQ(segDir->mSegmentDatas[i].GetBaseDocId(), cloneSegDir->mSegmentDatas[i].GetBaseDocId());
        ASSERT_EQ(segDir->mSegmentDatas[i].GetSegmentId(), cloneSegDir->mSegmentDatas[i].GetSegmentId());
    }

    ASSERT_TRUE(segDir->mRootDirectory.get() == cloneSegDir->mRootDirectory.get());
    ASSERT_TRUE(segDir->mIndexFormatVersion == cloneSegDir->mIndexFormatVersion);

    // sub SegmentDirectory will clone
    ASSERT_TRUE(segDir->mSubSegmentDirectory.get() != cloneSegDir->mSubSegmentDirectory.get());
}

SegmentDirectoryTest::ExistStatus SegmentDirectoryTest::CheckVersionExists(const Version& version)
{
    Version currentVersion = version;
    fslib::FileList versionList;
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    VersionLoader::ListVersion(rootDir, versionList);
    bool currentVersionExist = false;
    for (size_t i = 0; i < versionList.size(); ++i) {
        Version version;
        version.Load(rootDir, versionList[i]);
        if (currentVersion.GetVersionId() == version.GetVersionId()) {
            currentVersionExist = true;
        }
    }

    fslib::FileList segments;
    VersionLoader::ListSegments(rootDir, segments);
    size_t segCount = currentVersion.GetSegmentCount();
    for (size_t i = 0; i < segments.size(); ++i) {
        segmentid_t currentSegment = Version::GetSegmentIdByDirName(segments[i]);
        currentVersion.RemoveSegment(currentSegment);
    }
    if (currentVersion.GetSegmentCount() == 0 && currentVersionExist) {
        return FULL_EXISTS;
    }
    if (currentVersion.GetSegmentCount() == segCount && !currentVersionExist) {
        return NONE_EXISTS;
    }
    return SOME_EXISTS;
}

void SegmentDirectoryTest::TestRollbackToCurrentVersion()
{
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    auto resetFSWithoutAutoMount = [&]() {
        RESET_FILE_SYSTEM("ut", false /* auto mount */);
        rootDir = GET_PARTITION_DIRECTORY();
        ASSERT_EQ(FSEC_OK,
                  rootDir->GetFileSystem()->MountDir(GET_TEMP_DATA_PATH(), "", "", file_system::FSMT_READ_WRITE, true));
    };

    Version version1 = VersionMaker::Make(rootDir, 1, "1,2", "", "", 0, true);
    resetFSWithoutAutoMount();
    Version version2 = VersionMaker::Make(rootDir, 2, "2,3,4", "", "", 100, true);
    resetFSWithoutAutoMount();
    Version version3 = VersionMaker::Make(rootDir, 3, "4,6,7", "", "", 200, true);

    resetFSWithoutAutoMount();
    SegmentDirectoryPtr segDir(new SegmentDirectory);
    segDir->Init(rootDir, version2, true);

    ASSERT_EQ(FULL_EXISTS, CheckVersionExists(version3));
    segDir->RollbackToCurrentVersion();
    ASSERT_EQ(FULL_EXISTS, CheckVersionExists(version1));
    ASSERT_EQ(FULL_EXISTS, CheckVersionExists(version2));
    ASSERT_EQ(NONE_EXISTS, CheckVersionExists(version3 - version2));

    resetFSWithoutAutoMount();
    segDir->RollbackToCurrentVersion();
    ASSERT_EQ(FULL_EXISTS, CheckVersionExists(version1));
    ASSERT_EQ(FULL_EXISTS, CheckVersionExists(version2));
}
}} // namespace indexlib::index_base
