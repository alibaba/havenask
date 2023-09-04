#include "indexlib/index_base/segment/test/parallel_offline_segment_directory_unittest.h"

#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, ParallelOfflineSegmentDirectoryTest);

ParallelOfflineSegmentDirectoryTest::ParallelOfflineSegmentDirectoryTest() {}

ParallelOfflineSegmentDirectoryTest::~ParallelOfflineSegmentDirectoryTest() {}

void ParallelOfflineSegmentDirectoryTest::CaseSetUp() {}

void ParallelOfflineSegmentDirectoryTest::CaseTearDown() {}

void ParallelOfflineSegmentDirectoryTest::CreateEmptyFile(const std::string& dir)
{
    IndexFormatVersion indexFormatVersion;
    indexFormatVersion.Set("1.8.4");
    indexFormatVersion.Store(PathUtil::JoinPath(dir, "index_format_version"), FenceContext::NoFence());

    std::string emptyData("");
    auto ret = FslibWrapper::AtomicStore(PathUtil::JoinPath(dir, "schema.json"), emptyData).Code();
    (void)ret;
}

void ParallelOfflineSegmentDirectoryTest::TestSimpleProcess()
{
    string secondaryPath = GET_TEMP_DATA_PATH() + "/partition_path";
    FslibWrapper::MkDirE(secondaryPath);
    ParallelBuildInfo parallelInfo(2, 0, 1);
    parallelInfo.SetBaseVersion(0);
    Version onDiskVersion = PrepareRootDir(secondaryPath, "0,1");

    CreateEmptyFile(secondaryPath);
    ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(secondaryPath, onDiskVersion.GetVersionId(), "", FSMT_READ_ONLY,
                                                       nullptr));
    parallelInfo.StoreIfNotExist(GET_PARTITION_DIRECTORY());

    // check new segment id
    ParallelOfflineSegmentDirectoryPtr segDir(new ParallelOfflineSegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), onDiskVersion, true);

    ASSERT_EQ((segmentid_t)3, segDir->CreateNewSegmentId());
    segDir->IncLastSegmentId();
    ASSERT_EQ((segmentid_t)5, segDir->CreateNewSegmentId());

    SegmentDirectoryPtr subDir = segDir->GetSubSegmentDirectory();
    ASSERT_EQ((segmentid_t)3, subDir->CreateNewSegmentId());
    subDir->IncLastSegmentId();
    ASSERT_EQ((segmentid_t)5, subDir->CreateNewSegmentId());
}

void ParallelOfflineSegmentDirectoryTest::TestRecover()
{
    ParallelBuildInfo parallelInfo(2, 0, 1);
    parallelInfo.SetBaseVersion(0);
    parallelInfo.StoreIfNotExist(GET_PARTITION_DIRECTORY());

    string secondaryPath = GET_TEMP_DATA_PATH() + "/partition_path";
    FslibWrapper::MkDirE(secondaryPath);
    Version onDiskVersion = PrepareRootDir(secondaryPath, "0,1");
    DirectoryPtr partitionRootDir = test::DirectoryCreator::Create(secondaryPath, true);
    VersionMaker::MakeIncSegment(partitionRootDir, 3, true);
    ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountDir(secondaryPath, "", "", FSMT_READ_ONLY, true));

    VersionMaker::MakeIncSegment(GET_PARTITION_DIRECTORY(), 4, true);
    VersionMaker::MakeIncSegment(GET_PARTITION_DIRECTORY(), 5, true);
    GET_PARTITION_DIRECTORY()->RemoveFile("segment_5_level_0/segment_info");
    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist("segment_5_level_0"));

    ParallelOfflineSegmentDirectoryPtr segDir(new ParallelOfflineSegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), onDiskVersion, true);

    Version version = segDir->GetVersion();
    // check partition path not recover ps: why?? by feishi.wzj
    // ASSERT_FALSE(version.HasSegment(3));

    // check instance path recover
    ASSERT_TRUE(version.HasSegment(4));
    ASSERT_FALSE(version.HasSegment(5));
    ASSERT_FALSE(GET_PARTITION_DIRECTORY()->IsExist("segment_5_level_0"));

    ASSERT_EQ((segmentid_t)4, version.GetLastSegment());
    ASSERT_EQ((segmentid_t)6, segDir->CreateNewSegmentId());
    segDir->IncLastSegmentId();
    ASSERT_EQ((segmentid_t)8, segDir->CreateNewSegmentId());

    SegmentDirectoryPtr subDir = segDir->GetSubSegmentDirectory();
    version = subDir->GetVersion();
    ASSERT_EQ((segmentid_t)4, version.GetLastSegment());
    ASSERT_EQ((segmentid_t)6, subDir->CreateNewSegmentId());
    subDir->IncLastSegmentId();
    ASSERT_EQ((segmentid_t)8, subDir->CreateNewSegmentId());
}

void ParallelOfflineSegmentDirectoryTest::TestParallelIncBuildFromEmptyFullIndex()
{
    string secondaryPath = GET_TEMP_DATA_PATH() + "/partition_path";
    FslibWrapper::MkDirE(secondaryPath);
    ParallelBuildInfo parallelInfo(2, 0, 1);
    parallelInfo.SetBaseVersion(0);
    Version onDiskVersion = PrepareRootDir(secondaryPath, "");
    parallelInfo.StoreIfNotExist(GET_PARTITION_DIRECTORY());

    // check new segment id
    ParallelOfflineSegmentDirectoryPtr segDir(new ParallelOfflineSegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), onDiskVersion, true);

    ASSERT_EQ((segmentid_t)1, segDir->CreateNewSegmentId());
    segDir->IncLastSegmentId();
    ASSERT_EQ((segmentid_t)3, segDir->CreateNewSegmentId());

    SegmentDirectoryPtr subDir = segDir->GetSubSegmentDirectory();
    ASSERT_EQ((segmentid_t)1, subDir->CreateNewSegmentId());
    subDir->IncLastSegmentId();
    ASSERT_EQ((segmentid_t)3, subDir->CreateNewSegmentId());
}

void ParallelOfflineSegmentDirectoryTest::TestCreateNewSegmentAfterRecover()
{
    string secondaryPath = GET_TEMP_DATA_PATH() + "/partition_path";
    FslibWrapper::MkDirE(secondaryPath);
    ParallelBuildInfo parallelInfo(2, 0, 1);
    parallelInfo.SetBaseVersion(0);
    Version onDiskVersion = PrepareRootDir(secondaryPath, "");
    parallelInfo.StoreIfNotExist(GET_PARTITION_DIRECTORY());

    // check new segment id
    ParallelOfflineSegmentDirectoryPtr segDir(new ParallelOfflineSegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), onDiskVersion, true);
    ASSERT_EQ((segmentid_t)1, segDir->CreateNewSegmentId());
    segDir->IncLastSegmentId();
    ASSERT_EQ((segmentid_t)3, segDir->CreateNewSegmentId());

    // create instance version
    VersionMaker::MakeIncSegment(GET_PARTITION_DIRECTORY(), 1, true);
    VersionMaker::MakeIncSegment(GET_PARTITION_DIRECTORY(), 3, true);

    Version instanceVersion = onDiskVersion;
    instanceVersion.AddSegment(1);
    instanceVersion.AddSegment(3);
    instanceVersion.IncVersionId();
    instanceVersion.Store(GET_PARTITION_DIRECTORY(), false);

    ParallelOfflineSegmentDirectoryPtr recoverSegDir(new ParallelOfflineSegmentDirectory);
    recoverSegDir->Init(GET_PARTITION_DIRECTORY(), onDiskVersion, true);
    ASSERT_EQ((segmentid_t)5, recoverSegDir->CreateNewSegmentId());
    recoverSegDir->IncLastSegmentId();
    ASSERT_EQ((segmentid_t)7, recoverSegDir->CreateNewSegmentId());
}

void ParallelOfflineSegmentDirectoryTest::TestCheckValidBaseVersion()
{
    LoadConfigList loadConfigList;
    string secondaryPath = GET_TEMP_DATA_PATH() + "/partition_path";
    FslibWrapper::MkDirE(secondaryPath);
    ParallelBuildInfo parallelInfo(2, 0, 1);

    // set invalid base version
    parallelInfo.SetBaseVersion(3);
    Version onDiskVersion = PrepareRootDir(secondaryPath, "0,1");
    CreateEmptyFile(secondaryPath);
    ASSERT_EQ(FSEC_OK, GET_FILE_SYSTEM()->MountVersion(secondaryPath, onDiskVersion.GetVersionId(), "", FSMT_READ_ONLY,
                                                       nullptr));
    parallelInfo.StoreIfNotExist(GET_PARTITION_DIRECTORY());

    ParallelOfflineSegmentDirectoryPtr segDir(new ParallelOfflineSegmentDirectory);
    ASSERT_THROW(segDir->Init(GET_PARTITION_DIRECTORY(), onDiskVersion, true), InconsistentStateException);
}

Version ParallelOfflineSegmentDirectoryTest::PrepareRootDir(const string& path, const string& segmentIds)
{
    DirectoryPtr rootDir = test::DirectoryCreator::Create(path, true);
    Version onDiskVersion = VersionMaker::Make(rootDir, 0, segmentIds, "", "", 0, true);
    return onDiskVersion;
}

void ParallelOfflineSegmentDirectoryTest::TestGetNextSegmentId()
{
    {
        LoadConfigList loadConfigList;
        string secondaryPath = GET_TEMP_DATA_PATH() + "/partition_path";
        ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(secondaryPath).Code());
        ParallelBuildInfo parallelInfo1(2, 0, 1);
        parallelInfo1.SetBaseVersion(0);
        Version onDiskVersion = PrepareRootDir(secondaryPath, "");
        parallelInfo1.StoreIfNotExist(GET_PARTITION_DIRECTORY());

        // check new segment id by base >= mStartBuildingSegId
        ParallelOfflineSegmentDirectoryPtr segDir(new ParallelOfflineSegmentDirectory);
        segDir->Init(GET_PARTITION_DIRECTORY(), onDiskVersion, true);
        ASSERT_EQ((segmentid_t)10, segDir->GetNextSegmentId(8));
        segDir->IncLastSegmentId();
        ASSERT_EQ((segmentid_t)3, segDir->CreateNewSegmentId());
        ASSERT_EQ((segmentid_t)10, segDir->GetNextSegmentId(8));

        // create instance version
        VersionMaker::MakeIncSegment(GET_PARTITION_DIRECTORY(), 1, true);
        VersionMaker::MakeIncSegment(GET_PARTITION_DIRECTORY(), 3, true);

        Version instanceVersion = onDiskVersion;
        instanceVersion.AddSegment(1);
        instanceVersion.AddSegment(3);
        instanceVersion.IncVersionId();
        instanceVersion.Store(GET_PARTITION_DIRECTORY(), false);

        ParallelOfflineSegmentDirectoryPtr recoverSegDir(new ParallelOfflineSegmentDirectory);
        recoverSegDir->Init(GET_PARTITION_DIRECTORY(), onDiskVersion, true);

        ASSERT_EQ((segmentid_t)5, recoverSegDir->CreateNewSegmentId());
        recoverSegDir->IncLastSegmentId();
        ASSERT_EQ((segmentid_t)7, recoverSegDir->CreateNewSegmentId());

        ASSERT_EQ((segmentid_t)9, recoverSegDir->GetNextSegmentId(7));
        recoverSegDir->IncLastSegmentId();
        ASSERT_EQ((segmentid_t)11, recoverSegDir->GetNextSegmentId(9));

        // test baseSegmentId < mStartBuildingSegId
        ASSERT_EQ((segmentid_t)1, recoverSegDir->GetNextSegmentId(0));
    }
}
}} // namespace indexlib::index_base
