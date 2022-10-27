#include "indexlib/index_base/segment/test/parallel_offline_segment_directory_unittest.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/file_system/local_directory.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/file_system/directory_creator.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, ParallelOfflineSegmentDirectoryTest);

ParallelOfflineSegmentDirectoryTest::ParallelOfflineSegmentDirectoryTest()
{
}

ParallelOfflineSegmentDirectoryTest::~ParallelOfflineSegmentDirectoryTest()
{
}

void ParallelOfflineSegmentDirectoryTest::CaseSetUp()
{
}

void ParallelOfflineSegmentDirectoryTest::CaseTearDown()
{
}

void ParallelOfflineSegmentDirectoryTest::TestSimpleProcess()
{
    LoadConfigList loadConfigList;
    string secondaryPath = GET_TEST_DATA_PATH() + "/partition_path";
    FileSystemWrapper::MkDir(secondaryPath);
    RESET_FILE_SYSTEM(loadConfigList, false, true, true, false, secondaryPath, true);
    ParallelBuildInfo parallelInfo(2, 0, 1);
    parallelInfo.SetBaseVersion(0);
    Version onDiskVersion = PrepareRootDir(secondaryPath, "0,1");
    parallelInfo.StoreIfNotExist(GET_TEST_DATA_PATH());

    {
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

    {
        // check recover
        DirectoryPtr partitionRootDir = DirectoryCreator::Create(secondaryPath, true);
        VersionMaker::MakeIncSegment(partitionRootDir, 3, true);

        DirectoryPtr instanceRootDir = DirectoryCreator::Create(GET_TEST_DATA_PATH(), true);
        VersionMaker::MakeIncSegment(instanceRootDir, 4, true);
        VersionMaker::MakeIncSegment(instanceRootDir, 5, true);
        instanceRootDir->RemoveFile("segment_5_level_0/segment_info");
        ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExist("segment_5_level_0"));
        
        ParallelOfflineSegmentDirectoryPtr segDir(new ParallelOfflineSegmentDirectory);
        segDir->Init(GET_PARTITION_DIRECTORY(), onDiskVersion, true);

        Version version = segDir->GetVersion();
        // check partition path not recover
        ASSERT_FALSE(version.HasSegment(3));
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

}

void ParallelOfflineSegmentDirectoryTest::TestParallelIncBuildFromEmptyFullIndex()
{
    LoadConfigList loadConfigList;
    string secondaryPath = GET_TEST_DATA_PATH() + "/partition_path";
    FileSystemWrapper::MkDir(secondaryPath);
    RESET_FILE_SYSTEM(loadConfigList, false, true, true, false, secondaryPath);
    ParallelBuildInfo parallelInfo(2, 0, 1);
    parallelInfo.SetBaseVersion(0);
    Version onDiskVersion = PrepareRootDir(secondaryPath, "");
    parallelInfo.StoreIfNotExist(GET_TEST_DATA_PATH());

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
    LoadConfigList loadConfigList;
    string secondaryPath = GET_TEST_DATA_PATH() + "/partition_path";
    FileSystemWrapper::MkDir(secondaryPath);
    RESET_FILE_SYSTEM(loadConfigList, false, true, true, false, secondaryPath);
    ParallelBuildInfo parallelInfo(2, 0, 1);
    parallelInfo.SetBaseVersion(0);
    Version onDiskVersion = PrepareRootDir(secondaryPath, "");
    parallelInfo.StoreIfNotExist(GET_TEST_DATA_PATH());

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
    string secondaryPath = GET_TEST_DATA_PATH() + "/partition_path";
    FileSystemWrapper::MkDir(secondaryPath);
    RESET_FILE_SYSTEM(loadConfigList, false, true, true, false, secondaryPath);
    ParallelBuildInfo parallelInfo(2, 0, 1);

    // set invalid base version
    parallelInfo.SetBaseVersion(3);
    Version onDiskVersion = PrepareRootDir(secondaryPath, "0,1");
    parallelInfo.StoreIfNotExist(GET_TEST_DATA_PATH());

    ParallelOfflineSegmentDirectoryPtr segDir(new ParallelOfflineSegmentDirectory);
    ASSERT_THROW(segDir->Init(GET_PARTITION_DIRECTORY(), onDiskVersion, true),
                 InconsistentStateException);
}

Version ParallelOfflineSegmentDirectoryTest::PrepareRootDir(
        const string& path, const string& segmentIds)
{
    DirectoryPtr rootDir = DirectoryCreator::Create(path, true);
    Version onDiskVersion = VersionMaker::Make(rootDir,
            0, segmentIds, "", "", 0, true);
    return onDiskVersion;
}

IE_NAMESPACE_END(index_base);

