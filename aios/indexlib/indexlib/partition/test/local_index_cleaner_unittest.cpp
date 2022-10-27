#include "indexlib/partition/test/local_index_cleaner_unittest.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/index_base/index_meta/index_path_util.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, LocalIndexCleanerTest);

namespace
{
    class MockReaderContainer : public ReaderContainer
    {
    public:
        MockReaderContainer(const vector<Version>& versions = {})
            : mVersions(versions)
            {}
        void GetIncVersions(vector<Version>& versions) const override {
            for (const Version& version : mVersions) {
                versions.push_back(version);
            }
        }
        versionid_t GetLatestReaderVersion() const override {
            versionid_t lastestVersionId = INVALID_VERSION;
            for (const Version& version : mVersions) {
                lastestVersionId = max(lastestVersionId, version.GetVersionId());
            }
            return lastestVersionId;
        }
    private:
        vector<Version> mVersions;
    };
    DEFINE_SHARED_PTR(MockReaderContainer);
}

LocalIndexCleanerTest::LocalIndexCleanerTest()
{
}

LocalIndexCleanerTest::~LocalIndexCleanerTest()
{
}

void LocalIndexCleanerTest::CaseSetUp()
{
    mPrimaryDir = GET_PARTITION_DIRECTORY()->MakeDirectory("primary");
    mSecondaryDir = GET_PARTITION_DIRECTORY()->MakeDirectory("secondary");
    mPrimaryPath = mPrimaryDir->GetPath();
    mSecondaryPath = mSecondaryDir->GetPath();
    
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema,
            "string0:string;string1:string;long1:uint32;string2:string:true", 
            "string2:string:string2;"
            "pk:primarykey64:string1",
            "long1;string0;string1;string2",
            "string1" );
}

void LocalIndexCleanerTest::CaseTearDown()
{
}

/// -- begin -- ut from on_disk_index_cleaner_unittest.cpp
void LocalIndexCleanerTest::TestXSimpleProcess()
{
    VersionMaker::Make(mSecondaryDir, 0, "0,1");
    VersionMaker::Make(mSecondaryDir, 1, "1,2");
    LocalIndexCleaner cleaner(mSecondaryDir, 1, ReaderContainerPtr());
    cleaner.Clean({});
    ASSERT_FALSE(mSecondaryDir->IsExist("version.0"));
    ASSERT_FALSE(mSecondaryDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_1_level_0"));
}

void LocalIndexCleanerTest::TestXDeployIndexMeta()
{
    VersionMaker::Make(mSecondaryDir, 0, "0,1");
    VersionMaker::Make(mSecondaryDir, 1, "1,2");
    mSecondaryDir->Store(DeployIndexWrapper::GetDeployMetaFileName(0), "");
    mSecondaryDir->Store(DeployIndexWrapper::GetDeployMetaFileName(1), "");
    ASSERT_TRUE(mSecondaryDir->IsExist("version.0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("deploy_meta.0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("deploy_meta.1"));
    LocalIndexCleaner cleaner(mSecondaryDir, 1, ReaderContainerPtr());
    cleaner.Clean({});
    ASSERT_FALSE(mSecondaryDir->IsExist("version.0"));
    ASSERT_FALSE(mSecondaryDir->IsExist("deploy_meta.0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.1"));
    ASSERT_TRUE(mSecondaryDir->IsExist("deploy_meta.1"));
}

void LocalIndexCleanerTest::TestXCleanWithNoClean()
{
    VersionMaker::Make(mSecondaryDir, 0, "0");
    VersionMaker::Make(mSecondaryDir, 1, "0,1");

    LocalIndexCleaner cleaner(mSecondaryDir, 2, ReaderContainerPtr());
    cleaner.Clean({});

    ASSERT_TRUE(mSecondaryDir->IsExist("version.0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.1"));

    ASSERT_TRUE(mSecondaryDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_1_level_0"));
}

void LocalIndexCleanerTest::TestXCleanWithKeepCount0()
{
    VersionMaker::Make(mSecondaryDir, 0, "0");
    VersionMaker::Make(mSecondaryDir, 1, "1");

    LocalIndexCleaner cleaner(mSecondaryDir, 0, ReaderContainerPtr());
    cleaner.Clean({});

    ASSERT_FALSE(mSecondaryDir->IsExist("version.0"));
    ASSERT_FALSE(mSecondaryDir->IsExist("version.1"));

    ASSERT_FALSE(mSecondaryDir->IsExist("segment_0_level_0"));
    ASSERT_FALSE(mSecondaryDir->IsExist("segment_1_level_0"));
}

void LocalIndexCleanerTest::TestXCleanWithIncontinuousVersion() 
{
    VersionMaker::Make(mSecondaryDir, 0, "0,1,2");
    VersionMaker::Make(mSecondaryDir, 2, "0,3");
    VersionMaker::Make(mSecondaryDir, 4, "0,3,4");
    LocalIndexCleaner cleaner(mSecondaryDir, 2, ReaderContainerPtr());
    cleaner.Clean({});

    ASSERT_FALSE(mSecondaryDir->IsExist("version.0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.2"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.4"));

    ASSERT_TRUE(mSecondaryDir->IsExist("segment_0_level_0"));
    ASSERT_FALSE(mSecondaryDir->IsExist("segment_1_level_0"));
    ASSERT_FALSE(mSecondaryDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_3_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_4_level_0"));
}

void LocalIndexCleanerTest::TestXCleanWithInvalidSegments()
{
    VersionMaker::Make(mSecondaryDir, 0, "0");
    VersionMaker::Make(mSecondaryDir, 2, "9");
    VersionMaker::MakeIncSegment(mSecondaryDir, 5);
    VersionMaker::MakeIncSegment(mSecondaryDir, 10);
    
    LocalIndexCleaner cleaner(mSecondaryDir, 1, ReaderContainerPtr());
    cleaner.Clean({});

    ASSERT_FALSE(mSecondaryDir->IsExist("version.0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.2"));

    ASSERT_FALSE(mSecondaryDir->IsExist("segment_5_level_0"));
    ASSERT_FALSE(mSecondaryDir->IsExist("segment_10_level_0"));
}

void LocalIndexCleanerTest::TestXCleanWithEmptyVersion()
{
    VersionMaker::Make(mSecondaryDir, 0, "0");
    Version(2).Store(mSecondaryDir, true);
    Version(4).Store(mSecondaryDir, true);
    LocalIndexCleaner cleaner(mSecondaryDir, 1, ReaderContainerPtr());
    cleaner.Clean({});

    ASSERT_FALSE(mSecondaryDir->IsExist("version.0"));
    ASSERT_FALSE(mSecondaryDir->IsExist("version.2"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.4"));
    ASSERT_FALSE(mSecondaryDir->IsExist("segment_0_level_0"));
}

void LocalIndexCleanerTest::TestXCleanWithAllEmptyVersion()
{
    Version(0).Store(mSecondaryDir, true);
    Version(1).Store(mSecondaryDir, true);
    VersionMaker::MakeIncSegment(mSecondaryDir, 0);
    LocalIndexCleaner cleaner(mSecondaryDir, 1, ReaderContainerPtr());
    cleaner.Clean({});

    ASSERT_FALSE(mSecondaryDir->IsExist("version.0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.1"));
    ASSERT_FALSE(mSecondaryDir->IsExist("segment_0_level_0"));
}

void LocalIndexCleanerTest::TestXCleanWithUsingVersion()
{
    vector<Version> readerVersions;
    VersionMaker::Make(mSecondaryDir, 0, "0");
    readerVersions.push_back(VersionMaker::Make(mSecondaryDir, 1, "1,2"));
    readerVersions.push_back(VersionMaker::Make(mSecondaryDir, 2, "1,3"));
    VersionMaker::Make(mSecondaryDir, 3, "1,3,4");
    MockReaderContainerPtr container(new MockReaderContainer(readerVersions));
    LocalIndexCleaner cleaner(mSecondaryDir, 1, container);
    cleaner.Clean({3});

    ASSERT_FALSE(mSecondaryDir->IsExist("version.0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.1"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.2"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.3"));
    ASSERT_FALSE(mSecondaryDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_3_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_4_level_0"));
}

void LocalIndexCleanerTest::TestXCleanWithUsingOldVersion()
{
    vector<Version> readerVersions;
    readerVersions.push_back(VersionMaker::Make(mSecondaryDir, 0, "0"));
    VersionMaker::Make(mSecondaryDir, 1, "1,2");
    VersionMaker::Make(mSecondaryDir, 2, "1,3");
    MockReaderContainerPtr container(new MockReaderContainer(readerVersions));
    LocalIndexCleaner cleaner(mSecondaryDir, 1, container);
    cleaner.Clean({1,2});

    ASSERT_TRUE(mSecondaryDir->IsExist("version.0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.1"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.2"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_3_level_0"));
}

void LocalIndexCleanerTest::TestXCleanWithRollbackVersion()
{
    VersionMaker::Make(mSecondaryDir, 0, "0");
    VersionMaker::Make(mSecondaryDir, 1, "1");
    VersionMaker::Make(mSecondaryDir, 2, "0");
    LocalIndexCleaner cleaner(mSecondaryDir, 1, ReaderContainerPtr());
    cleaner.Clean({});

    ASSERT_FALSE(mSecondaryDir->IsExist("version.0"));
    ASSERT_FALSE(mSecondaryDir->IsExist("version.1"));
    ASSERT_TRUE(mSecondaryDir->IsExist("version.2"));
    ASSERT_TRUE(mSecondaryDir->IsExist("segment_0_level_0"));
    ASSERT_FALSE(mSecondaryDir->IsExist("segment_1_level_0"));
}
/// -- end -- ut from on_disk_index_cleaner_unittest.cpp

void LocalIndexCleanerTest::DpToOnline(const vector<versionid_t> versionIds,
                                       const vector<segmentid_t> segmentIds)
{
    for (versionid_t versionId : versionIds)
    {
        string versionFileName = Version::GetVersionFileName(versionId);
        FileSystemWrapper::Copy(mSecondaryPath + "/" + versionFileName,
                                mPrimaryPath + "/" + versionFileName);
    }
    for (segmentid_t segmentId : segmentIds)
    {
        string segmentFileName = Version().GetNewSegmentDirName(segmentId);
        FileSystemWrapper::SymLink(mSecondaryPath + "/" + segmentFileName,
                                   mPrimaryPath + "/" + segmentFileName);
    }
}

void LocalIndexCleanerTest::CheckOnlineVersions(const vector<versionid_t>& versionIds,
                                                const vector<segmentid_t>& segmentIds)
{
    fslib::FileList actualFiles;
    FileSystemWrapper::ListDir(mPrimaryPath, actualFiles);

    vector<string> expectFiles;
    for (versionid_t versionId : versionIds)
    {
        expectFiles.push_back(Version::GetVersionFileName(versionId));
    }
    for (segmentid_t segmentId : segmentIds)
    {
        expectFiles.push_back(Version().GetNewSegmentDirName(segmentId));
    }
    EXPECT_THAT(actualFiles, testing::UnorderedElementsAreArray(expectFiles));
};

void LocalIndexCleanerTest::Clean(const map<versionid_t, Version>& versions,
                                  const vector<versionid_t>& keepVersionIds,
                                  const vector<versionid_t>& readerContainerHoldVersionIds,
                                  uint32_t keepVersionCount)
{
    vector<Version> readerVersions;
    for (versionid_t versionId : readerContainerHoldVersionIds)
    {
        readerVersions.emplace_back(versions.at(versionId));
    }
    ReaderContainerPtr readerContainer(new MockReaderContainer(readerVersions));
    auto fs = IndexlibFileSystemCreator::Create(mPrimaryPath, mSecondaryPath);
    DirectoryPtr dir = DirectoryCreator::Get(fs, mPrimaryPath, true);
    LocalIndexCleaner cleaner(dir, keepVersionCount, readerContainer);
    cleaner.Clean(keepVersionIds);
};

void LocalIndexCleanerTest::ResetOnlineDirectory() {
    if (FileSystemWrapper::IsExist(mPrimaryPath)) {
        FileSystemWrapper::DeleteDir(mPrimaryPath);
    }
    FileSystemWrapper::MkDir(mPrimaryPath);
};

void LocalIndexCleanerTest::TestSimpleProcess()
{
    auto versions = PartitionDataMaker::MakeVersions(mSecondaryDir, "1:0,1");
    for (auto keepVersionCount : vector<uint32_t>({0, 1, 9, INVALID_KEEP_VERSION_COUNT}))
    {
        ResetOnlineDirectory();
        Clean(versions, {}, {}, keepVersionCount);
        CheckOnlineVersions({}, {});

        ResetOnlineDirectory();
        DpToOnline({1}, {0, 1});
        Clean(versions, {}, {1}, keepVersionCount);
        CheckOnlineVersions({1}, {0, 1});

        ResetOnlineDirectory();
        DpToOnline({1}, {0, 1});
        Clean(versions, {1}, {}, keepVersionCount);
        CheckOnlineVersions({1}, {0, 1});

        ResetOnlineDirectory();
        DpToOnline({1}, {0, 1});
        Clean(versions, {1}, {1}, keepVersionCount);
        CheckOnlineVersions({1}, {0, 1});
    }
}

void LocalIndexCleanerTest::TestNotDpVersionFile()
{
    auto versions = PartitionDataMaker::MakeVersions(mSecondaryDir, "1:0,1");
    for (auto keepVersionCount : vector<uint32_t>({0, 1, 9, INVALID_KEEP_VERSION_COUNT}))
    {
        ResetOnlineDirectory();
        DpToOnline({}, {0, 1});
        Clean(versions, {}, {1}, keepVersionCount);
        CheckOnlineVersions({}, {0, 1});

        ResetOnlineDirectory();
        DpToOnline({}, {0, 1});
        Clean(versions, {1}, {}, keepVersionCount);
        CheckOnlineVersions({}, {0, 1});

        ResetOnlineDirectory();
        DpToOnline({}, {0, 1});
        Clean(versions, {1}, {1}, keepVersionCount);
        CheckOnlineVersions({}, {0, 1});
    }
}

void LocalIndexCleanerTest::TestCleanWhenSomeVersionDeploying()
{
    auto versions = PartitionDataMaker::MakeVersions(mSecondaryDir, "0:0; 2:0,1,2; 4:2,3,4");
    DpToOnline({0, 2}, {0, 1, 2});
    Clean(versions, {2, 4}, {2}, 1);
    CheckOnlineVersions({2}, {0, 1, 2});

    ResetOnlineDirectory();        
    DpToOnline({0, 2}, {0, 1, 2});
    Clean(versions, {2, 4}, {2}, 3);
    CheckOnlineVersions({0, 2}, {0, 1, 2});

    ResetOnlineDirectory();
    DpToOnline({0, 2}, {0, 1, 2, 4});
    Clean(versions, {2, 4}, {2}, 1);
    CheckOnlineVersions({2}, {0, 1, 2, 4});
    
    ResetOnlineDirectory();
    DpToOnline({0, 2}, {0, 1, 2, 4});
    Clean(versions, {2, 4}, {2}, 3);
    CheckOnlineVersions({0, 2}, {0, 1, 2, 4});
    
    ResetOnlineDirectory();
    DpToOnline({0, 2, 4}, {0, 1, 2, 4});
    Clean(versions, {2, 4}, {2}, 1);
    CheckOnlineVersions({2, 4}, {0, 1, 2, 4});
    
    ResetOnlineDirectory();
    DpToOnline({0, 2, 4}, {0, 1, 2, 4});
    Clean(versions, {2, 4}, {2}, 2);
    CheckOnlineVersions({2, 4}, {0, 1, 2, 4});
    
    ResetOnlineDirectory();
    DpToOnline({0, 2, 4}, {0, 1, 2, 4});
    Clean(versions, {2, 4}, {2}, 3);
    CheckOnlineVersions({0, 2, 4}, {0, 1, 2, 4});
}

void LocalIndexCleanerTest::TestProcessSimulation()
{
    uint32_t keepVersionCount = 1;
    auto versions = PartitionDataMaker::MakeVersions(mSecondaryDir,
            "0:0; 1:0,1; 2:1,2; 3:2,3; 4:3,4; 5:4,5; 6:5,6; 7:6,7");
    auto fs = IndexlibFileSystemCreator::Create(mPrimaryPath, mSecondaryPath);
    DirectoryPtr dir = DirectoryCreator::Get(fs, mPrimaryPath, true);
    vector<Version> emptyVersions;

    Clean(versions, {}, {}, keepVersionCount);

    // start dp
    Clean(versions, {0}, {}, keepVersionCount);
    Clean(versions, {1}, {}, keepVersionCount);
    
    // deploying version 1
    DpToOnline({}, {0});
    Clean(versions, {1}, {}, keepVersionCount);
    CheckOnlineVersions({}, {0});
    
    DpToOnline({}, {1});
    Clean(versions, {1}, {}, keepVersionCount);
    CheckOnlineVersions({}, {0,1});

    // open 
    Clean(versions, {1}, {1}, keepVersionCount);
    CheckOnlineVersions({}, {0,1});

    // deploying version 2
    DpToOnline({}, {2});
    Clean(versions, {1,2}, {1}, keepVersionCount);
    CheckOnlineVersions({}, {0,1,2});
    
    // deploying version 4
    DpToOnline({}, {3});
    Clean(versions, {1,4}, {1}, keepVersionCount);
    CheckOnlineVersions({}, {0,1,3});
    
    DpToOnline({}, {4});
    Clean(versions, {1,4}, {1}, keepVersionCount);
    CheckOnlineVersions({}, {0,1,3,4});

    // reoprn version 1->4
    Clean(versions, {4}, {1}, keepVersionCount);
    CheckOnlineVersions({}, {0,1,3,4});

    Clean(versions, {4}, {1,4}, keepVersionCount);
    CheckOnlineVersions({}, {0,1,3,4});

    Clean(versions, {4}, {4}, keepVersionCount);
    CheckOnlineVersions({}, {3,4});
    
    // dp version 5 6 7
    keepVersionCount = 3;
    
    DpToOnline({5,6,7}, {4,5,6,7});
    Clean(versions, {7}, {7}, keepVersionCount);
    CheckOnlineVersions({5,6,7}, {4,5,6,7});
}

/*
void LocalIndexCleanerTest::TestRollbackVersion()
{
    auto versions = PartitionDataMaker::MakeVersions(mSecondaryDir,
            "0:0; 1:0,1; 2:1,2; 3:0,1;");
    auto fs = IndexlibFileSystemCreator::Create(mPrimaryPath, mSecondaryPath);
    DirectoryPtr dir = DirectoryCreator::Get(fs, mPrimaryPath, true);
    vector<Version> emptyVersions;

    {
        DpToOnline({1,2}, {0,1,2});
        ReaderContainerPtr readerContainer(new MockReaderContainer({versions.at(2)}));
        LocalIndexCleaner(dir, 1, readerContainer).Execute();
        CheckOnlineVersions({2}, {1,2});
    }
    {
        DpToOnline({3}, {0});
        ReaderContainerPtr readerContainer(new MockReaderContainer({versions.at(2)}));
        LocalIndexCleaner(dir, 1, readerContainer).Execute();
        CheckOnlineVersions({2,3}, {0,1,2});
    }
    {
        ReaderContainerPtr readerContainer(new MockReaderContainer({versions.at(3)}));
        LocalIndexCleaner(dir, 1, readerContainer).Execute();
        CheckOnlineVersions({3}, {0,1,2});
    }
}

void LocalIndexCleanerTest::TestWorkAsExecuteor()
{
    auto versions = PartitionDataMaker::MakeVersions(mSecondaryDir, "0:0; 1:0,1; 2:1,2; 3:2,3; 4:3,4");
    DpToOnline({0, 1, 2, 3}, {0, 1, 2, 3});

    auto fs = IndexlibFileSystemCreator::Create(mPrimaryPath, mSecondaryPath);
    DirectoryPtr dir = DirectoryCreator::Get(fs, mPrimaryPath, true);
    vector<Version> emptyVersions;

    {
        ReaderContainerPtr readerContainer(new MockReaderContainer(
                                               {versions.at(1), versions.at(2)}));
        LocalIndexCleaner(dir, 1, readerContainer).Execute();
        CheckOnlineVersions({1,2,3}, {0,1,2,3});
    }
    {
        ReaderContainerPtr readerContainer(new MockReaderContainer({versions.at(2)}));
        LocalIndexCleaner(dir, 1, readerContainer).Execute();
        CheckOnlineVersions({2,3}, {1,2,3});
    }
    {
        ReaderContainerPtr readerContainer(new MockReaderContainer({versions.at(3)}));
        LocalIndexCleaner(dir, 1, readerContainer).Execute();
        CheckOnlineVersions({3}, {2,3});
    }
    {
        ReaderContainerPtr readerContainer(new MockReaderContainer(emptyVersions));
        LocalIndexCleaner(dir, 1, readerContainer).Execute();
        CheckOnlineVersions({3}, {2,3});
    }
    
    DpToOnline({4}, {4});
    {
        ReaderContainerPtr readerContainer(new MockReaderContainer(emptyVersions));
        LocalIndexCleaner(dir, 3, readerContainer).Execute();
        CheckOnlineVersions({3,4}, {2,3,4});
    }
    {
        ReaderContainerPtr readerContainer(new MockReaderContainer(emptyVersions));
        LocalIndexCleaner(dir, 2, readerContainer).Execute();
        CheckOnlineVersions({3,4}, {2,3,4});
    }
    {
        ReaderContainerPtr readerContainer(new MockReaderContainer(emptyVersions));
        LocalIndexCleaner(dir, 1, readerContainer).Execute();
        CheckOnlineVersions({4}, {3,4});
    }
    {
        ReaderContainerPtr readerContainer(new MockReaderContainer(emptyVersions));
        LocalIndexCleaner(dir, 0, readerContainer).Execute();
        CheckOnlineVersions({}, {});
    }
}
*/

IE_NAMESPACE_END(partition);
