#include "indexlib/partition/test/local_index_cleaner_unittest.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/index_path_util.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, LocalIndexCleanerTest);

namespace {
class MockReaderContainer : public ReaderContainer
{
public:
    MockReaderContainer(const vector<Version>& versions = {}) : mVersions(versions) {}
    void GetIncVersions(vector<Version>& versions) const override
    {
        for (const Version& version : mVersions) {
            versions.push_back(version);
        }
    }
    versionid_t GetLatestReaderVersion() const override
    {
        versionid_t lastestVersionId = INVALID_VERSIONID;
        for (const Version& version : mVersions) {
            lastestVersionId = max(lastestVersionId, version.GetVersionId());
        }
        return lastestVersionId;
    }

    void SetWhiteList(const std::set<std::string>& whiteList) { mWhiteList = whiteList; }
    bool GetNeedKeepDeployFiles(std::set<std::string>* whiteList) override
    {
        *whiteList = mWhiteList;
        return true;
    }

public:
    std::set<std::string> mWhiteList;

private:
    vector<Version> mVersions;
};
DEFINE_SHARED_PTR(MockReaderContainer);

} // namespace

LocalIndexCleanerTest::LocalIndexCleanerTest() {}

LocalIndexCleanerTest::~LocalIndexCleanerTest() {}

void LocalIndexCleanerTest::CheckFileState(const std::set<std::string>& whiteList,
                                           const std::set<std::string>& blackList)
{
    for (const auto& name : whiteList) {
        std::string filePath = FslibWrapper::JoinPath(mPrimaryPath, name);
        auto ret = FslibWrapper::IsExist(filePath);
        ASSERT_EQ(indexlib::file_system::FSEC_OK, ret.ec);
        ASSERT_TRUE(ret.result) << filePath << " not exist!";
    }
    for (const auto& name : blackList) {
        std::string filePath = FslibWrapper::JoinPath(mPrimaryPath, name);
        auto ret = FslibWrapper::IsExist(filePath);
        ASSERT_EQ(indexlib::file_system::FSEC_OK, ret.ec);
        ASSERT_FALSE(ret.result) << filePath << "exists!";
    }
}

void LocalIndexCleanerTest::CaseSetUp()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_DISK;
    RESET_FILE_SYSTEM("ut", false, fsOptions);

    mPrimaryDir = GET_PARTITION_DIRECTORY()->MakeDirectory("primary");
    mSecondaryDir = GET_PARTITION_DIRECTORY()->MakeDirectory("secondary");
    mPrimaryPath = mPrimaryDir->GetOutputPath();
    mSecondaryPath = mSecondaryDir->GetOutputPath();

    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema, "string0:string;string1:string;long1:uint32;string2:string:true",
                                     "string2:string:string2;"
                                     "pk:primarykey64:string1",
                                     "long1;string0;string1;string2", "string1");
}

void LocalIndexCleanerTest::CaseTearDown() {}

/// -- begin -- ut from on_disk_index_cleaner_unittest.cpp
void LocalIndexCleanerTest::TestXSimpleProcess()
{
    VersionMaker::Make(mPrimaryDir, 0, "0,1");
    VersionMaker::Make(mPrimaryDir, 1, "1,2");
    LocalIndexCleaner cleaner(mPrimaryPath, mSecondaryPath, 1, ReaderContainerPtr());
    cleaner.Clean({});
    CheckOnlineVersions({1}, {1, 2});
}

void LocalIndexCleanerTest::TestXDeployIndexMeta()
{
    VersionMaker::Make(mPrimaryDir, 0, "0,1");
    VersionMaker::Make(mPrimaryDir, 1, "1,2");
    WriterOption writerOption = WriterOption::BufferAtomicDump();
    mPrimaryDir->Store("deploy_meta.0", "", writerOption);
    mPrimaryDir->Store("deploy_meta.1", "", writerOption);
    CheckOnlineVersions({0, 1}, {0, 1, 2}, {"deploy_meta.0", "deploy_meta.1"});
    LocalIndexCleaner cleaner(mPrimaryPath, "", 1, ReaderContainerPtr());
    cleaner.Clean({});
    CheckOnlineVersions({1}, {1, 2}, {"deploy_meta.1"});
}

void LocalIndexCleanerTest::TestXCleanWithNoClean()
{
    VersionMaker::Make(mPrimaryDir, 0, "0");
    VersionMaker::Make(mPrimaryDir, 1, "0,1");

    LocalIndexCleaner cleaner(mPrimaryPath, "", 2, ReaderContainerPtr());
    cleaner.Clean({});
    CheckOnlineVersions({0, 1}, {0, 1});
}

void LocalIndexCleanerTest::TestXCleanWithKeepCount0()
{
    VersionMaker::Make(mPrimaryDir, 0, "0");
    VersionMaker::Make(mPrimaryDir, 1, "1");

    LocalIndexCleaner cleaner(mPrimaryPath, "", 0, ReaderContainerPtr());
    cleaner.Clean({});
    CheckOnlineVersions({}, {});
}

void LocalIndexCleanerTest::TestXCleanWithIncontinuousVersion()
{
    VersionMaker::Make(mPrimaryDir, 0, "0,1,2");
    VersionMaker::Make(mPrimaryDir, 2, "0,3");
    VersionMaker::Make(mPrimaryDir, 4, "0,3,4");
    LocalIndexCleaner cleaner(mPrimaryPath, "", 2, ReaderContainerPtr());
    cleaner.Clean({});
    CheckOnlineVersions({2, 4}, {0, 3, 4});
}

void LocalIndexCleanerTest::TestXCleanWithInvalidSegments()
{
    VersionMaker::Make(mPrimaryDir, 0, "0");
    VersionMaker::Make(mPrimaryDir, 2, "9");
    VersionMaker::MakeIncSegment(mPrimaryDir, 5);
    VersionMaker::MakeIncSegment(mPrimaryDir, 10);

    LocalIndexCleaner cleaner(mPrimaryPath, "", 1, ReaderContainerPtr());
    cleaner.Clean({});
    CheckOnlineVersions({2}, {9});
}

void LocalIndexCleanerTest::TestXCleanWithEmptyVersion()
{
    VersionMaker::Make(mPrimaryDir, 0, "0");
    Version(2).Store(mPrimaryDir, true);
    Version(4).Store(mPrimaryDir, true);
    LocalIndexCleaner cleaner(mPrimaryPath, "", 1, ReaderContainerPtr());
    cleaner.Clean({});
    CheckOnlineVersions({4}, {});
}

void LocalIndexCleanerTest::TestXCleanWithAllEmptyVersion()
{
    Version(0).Store(mPrimaryDir, true);
    Version(1).Store(mPrimaryDir, true);
    VersionMaker::MakeIncSegment(mPrimaryDir, 0);
    LocalIndexCleaner cleaner(mPrimaryPath, "", 1, ReaderContainerPtr());
    cleaner.Clean({});
    CheckOnlineVersions({1}, {});
}

void LocalIndexCleanerTest::TestXCleanWithUsingVersion()
{
    vector<Version> readerVersions;
    VersionMaker::Make(mPrimaryDir, 0, "0");
    readerVersions.push_back(VersionMaker::Make(mPrimaryDir, 1, "1,2"));
    readerVersions.push_back(VersionMaker::Make(mPrimaryDir, 2, "1,3"));
    VersionMaker::Make(mPrimaryDir, 3, "1,3,4");
    MockReaderContainerPtr container(new MockReaderContainer(readerVersions));
    LocalIndexCleaner cleaner(mPrimaryPath, "", 1, container);
    cleaner.Clean({3});
    CheckOnlineVersions({1, 2, 3}, {1, 2, 3, 4});
}

void LocalIndexCleanerTest::TestXCleanWithUsingOldVersion()
{
    vector<Version> readerVersions;
    readerVersions.push_back(VersionMaker::Make(mPrimaryDir, 0, "0"));
    VersionMaker::Make(mPrimaryDir, 1, "1,2");
    VersionMaker::Make(mPrimaryDir, 2, "1,3");
    MockReaderContainerPtr container(new MockReaderContainer(readerVersions));
    LocalIndexCleaner cleaner(mPrimaryPath, "", 1, container);
    cleaner.Clean({1, 2});
    CheckOnlineVersions({0, 1, 2}, {0, 1, 2, 3});
}

void LocalIndexCleanerTest::TestXCleanWithRollbackVersion()
{
    VersionMaker::Make(mPrimaryDir, 0, "0");
    VersionMaker::Make(mPrimaryDir, 1, "1");
    VersionMaker::Make(mPrimaryDir, 2, "0");
    LocalIndexCleaner cleaner(mPrimaryPath, "", 1, ReaderContainerPtr());
    cleaner.Clean({});
    CheckOnlineVersions({2}, {0});
}
/// -- end -- ut from on_disk_index_cleaner_unittest.cpp

void LocalIndexCleanerTest::DpToOnline(const vector<versionid_t> versionIds, const vector<segmentid_t> segmentIds)
{
    for (versionid_t versionId : versionIds) {
        string versionFileName = Version::GetVersionFileName(versionId);
        ASSERT_EQ(
            FSEC_OK,
            FslibWrapper::Copy(mSecondaryPath + "/" + versionFileName, mPrimaryPath + "/" + versionFileName).Code());
    }
    for (segmentid_t segmentId : segmentIds) {
        string segmentFileName = Version().GetNewSegmentDirName(segmentId);
        auto ec = FslibWrapper::SymLink(mSecondaryPath + "/" + segmentFileName, mPrimaryPath + "/" + segmentFileName);
        (void)ec;
    }
}

void LocalIndexCleanerTest::CheckOnlineVersions(const vector<versionid_t>& versionIds,
                                                const vector<segmentid_t>& segmentIds,
                                                const vector<std::string>& otherExpectFiles)
{
    fslib::FileList actualFiles;
    FslibWrapper::ListDirE(mPrimaryPath, actualFiles);

    vector<string> expectFiles = otherExpectFiles;
    for (versionid_t versionId : versionIds) {
        expectFiles.push_back(Version::GetVersionFileName(versionId));
    }
    for (segmentid_t segmentId : segmentIds) {
        expectFiles.push_back(Version().GetNewSegmentDirName(segmentId));
    }
    EXPECT_THAT(actualFiles, testing::UnorderedElementsAreArray(expectFiles));
};

void LocalIndexCleanerTest::Clean(const map<versionid_t, Version>& versions, const vector<versionid_t>& keepVersionIds,
                                  const vector<versionid_t>& readerContainerHoldVersionIds, uint32_t keepVersionCount)
{
    vector<Version> readerVersions;
    for (versionid_t versionId : readerContainerHoldVersionIds) {
        readerVersions.emplace_back(versions.at(versionId));
    }
    ReaderContainerPtr readerContainer(new MockReaderContainer(readerVersions));

    LocalIndexCleaner cleaner(mPrimaryPath, mSecondaryPath, keepVersionCount, readerContainer);
    cleaner.Clean(keepVersionIds);
};

void LocalIndexCleanerTest::ResetOnlineDirectory()
{
    if (FslibWrapper::IsExist(mPrimaryPath).GetOrThrow()) {
        FslibWrapper::DeleteDirE(mPrimaryPath, DeleteOption::NoFence(false));
    }
    FslibWrapper::MkDirE(mPrimaryPath);
};

void LocalIndexCleanerTest::TestSimpleProcess()
{
    auto versions = PartitionDataMaker::MakeVersions(mSecondaryDir, "1:0,1");
    for (auto keepVersionCount : vector<uint32_t>({0, 1, 9, INVALID_KEEP_VERSION_COUNT})) {
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
    for (auto keepVersionCount : vector<uint32_t>({0, 1, 9, INVALID_KEEP_VERSION_COUNT})) {
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
    auto versions =
        PartitionDataMaker::MakeVersions(mSecondaryDir, "0:0; 1:0,1; 2:1,2; 3:2,3; 4:3,4; 5:4,5; 6:5,6; 7:6,7");

    FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    auto fs = FileSystemCreator::Create("LocalIndexCleanerTest", mPrimaryPath, fsOptions).GetOrThrow();
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
    CheckOnlineVersions({}, {0, 1});

    // open
    Clean(versions, {1}, {1}, keepVersionCount);
    CheckOnlineVersions({}, {0, 1});

    // deploying version 2
    DpToOnline({}, {2});
    Clean(versions, {1, 2}, {1}, keepVersionCount);
    CheckOnlineVersions({}, {0, 1, 2});

    // deploying version 4
    DpToOnline({}, {3});
    Clean(versions, {1, 4}, {1}, keepVersionCount);
    CheckOnlineVersions({}, {0, 1, 3});

    DpToOnline({}, {4});
    Clean(versions, {1, 4}, {1}, keepVersionCount);
    CheckOnlineVersions({}, {0, 1, 3, 4});

    // reoprn version 1->4
    Clean(versions, {4}, {1}, keepVersionCount);
    CheckOnlineVersions({}, {0, 1, 3, 4});

    Clean(versions, {4}, {1, 4}, keepVersionCount);
    CheckOnlineVersions({}, {0, 1, 3, 4});

    Clean(versions, {4}, {4}, keepVersionCount);
    CheckOnlineVersions({}, {3, 4});

    // dp version 5 6 7
    keepVersionCount = 3;

    DpToOnline({5, 6, 7}, {4, 5, 6, 7});
    Clean(versions, {7}, {7}, keepVersionCount);
    CheckOnlineVersions({5, 6, 7}, {4, 5, 6, 7});
}

void LocalIndexCleanerTest::TestCleanUnreferencedSegments()
{
    auto version = VersionMaker::Make(mPrimaryDir, 1, "1,2,3");
    LocalIndexCleaner cleaner(mPrimaryPath, mSecondaryPath, 1, ReaderContainerPtr());

    // test clean invalid_version
    ASSERT_TRUE(cleaner.CleanUnreferencedIndexFiles(Version(-1), {}));
    // test clean empty_version
    ASSERT_TRUE(cleaner.CleanUnreferencedIndexFiles(Version(2), {}));

    std::set<std::string> toKeepFiles;
    toKeepFiles.insert("segment_1_level_0/deploy_index");
    toKeepFiles.insert("segment_2_level_0/deploy_index");

    ASSERT_TRUE(cleaner.CleanUnreferencedSegments(version, toKeepFiles));

    std::string seg3DirPath = FslibWrapper::JoinPath(mPrimaryPath, "segment_3_level_0");
    std::string seg2DirPath = FslibWrapper::JoinPath(mPrimaryPath, "segment_2_level_0");
    std::string seg1DirPath = FslibWrapper::JoinPath(mPrimaryPath, "segment_1_level_0");
    std::string seg0DirPath = FslibWrapper::JoinPath(mPrimaryPath, "segment_0_level_0");

    ASSERT_TRUE(FslibWrapper::MkDirIfNotExist(FslibWrapper::JoinPath(mPrimaryPath, "segment_0_level_0")).OK());

    ASSERT_TRUE(FslibWrapper::IsExist(seg0DirPath).GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(seg1DirPath).GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(seg2DirPath).GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(seg3DirPath).GetOrThrow());
}

void LocalIndexCleanerTest::TestCleanUnreferencedLocalFiles()
{
    auto version = VersionMaker::Make(mPrimaryDir, 1, "1,2,3");
    LocalIndexCleaner cleaner(mPrimaryPath, mSecondaryPath, 1, ReaderContainerPtr());
    std::set<std::string> toKeepFiles;
    std::set<std::string> toRemoveFiles;
    for (size_t segmentId = 1; segmentId <= 3; segmentId++) {
        std::string segDirName = "segment_" + std::to_string(segmentId) + "_level_0";
        toKeepFiles.insert(segDirName + "/segment_info");
        toKeepFiles.insert(segDirName + "/deploy_index");
        toRemoveFiles.insert(segDirName + "/counter");
        toRemoveFiles.insert(segDirName + "/segment_file_list");
    }

    // test directory will be recursively cleaned

    ASSERT_TRUE(FslibWrapper::MkDir(FslibWrapper::JoinPath(mPrimaryPath, "segment_3_level_0/index/"), true).OK());
    ASSERT_TRUE(
        FslibWrapper::Store(FslibWrapper::JoinPath(mPrimaryPath, "segment_3_level_0/index/index1"), "someindex").OK());
    ASSERT_TRUE(
        FslibWrapper::MkDir(FslibWrapper::JoinPath(mPrimaryPath, "segment_3_level_0/attribute/subdir/"), true).OK());
    ASSERT_TRUE(
        FslibWrapper::Store(FslibWrapper::JoinPath(mPrimaryPath, "segment_3_level_0/attribute/attr1"), "someattribute")
            .OK());
    ASSERT_TRUE(FslibWrapper::Store(FslibWrapper::JoinPath(mPrimaryPath, "segment_3_level_0/attribute/subdir/attr2"),
                                    "someattribute")
                    .OK());
    toRemoveFiles.insert("segment_3_level_0/index/index1");
    toRemoveFiles.insert("segment_3_level_0/attribute/attr1");
    toRemoveFiles.insert("segment_3_level_0/attribute/subdir/attr2");

    cleaner.CleanUnreferencedLocalFiles(version, toKeepFiles);

    // test sub directories in segment will not be cleaned
    toKeepFiles.insert("segment_3_level_0/index/");
    toKeepFiles.insert("segment_3_level_0/attribute/subdir");
    CheckFileState(toKeepFiles, toRemoveFiles);
}

void LocalIndexCleanerTest::TestFslibWrapperListDir()
{
    auto version = VersionMaker::Make(mPrimaryDir, 1, "1,2,3");
    fslib::FileList fileList;
    auto ret = FslibWrapper::ListDirRecursive(mPrimaryPath, fileList).Code();
    ASSERT_EQ(FSEC_OK, ret);
    for (const auto& name : fileList) {
        auto filePath = FslibWrapper::JoinPath(mPrimaryPath, name);
        auto [ec, isDir] = FslibWrapper::IsDir(filePath);
        ASSERT_EQ(FSEC_OK, ec);
        if (isDir) {
            ASSERT_EQ('/', name.back());
        }
    }
}

void LocalIndexCleanerTest::TestCleanWithKeepFiles()
{
    vector<Version> readerVersions;
    auto version0 = VersionMaker::Make(mPrimaryDir, 0, "0");
    auto version1 = VersionMaker::Make(mPrimaryDir, 1, "1,2");
    auto version2 = VersionMaker::Make(mPrimaryDir, 2, "1,3");
    auto version3 = VersionMaker::Make(mPrimaryDir, 3, "1,3,4");
    auto version4 = VersionMaker::Make(mPrimaryDir, 4, "1,3,4,5");
    readerVersions.push_back(version1);
    readerVersions.push_back(version2);
    readerVersions.push_back(version3);

    MockReaderContainerPtr fakeContainer(new MockReaderContainer(readerVersions));
    fakeContainer->SetWhiteList({"segment_1_level_0/segment_info", "segment_1_level_0/deploy_index",
                                 "segment_2_level_0/segment_info", "segment_2_level_0/deploy_index",
                                 "segment_3_level_0/segment_info", "segment_3_level_0/deploy_index",
                                 "segment_3_level_0/counter", "segment_3_level_0/segment_file_list"});
    LocalIndexCleaner cleaner(mPrimaryPath, mSecondaryPath, 1, fakeContainer);

    std::set<string> whiteList = {};
    // test empty whiteList
    ASSERT_TRUE(cleaner.CleanUnreferencedIndexFiles(version2, whiteList));

    CheckFileState(
        {"segment_0_level_0/segment_info", "segment_0_level_0/deploy_index", "segment_0_level_0/counter",
         "segment_0_level_0/segment_file_list", "segment_1_level_0/segment_info", "segment_1_level_0/deploy_index",
         "segment_2_level_0/segment_info", "segment_2_level_0/deploy_index", "segment_2_level_0/counter",
         "segment_2_level_0/segment_file_list", "segment_3_level_0/segment_info", "segment_3_level_0/deploy_index",
         "segment_3_level_0/counter", "segment_3_level_0/segment_file_list", "segment_4_level_0/segment_info",
         "segment_4_level_0/deploy_index", "segment_4_level_0/counter", "segment_4_level_0/segment_file_list"},
        {
            "segment_1_level_0/counter",
            "segment_1_level_0/segment_file_list",
        });

    // test passing whiteList, will be merged with container's whitelist
    fakeContainer->SetWhiteList({"segment_1_level_0/segment_info"});
    whiteList = {
        "segment_4_level_0/segment_info",
        "segment_4_level_0/segment_file_list",
    };
    ASSERT_TRUE(cleaner.CleanUnreferencedIndexFiles(version3, whiteList));

    CheckFileState(
        {
            "segment_0_level_0/segment_info",
            "segment_0_level_0/deploy_index",
            "segment_0_level_0/counter",
            "segment_0_level_0/segment_file_list",
            "segment_1_level_0/segment_info",
            "segment_2_level_0/segment_info",
            "segment_2_level_0/deploy_index",
            "segment_2_level_0/counter",
            "segment_2_level_0/segment_file_list",
            "segment_4_level_0/segment_info",
            "segment_4_level_0/segment_file_list",

        },
        {"segment_1_level_0/counter", "segment_1_level_0/segment_file_list", "segment_3_level_0",
         "segment_4_level_0/counter", "segment_4_level_0/deploy_index"});
}
/*
void LocalIndexCleanerTest::TestRollbackVersion()
{
    auto versions = PartitionDataMaker::MakeVersions(mSecondaryDir,
            "0:0; 1:0,1; 2:1,2; 3:0,1;");
    auto fs = FileSystemCreator::Create(mPrimaryPath, mSecondaryPath).GetOrThrow();
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
    auto versions = PartitionDataMaker::MakeVersions(mSecondaryDir, "0:0; 1:0,1; 2:1,2; 3:2,3;
4:3,4"); DpToOnline({0, 1, 2, 3}, {0, 1, 2, 3});

    auto fs = FileSystemCreator::Create(mPrimaryPath, mSecondaryPath).GetOrThrow();
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
}} // namespace indexlib::partition
