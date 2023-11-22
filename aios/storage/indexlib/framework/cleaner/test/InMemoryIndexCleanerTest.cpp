#include "indexlib/framework/cleaner/InMemoryIndexCleaner.h"

#include <string>

#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletReaderContainer.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/test/FakeTabletReader.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::framework {

class MockSegment : public Segment
{
public:
    MockSegment(const SegmentMeta& meta) : Segment(SegmentStatus::ST_BUILT) { SetSegmentMeta(meta); }
    ~MockSegment() = default;
    size_t EvaluateCurrentMemUsed() override
    {
        // TODO(xiuchen) impl
        return 0;
    }
    std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override
    {
        return {Status::OK(), 0};
    }
};

class InMemoryIndexCleanerTest : public TESTBASE
{
public:
    InMemoryIndexCleanerTest() {}
    ~InMemoryIndexCleanerTest() {}

    void setUp() override
    {
        indexlib::file_system::FileSystemOptions fsOptions;
        std::string rootPath = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();
        _rootDir = indexlib::file_system::Directory::Get(fs);
    }
    void tearDown() override {}

private:
    void MakeVersion(versionid_t vid, const std::vector<segmentid_t>& segIds);
    void StoreVersion(const std::string& fileName, const std::string& content);
    void CreateDirectory(const std::string& dir) { _rootDir->MakeDirectory(dir); }
    void CreateFile(const std::string& fileName, const std::string& content = std::string(""));

private:
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
    const std::string _tabletName = {"tablet_ut"};
};

void InMemoryIndexCleanerTest::CreateFile(const std::string& fileName, const std::string& content)
{
    _rootDir->Store(fileName, content);
}

void InMemoryIndexCleanerTest::MakeVersion(versionid_t vid, const std::vector<segmentid_t>& segIds)
{
    Version version(vid);
    for (const auto& segId : segIds) {
        version.AddSegment(segId);
        _rootDir->MakeDirectory(PathUtil::NewSegmentDirName(segId, 0));
    }
    indexlib::file_system::WriterOption writerOption;
    writerOption.atomicDump = true;
    _rootDir->Store(version.GetVersionFileName(), version.ToString(), writerOption);
    _rootDir->Store(indexlib::file_system::EntryTable::GetEntryTableFileName(vid), "", writerOption);
    _rootDir->Sync(true);
}

TEST_F(InMemoryIndexCleanerTest, CleanWithNoSegmentRemoved)
{
    MakeVersion(0 /* vid */, {0} /* segIds */);
    MakeVersion(1 /* vid */, {0, 1} /* segIds */);
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    InMemoryIndexCleaner cleaner(_tabletName, tabletReaderContainer.get(), _rootDir, /*mutex*/ nullptr,
                                 /*cleanPhysicalFiles*/ true);
    ASSERT_TRUE(cleaner.Clean().IsOK());

    ASSERT_TRUE(_rootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(_rootDir->IsExist("segment_1_level_0"));
    ASSERT_FALSE(_rootDir->IsExist("version.0"));
    ASSERT_TRUE(_rootDir->IsExist("version.1"));
}

TEST_F(InMemoryIndexCleanerTest, CleanSegmentWithEmptyDirectoryTest)
{
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    InMemoryIndexCleaner cleaner(_tabletName, tabletReaderContainer.get(), _rootDir, nullptr,
                                 /*cleanPhysicalFiles*/ true);
    ASSERT_TRUE(cleaner.Clean().IsOK());
}

TEST_F(InMemoryIndexCleanerTest, CleanSegmentWithNotInVersionTest)
{
    MakeVersion(0 /* vid */, {0} /* segIds */);
    MakeVersion(1 /* vid */, {2} /* segIds */);

    _rootDir->MakeDirectory("segment_1_level_0");
    _rootDir->MakeDirectory("segment_3_level_0");

    // currently segIds {0, 1, 2, 3} segId=1 will be removed, segId=3 will be reserved( >= lastSegmentid)
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    InMemoryIndexCleaner cleaner(_tabletName, tabletReaderContainer.get(), _rootDir, nullptr,
                                 /*cleanPhysicalFiles*/ true);
    ASSERT_TRUE(cleaner.Clean().IsOK());

    ASSERT_FALSE(_rootDir->IsExist("segment_0_level_0"));
    ASSERT_FALSE(_rootDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(_rootDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(_rootDir->IsExist("segment_3_level_0"));
}

TEST_F(InMemoryIndexCleanerTest, CleanUnusedVersionsTest)
{
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    InMemoryIndexCleaner cleaner(_tabletName, tabletReaderContainer.get(), _rootDir, nullptr,
                                 /*cleanPhysicalFiles*/ true);
    ASSERT_TRUE(cleaner.Clean().IsOK());

    MakeVersion(0 /* vid */, {0} /* segIds */);
    ASSERT_TRUE(_rootDir->IsExist("version.0"));
    ASSERT_TRUE(cleaner.Clean().IsOK());

    MakeVersion(1 /* vid */, {2} /* segIds */);
    ASSERT_TRUE(cleaner.Clean().IsOK());
    ASSERT_FALSE(_rootDir->IsExist("version.0"));
    ASSERT_TRUE(_rootDir->IsExist("version.1"));

    for (versionid_t vid = 2; vid < 10; ++vid) {
        MakeVersion(vid /* vid */, {} /* no segIds */);
    }
    ASSERT_TRUE(cleaner.Clean().IsOK());

    for (versionid_t vid = 2; vid < 9; ++vid) {
        ASSERT_FALSE(_rootDir->IsExist(std::string("version.") + std::to_string(vid)));
    }
    ASSERT_TRUE(_rootDir->IsExist("version.9"));
}

TEST_F(InMemoryIndexCleanerTest, CleanUnusedSegmentsTestWithEmptyReaderContainer)
{
    MakeVersion(0 /* vid */, {0} /* segIds */);
    MakeVersion(1 /* vid */, {2} /* segIds */);
    MakeVersion(2 /* vid */, {2, 3} /* segIds */);

    _rootDir->MakeDirectory("segment_4_level_0");

    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    InMemoryIndexCleaner cleaner(_tabletName, tabletReaderContainer.get(), _rootDir, nullptr,
                                 /*cleanPhysicalFiles*/ true);
    ASSERT_TRUE(cleaner.Clean().IsOK());

    ASSERT_FALSE(_rootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(_rootDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(_rootDir->IsExist("segment_3_level_0"));
    ASSERT_TRUE(_rootDir->IsExist("segment_4_level_0"));
}

TEST_F(InMemoryIndexCleanerTest, CleanUnusedSegmentsTestWithReaderContainer)
{
    MakeVersion(0 /* vid */, {0} /* segIds */);
    MakeVersion(1 /* vid */, {2} /* segIds */);
    MakeVersion(2 /* vid */, {2, 3} /* segIds */);

    _rootDir->MakeDirectory("segment_4_level_0");

    auto tabletData = std::make_shared<TabletData>("");
    SegmentMeta meta(/*segId*/ 0);
    auto segment = std::make_shared<MockSegment>(meta);
    tabletData->TEST_PushSegment(segment);

    auto reader = std::make_shared<FakeTabletReader>("");
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    tabletReaderContainer->AddTabletReader(tabletData, reader, /*version deploy description*/ nullptr);
    InMemoryIndexCleaner cleaner(_tabletName, tabletReaderContainer.get(), _rootDir, nullptr,
                                 /*cleanPhysicalFiles*/ true);
    ASSERT_TRUE(cleaner.Clean().IsOK());

    ASSERT_TRUE(_rootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(_rootDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(_rootDir->IsExist("segment_3_level_0"));
    ASSERT_TRUE(_rootDir->IsExist("segment_4_level_0"));
}

TEST_F(InMemoryIndexCleanerTest, TestCleanPrivateVersion)
{
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    InMemoryIndexCleaner cleaner(_tabletName, tabletReaderContainer.get(), _rootDir, nullptr,
                                 /*cleanPhysicalFiles*/ true);
    for (size_t i = 0; i < 3; ++i) {
        std::string versionFile = "version.";
        versionFile += std::to_string(Version::PRIVATE_VERSION_ID_MASK | i);
        MakeVersion(Version::PRIVATE_VERSION_ID_MASK | i /* vid */, {int(i)} /* segIds */);
        ASSERT_TRUE(_rootDir->IsExist(versionFile));
    }
    auto checkExist = [this](const std::vector<versionid_t>& existVersions,
                             const std::vector<versionid_t>& notExistVersions) {
        std::cerr << "check begin" << std::endl;
        for (auto version : existVersions) {
            auto vid = Version::PRIVATE_VERSION_ID_MASK | version;
            std::string versionFile = "version.";
            versionFile += std::to_string(vid);
            auto entryTableFile = indexlib::file_system::EntryTable::GetEntryTableFileName(vid);
            EXPECT_TRUE(_rootDir->IsExist(versionFile)) << versionFile << " expect exist but not";
            EXPECT_TRUE(_rootDir->IsExist(entryTableFile)) << entryTableFile << " expect exist but not";
        }
        for (auto version : notExistVersions) {
            auto vid = Version::PRIVATE_VERSION_ID_MASK | version;
            std::string versionFile = "version.";
            versionFile += std::to_string(vid);
            auto entryTableFile = indexlib::file_system::EntryTable::GetEntryTableFileName(vid);
            EXPECT_FALSE(_rootDir->IsExist(versionFile)) << versionFile << " not expect exist but exist";
            EXPECT_FALSE(_rootDir->IsExist(entryTableFile)) << entryTableFile << " not exist but exist";
        }
    };
    auto addReader = [&](versionid_t versionId) {
        auto tabletData = std::make_shared<TabletData>("");
        Version version(versionId | Version::PRIVATE_VERSION_ID_MASK);
        tabletData->TEST_SetOnDiskVersion(version);
        tabletReaderContainer->AddTabletReader(tabletData, nullptr, nullptr);
    };
    ASSERT_TRUE(cleaner.Clean().IsOK());
    checkExist({0, 1, 2}, {});

    addReader(0);
    ASSERT_TRUE(cleaner.Clean().IsOK());
    checkExist({0, 1, 2}, {});
    tabletReaderContainer->EvictOldestTabletReader();
    ASSERT_TRUE(cleaner.Clean().IsOK());
    checkExist({1, 2}, {0});

    addReader(1);
    ASSERT_TRUE(cleaner.Clean().IsOK());
    checkExist({1, 2}, {0});
    tabletReaderContainer->EvictOldestTabletReader();
    ASSERT_TRUE(cleaner.Clean().IsOK());
    checkExist({2}, {0, 1});

    addReader(2);
    ASSERT_TRUE(cleaner.Clean().IsOK());
    checkExist({2}, {0, 1});
    tabletReaderContainer->EvictOldestTabletReader();
    ASSERT_TRUE(cleaner.Clean().IsOK());
    // will keep at least one version
    checkExist({2}, {0, 1});
}
} // namespace indexlibv2::framework
