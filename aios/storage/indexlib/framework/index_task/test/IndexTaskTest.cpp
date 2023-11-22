#include <string>

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/TabletLoader.h"
#include "indexlib/framework/TabletReader.h"
#include "indexlib/framework/TabletWriter.h"
#include "indexlib/framework/VersionCommitter.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/framework/index_task/IndexTaskContextCreator.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/framework/index_task/test/FakeResource.h"
#include "indexlib/framework/mock/MockTabletFactory.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class IndexTaskTest : public TESTBASE
{
public:
    IndexTaskTest() {}
    ~IndexTaskTest() {}

    void setUp() override { _testRoot = GET_TEMP_DATA_PATH(); }
    void tearDown() override {}
    void MakeSegment(const std::string& relativePath, segmentid_t segmentId);
    void MakeVersion(const std::string& relativePath, versionid_t versionId, const std::vector<segmentid_t>& segments,
                     const std::string& fenceName, int64_t versionTs = -1, int64_t versionLocator = -1,
                     bool sealed = false);

private:
    std::string _testRoot;
};

void IndexTaskTest::MakeSegment(const std::string& relativePath, segmentid_t segmentId)
{
    indexlib::file_system::IFileSystemPtr fs =
        indexlib::file_system::FileSystemCreator::Create("test", _testRoot).GetOrThrow();
    indexlib::file_system::DirectoryPtr rootDir = indexlib::file_system::Directory::Get(fs);
    auto dir = rootDir->MakeDirectory(relativePath);

    assert(dir);
    auto segDirName = std::string("segment_") + std::to_string(segmentId) + "_level_0";
    auto segDir = dir->MakeDirectory(segDirName);
    assert(segDir);
    segDir->Store(/*name*/ "data", /*fileContent*/ "");
    SegmentInfo segInfo;
    [[maybe_unused]] auto status = segInfo.Store(segDir->GetIDirectory());
    assert(status.IsOK());
}

void IndexTaskTest::MakeVersion(const std::string& relativePath, versionid_t versionId,
                                const std::vector<segmentid_t>& segments, const std::string& fenceName,
                                int64_t versionTs, int64_t versionLocator, bool sealed)
{
    auto fenceRoot = PathUtil::JoinPath(_testRoot, relativePath, fenceName);
    indexlib::file_system::IFileSystemPtr fs =
        indexlib::file_system::FileSystemCreator::Create("test", fenceRoot).GetOrThrow();

    auto status =
        indexlib::file_system::toStatus(fs->MountDir(fenceRoot, "", "", indexlib::file_system::FSMT_READ_WRITE, false));
    ASSERT_TRUE(status.IsOK());
    auto dir = indexlib::file_system::Directory::Get(fs);
    assert(dir);
    Version version;
    for (auto segId : segments) {
        version.AddSegment(segId);
    }
    version.SetVersionId(versionId);
    version.SetFenceName(fenceName);
    version.SetTimestamp(versionTs);
    Locator locator(0, versionLocator);
    version.SetLocator(locator);
    if (sealed) {
        version.SetSealed();
    }
    status = VersionCommitter::Commit(version, dir, {});
    ASSERT_TRUE(status.IsOK());
}

namespace {

class FakeDiskSegment : public DiskSegment
{
public:
    FakeDiskSegment(const SegmentMeta& segmentMeta) : DiskSegment(segmentMeta) {}
    Status Open(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                DiskSegment::OpenMode mode) override
    {
        if (mode != DiskSegment::OpenMode::LAZY) {
            return Status::Corruption();
        }
        return _segmentMeta.segmentDir->IsExist("data") ? Status::OK() : Status::Corruption();
    }
    Status Reopen(const std::vector<std::shared_ptr<config::ITabletSchema>>& schema) override
    {
        assert(false);
        return Status::OK();
    }
    std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override
    {
        return {Status::OK(), 0};
    }
    size_t EvaluateCurrentMemUsed() override { return 0; }
};

} // namespace

TEST_F(IndexTaskTest, testOnlyMergedSegments)
{
    MakeSegment("", Segment::MERGED_SEGMENT_ID_MASK | 2);
    MakeSegment("", Segment::MERGED_SEGMENT_ID_MASK | 3);
    MakeVersion("", Version::MERGED_VERSION_ID_MASK | 101,
                {Segment::MERGED_SEGMENT_ID_MASK | 2, Segment::MERGED_SEGMENT_ID_MASK | 3}, "");

    MockTabletFactory factory;
    EXPECT_CALL(factory, CreateDiskSegment(_, _))
        .WillRepeatedly(Invoke([](const SegmentMeta& segmentMeta, const framework::BuildResource&) {
            return std::make_unique<FakeDiskSegment>(segmentMeta);
        }));
    auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
    auto tabletSchema = std::make_shared<indexlibv2::config::TabletSchema>();
    auto context = IndexTaskContextCreator()
                       .SetTabletName("test")
                       .SetTaskEpochId("task0")
                       .SetExecuteEpochId("work")
                       .SetTabletFactory(&factory)
                       .AddSourceVersion(_testRoot, Version::MERGED_VERSION_ID_MASK | 101)
                       .SetDestDirectory(_testRoot)
                       .SetTabletOptions(tabletOptions)
                       .SetTabletSchema(tabletSchema)
                       .CreateContext();
    ASSERT_TRUE(context);
    ASSERT_TRUE(context->GetTabletData());

    ASSERT_EQ(Segment::MERGED_SEGMENT_ID_MASK | 3, context->GetMaxMergedSegmentId());
    ASSERT_EQ(Version::MERGED_VERSION_ID_MASK | 101, context->GetMaxMergedVersionId());
}

TEST_F(IndexTaskTest, testCreateSimpleContext)
{
    MakeSegment("", 0);
    MakeSegment("", 1);
    MakeVersion("", 100, {0, 1}, "");
    MockTabletFactory factory;
    EXPECT_CALL(factory, CreateDiskSegment(_, _))
        .WillRepeatedly(Invoke([](const SegmentMeta& segmentMeta, const framework::BuildResource&) {
            return std::make_unique<FakeDiskSegment>(segmentMeta);
        }));

    auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
    auto tabletSchema = std::make_shared<indexlibv2::config::TabletSchema>();
    auto provider = std::make_shared<indexlib::util::MetricProvider>();
    auto context = IndexTaskContextCreator()
                       .SetTabletName("test")
                       .SetTaskEpochId("task0")
                       .SetExecuteEpochId("work")
                       .SetTabletFactory(&factory)
                       .AddSourceVersion(_testRoot, 100)
                       .SetDestDirectory(_testRoot)
                       .SetTabletOptions(tabletOptions)
                       .SetTabletSchema(tabletSchema)
                       .SetMetricProvider(provider)
                       .CreateContext();
    ASSERT_TRUE(context);
    auto tabletName = context->GetTabletData()->GetTabletName();
    ASSERT_EQ("test", tabletName);
    ASSERT_TRUE(context->GetTabletOptions());
    ASSERT_TRUE(context->GetTabletSchema());
    ASSERT_TRUE(context->GetMetricProvider());
    ASSERT_TRUE(context->GetTabletData());

    ASSERT_EQ(1, context->GetMaxMergedSegmentId());
    ASSERT_EQ(100, context->GetMaxMergedVersionId());

    auto version = context->GetTabletData()->GetOnDiskVersion();
    ASSERT_EQ(100, version.GetVersionId());
    ASSERT_EQ(2u, version.GetSegmentCount());
    ASSERT_EQ(0, version[0]);
    ASSERT_EQ(1, version[1]);

    auto slice = context->GetTabletData()->CreateSlice();
    ASSERT_EQ(2u, slice.end() - slice.begin());
    auto iter = slice.begin();
    EXPECT_EQ(0, (*iter)->GetSegmentId());
    ++iter;
    EXPECT_EQ(1, (*iter)->GetSegmentId());
}

TEST_F(IndexTaskTest, testCreateContextWithNoSpecifiedSource)
{
    MakeSegment("", 0);
    MakeSegment("", 1);
    MakeSegment("", 2);

    MakeVersion("", 100, {0, 1}, "", /*versionTs*/ 100, /*versionLocator*/ 99, /*sealed*/ false);
    MakeVersion("", 101, {0, 1, 2}, "", /*versionTs*/ 90, /*versionLocator*/ 100, /*sealed*/ true);
    MockTabletFactory factory;
    EXPECT_CALL(factory, CreateDiskSegment(_, _))
        .WillRepeatedly(Invoke([](const SegmentMeta& segmentMeta, const framework::BuildResource&) {
            return std::make_unique<FakeDiskSegment>(segmentMeta);
        }));
    auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
    auto tabletSchema = std::make_shared<indexlibv2::config::TabletSchema>();
    auto context = IndexTaskContextCreator()
                       .SetTabletName("test")
                       .SetTaskEpochId("task0")
                       .SetExecuteEpochId("work")
                       .SetTabletFactory(&factory)
                       .SetDestDirectory(_testRoot)
                       .SetTabletOptions(tabletOptions)
                       .SetTabletSchema(tabletSchema)
                       .CreateContext();
    ASSERT_TRUE(context);
    ASSERT_TRUE(context->GetTabletData());

    auto version = context->GetTabletData()->GetOnDiskVersion();
    ASSERT_EQ(101, version.GetVersionId());
    ASSERT_EQ(3u, version.GetSegmentCount());
    ASSERT_EQ(0, version[0]);
    ASSERT_EQ(1, version[1]);
    ASSERT_EQ(2, version[2]);
}

TEST_F(IndexTaskTest, testDefaultLoadConfig)
{
    MakeSegment("", 0);
    MakeVersion("", 100, {0}, "");
    MockTabletFactory factory;
    EXPECT_CALL(factory, CreateDiskSegment(_, _))
        .WillRepeatedly(Invoke([](const SegmentMeta& segmentMeta, const framework::BuildResource&) {
            return std::make_unique<FakeDiskSegment>(segmentMeta);
        }));

    auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
    tabletOptions->SetIsOnline(false);
    auto tabletSchema = std::make_shared<indexlibv2::config::TabletSchema>();
    auto provider = std::make_shared<indexlib::util::MetricProvider>();
    auto context = IndexTaskContextCreator()
                       .SetTabletName("test")
                       .SetTaskEpochId("task0")
                       .SetExecuteEpochId("work")
                       .SetTabletFactory(&factory)
                       .AddSourceVersion(_testRoot, 100)
                       .SetDestDirectory(_testRoot)
                       .SetTabletOptions(tabletOptions)
                       .SetTabletSchema(tabletSchema)
                       .SetMetricProvider(provider)
                       .CreateContext();
    ASSERT_TRUE(context);

    auto slice = context->GetTabletData()->CreateSlice();
    ASSERT_EQ(1u, slice.end() - slice.begin());
    auto iter = slice.begin();
    auto dir = (*iter)->GetSegmentDirectory();
    ASSERT_TRUE(dir);
    auto readOption = indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_LOAD_CONFIG);
    auto fileReader = dir->CreateFileReader(/*path*/ "data", readOption);
    ASSERT_TRUE(fileReader);
    ASSERT_EQ(indexlib::file_system::FSFT_BLOCK, fileReader->GetType());
}
} // namespace indexlibv2::framework
