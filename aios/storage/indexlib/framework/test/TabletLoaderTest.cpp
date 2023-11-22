#include "indexlib/framework/TabletLoader.h"

#include "indexlib/framework/Locator.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/framework/mock/MockDiskSegment.h"
#include "indexlib/framework/mock/MockTabletLoader.h"
#include "indexlib/framework/mock/MockTabletSchema.h"
#include "indexlib/util/memory_control/MemoryReserver.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class TabletLoaderTest : public TESTBASE
{
public:
    TabletLoaderTest() = default;
    ~TabletLoaderTest() = default;

    void setUp() override { _schema = std::make_shared<MockTabletSchema>(); }
    void tearDown() override {}

private:
    std::shared_ptr<MockTabletSchema> _schema;
};

namespace {
auto createMemoryReserver(const std::shared_ptr<MemoryQuotaController>& memController)
{
    auto partitionMemController =
        std::make_shared<indexlib::util::PartitionMemoryQuotaController>(memController, "partition");
    auto blockMemController =
        std::make_shared<indexlib::util::BlockMemoryQuotaController>(partitionMemController, "block");
    auto memReserver = std::make_shared<indexlib::util::MemoryReserver>("reserver", blockMemController);
    return memReserver;
}
} // namespace

TEST_F(TabletLoaderTest, testPreLoad)
{
    {
        // test success
        segmentid_t segmentId = 540132;
        SegmentMeta segmentMeta(segmentId);
        auto diskSegment = std::make_shared<MockDiskSegment>(segmentMeta);
        EXPECT_CALL(*diskSegment, Open(_, _)).WillOnce(Return(Status::OK()));
        auto tabletLoader = std::make_unique<MockTabletLoader>();
        EXPECT_CALL(*tabletLoader, DoPreLoad(_, _, _)).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillOnce(Return((std::make_pair(Status::OK(), 1024))));
        auto memController = std::make_shared<MemoryQuotaController>("test", 6 * 1024 * 1024);
        auto memReserver = createMemoryReserver(memController);
        tabletLoader->Init(memController, _schema, memReserver, /*isOnline=*/true);
        Version version;
        version.AddSegment(diskSegment->GetSegmentId());
        auto status =
            tabletLoader->PreLoad(TabletData("table"), {std::make_pair(diskSegment, /*need open=*/true)}, version);
        ASSERT_TRUE(status.IsOK());
    }
    {
        // test no mem block quota
        segmentid_t segmentId = 540132;
        SegmentMeta segmentMeta(segmentId);
        auto diskSegment = std::make_shared<MockDiskSegment>(segmentMeta);
        auto tabletLoader = std::make_unique<MockTabletLoader>();
        auto calculator = [&](const std::vector<std::pair<std::shared_ptr<Segment>, /*need Open*/ bool>>& segmentPairs)
            -> std::pair<Status, size_t> {
            auto needOpenSegments = tabletLoader->TabletLoader::GetNeedOpenSegments(segmentPairs);
            return tabletLoader->TabletLoader::EstimateMemUsed(_schema, needOpenSegments);
        };
        auto needOpenSegmentPair = {
            std::make_pair(std::dynamic_pointer_cast<Segment>(diskSegment), /*need open*/ true)};
        auto needNotOpenSegmentPair = {
            std::make_pair(std::dynamic_pointer_cast<Segment>(diskSegment), /*need't open*/ false)};
        EXPECT_CALL(*diskSegment, EstimateMemUsed(_))
            .WillRepeatedly(Return(std::make_pair(Status::OK(), 2 * 1024 * 1024)));
        EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _))
            .WillOnce(Return(calculator(needOpenSegmentPair)))
            .WillOnce(Return(calculator(needNotOpenSegmentPair)));
        // block size 4 * 1024 * 1024
        auto memController = std::make_shared<MemoryQuotaController>("test", 2 * 1024 * 1024);
        auto memReserver = createMemoryReserver(memController);
        tabletLoader->Init(memController, _schema, memReserver, /*isOnline=*/true);
        Version version;
        version.AddSegment(diskSegment->GetSegmentId());
        auto status = tabletLoader->PreLoad(TabletData("table"), needOpenSegmentPair, version);
        ASSERT_TRUE(status.IsNoMem());
        status = tabletLoader->PreLoad(TabletData("table"), needNotOpenSegmentPair, version);
        ASSERT_TRUE(status.IsOK());
    }
    {
        // test offline build don't estimate memory
        segmentid_t segmentId = 540132;
        SegmentMeta segmentMeta(segmentId);
        auto diskSegment = std::make_shared<MockDiskSegment>(segmentMeta);
        auto tabletLoader = std::make_unique<MockTabletLoader>();
        auto needNotOpenSegmentPair = {
            std::make_pair(std::dynamic_pointer_cast<Segment>(diskSegment), /*need't open*/ false)};
        // offline mode will not call estimateMemUsed
        EXPECT_CALL(*diskSegment, EstimateMemUsed(_)).Times(0);
        EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _)).Times(0);
        auto memController = std::make_shared<MemoryQuotaController>("test", 2 * 1024 * 1024);
        auto memReserver = createMemoryReserver(memController);
        Version version;
        version.AddSegment(diskSegment->GetSegmentId());
        tabletLoader->Init(memController, _schema, memReserver, /*isOnline=*/false);
        auto status = tabletLoader->PreLoad(TabletData("table"), needNotOpenSegmentPair, version);
        ASSERT_TRUE(status.IsOK());
    }
    {
        // test built segment open failed
        segmentid_t segmentId = 540132;
        SegmentMeta segmentMeta(segmentId);
        auto diskSegment = std::make_shared<MockDiskSegment>(segmentMeta);
        EXPECT_CALL(*diskSegment, Open(_, _)).WillOnce(Return(Status::Corruption("open failed")));
        auto tabletLoader = std::make_unique<MockTabletLoader>();
        EXPECT_CALL(*tabletLoader, EstimateMemUsed(_, _)).WillOnce(Return(std::make_pair(Status::OK(), 1024)));
        auto memController = std::make_shared<MemoryQuotaController>("test", 6 * 1024 * 1024);
        auto memReserver = createMemoryReserver(memController);
        tabletLoader->Init(memController, _schema, memReserver, /*isOnline=*/true);
        Version version;
        version.AddSegment(diskSegment->GetSegmentId());
        auto status =
            tabletLoader->PreLoad(TabletData("table"), {std::make_pair(diskSegment, /*need open*/ true)}, version);
        ASSERT_TRUE(status.IsCorruption());
    }
}

TEST_F(TabletLoaderTest, testSimple)
{
    segmentid_t segmentId = 540132;
    SegmentMeta segmentMeta(segmentId);
    auto diskSegment = std::make_shared<MockDiskSegment>(segmentMeta);
    EXPECT_CALL(*diskSegment, EvaluateCurrentMemUsed()).WillOnce(Return(1024));
    auto tabletLoader = std::make_unique<MockTabletLoader>();
    ASSERT_TRUE(tabletLoader->EvaluateCurrentMemUsed({diskSegment.get()}) == 1024);
}

TEST_F(TabletLoaderTest, testRemainFollowerSegment)
{
    auto tabletLoader = std::make_unique<MockTabletLoader>();

    auto mergeSegment0 =
        std::make_shared<FakeSegment>(0 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto mergeSegment1 =
        std::make_shared<FakeSegment>(1 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto publicSegment0 = std::make_shared<FakeSegment>(0 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                                        Segment::SegmentStatus::ST_BUILT, Locator(0, 1800));
    auto publicSegment1 =
        std::make_shared<FakeSegment>(1 | Segment::PUBLIC_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto privateSegment0 = std::make_shared<FakeSegment>(0 | Segment::PRIVATE_SEGMENT_ID_MASK,
                                                         Segment::SegmentStatus::ST_BUILT, Locator(0, 1900));
    auto privateSegment1 = std::make_shared<FakeSegment>(1 | Segment::PRIVATE_SEGMENT_ID_MASK,
                                                         Segment::SegmentStatus::ST_BUILDING, Locator(0, 2000));

    auto privateTableData = TabletData("private");
    privateTableData.TEST_PushSegment(mergeSegment0);
    privateTableData.TEST_PushSegment(mergeSegment1);
    privateTableData.TEST_PushSegment(publicSegment0);
    privateTableData.TEST_PushSegment(publicSegment1);
    privateTableData.TEST_PushSegment(privateSegment0);
    privateTableData.TEST_PushSegment(privateSegment1);

    std::vector<std::shared_ptr<Segment>> versionSegments;
    versionSegments.push_back(
        std::make_shared<FakeSegment>(2 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT));
    versionSegments.push_back(
        std::make_shared<FakeSegment>(1 | Segment::PUBLIC_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT));

    Version version;
    version.SetLocator(Locator(0, 1800));
    auto [status, segments] = tabletLoader->GetRemainSegments(privateTableData, versionSegments, version);
    ASSERT_TRUE(status.IsOK());
    std::vector<segmentid_t> segmentIds = {2 | Segment::MERGED_SEGMENT_ID_MASK, 1 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                           0 | Segment::PRIVATE_SEGMENT_ID_MASK, 1 | Segment::PRIVATE_SEGMENT_ID_MASK};
    ASSERT_EQ(segmentIds.size(), segmentIds.size());
    for (size_t i = 0; i < segmentIds.size(); i++) {
        ASSERT_EQ(segmentIds[i], segments[i]->GetSegmentId());
    }
}

TEST_F(TabletLoaderTest, testRemainLeaderSegment)
{
    auto tabletLoader = std::make_unique<MockTabletLoader>();

    auto mergeSegment0 =
        std::make_shared<FakeSegment>(0 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto mergeSegment1 =
        std::make_shared<FakeSegment>(1 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto publicSegment0 = std::make_shared<FakeSegment>(0 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                                        Segment::SegmentStatus::ST_BUILT, Locator(0, 1800));
    auto publicSegment1 =
        std::make_shared<FakeSegment>(1 | Segment::PUBLIC_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);

    auto publicSegment2 = std::make_shared<FakeSegment>(2 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                                        Segment::SegmentStatus::ST_BUILT, Locator(0, 1900));

    auto privateTableData = TabletData("private");
    privateTableData.TEST_PushSegment(mergeSegment0);
    privateTableData.TEST_PushSegment(mergeSegment1);
    privateTableData.TEST_PushSegment(publicSegment0);
    privateTableData.TEST_PushSegment(publicSegment1);
    privateTableData.TEST_PushSegment(publicSegment2);

    std::vector<std::shared_ptr<Segment>> versionSegments;
    versionSegments.push_back(
        std::make_shared<FakeSegment>(2 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT));
    versionSegments.push_back(
        std::make_shared<FakeSegment>(1 | Segment::PUBLIC_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT));
    Version version;
    version.SetLocator(Locator(0, 1800));
    auto [status, segments] = tabletLoader->GetRemainSegments(privateTableData, versionSegments, version);
    ASSERT_TRUE(status.IsOK());
    std::vector<segmentid_t> segmentIds = {2 | Segment::MERGED_SEGMENT_ID_MASK, 1 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                           2 | Segment::PUBLIC_SEGMENT_ID_MASK};
    ASSERT_EQ(segmentIds.size(), segments.size());
    for (size_t i = 0; i < segmentIds.size(); i++) {
        ASSERT_EQ(segmentIds[i], segments[i]->GetSegmentId());
    }
}

TEST_F(TabletLoaderTest, testSrcChange)
{
    auto tabletLoader = std::make_unique<MockTabletLoader>();

    auto mergeSegment0 =
        std::make_shared<FakeSegment>(0 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto mergeSegment1 =
        std::make_shared<FakeSegment>(1 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto publicSegment0 = std::make_shared<FakeSegment>(0 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                                        Segment::SegmentStatus::ST_BUILT, Locator(0, 1800));
    auto publicSegment1 =
        std::make_shared<FakeSegment>(1 | Segment::PUBLIC_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);

    auto publicSegment2 = std::make_shared<FakeSegment>(2 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                                        Segment::SegmentStatus::ST_BUILT, Locator(1, 1900));

    auto privateTableData = TabletData("private");
    privateTableData.TEST_PushSegment(mergeSegment0);
    privateTableData.TEST_PushSegment(mergeSegment1);
    privateTableData.TEST_PushSegment(publicSegment0);
    privateTableData.TEST_PushSegment(publicSegment1);
    privateTableData.TEST_PushSegment(publicSegment2);

    std::vector<std::shared_ptr<Segment>> versionSegments;
    versionSegments.push_back(
        std::make_shared<FakeSegment>(2 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT));
    versionSegments.push_back(
        std::make_shared<FakeSegment>(1 | Segment::PUBLIC_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT));
    Version version;
    version.SetLocator(Locator(0, 1800));
    auto [status, segments] = tabletLoader->GetRemainSegments(privateTableData, versionSegments, version);
    ASSERT_FALSE(status.IsOK());
}

TEST_F(TabletLoaderTest, testBuildingSegmentEmpty)
{
    auto tabletLoader = std::make_unique<MockTabletLoader>();

    auto mergeSegment0 =
        std::make_shared<FakeSegment>(0 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto mergeSegment1 =
        std::make_shared<FakeSegment>(1 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto publicSegment0 = std::make_shared<FakeSegment>(0 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                                        Segment::SegmentStatus::ST_BUILT, Locator(0, 1800));
    auto publicSegment1 =
        std::make_shared<FakeSegment>(1 | Segment::PUBLIC_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);

    // empty building segment
    auto buildingSegment1 = std::make_shared<FakeSegment>(2 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                                          Segment::SegmentStatus::ST_BUILDING, Locator());

    auto privateTableData = TabletData("private");
    privateTableData.TEST_PushSegment(mergeSegment0);
    privateTableData.TEST_PushSegment(mergeSegment1);
    privateTableData.TEST_PushSegment(publicSegment0);
    privateTableData.TEST_PushSegment(publicSegment1);
    privateTableData.TEST_PushSegment(buildingSegment1);

    std::vector<std::shared_ptr<Segment>> versionSegments;
    versionSegments.push_back(
        std::make_shared<FakeSegment>(2 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT));
    versionSegments.push_back(
        std::make_shared<FakeSegment>(1 | Segment::PUBLIC_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT));
    Version version;
    version.SetLocator(Locator(0, 1800));
    auto [status, segments] = tabletLoader->GetRemainSegments(privateTableData, versionSegments, version);
    ASSERT_TRUE(status.IsOK());
    std::vector<segmentid_t> segmentIds = {2 | Segment::MERGED_SEGMENT_ID_MASK, 1 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                           2 | Segment::PUBLIC_SEGMENT_ID_MASK};
    ASSERT_EQ(segmentIds.size(), segments.size());
    for (size_t i = 0; i < segmentIds.size(); i++) {
        ASSERT_EQ(segmentIds[i], segments[i]->GetSegmentId());
    }
}

TEST_F(TabletLoaderTest, testBuildingSegmentLocatorEqualVersionLocator)
{
    auto tabletLoader = std::make_unique<MockTabletLoader>();

    auto mergeSegment0 =
        std::make_shared<FakeSegment>(0 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto mergeSegment1 =
        std::make_shared<FakeSegment>(1 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);
    auto publicSegment0 = std::make_shared<FakeSegment>(0 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                                        Segment::SegmentStatus::ST_BUILT, Locator(0, 1700));
    auto publicSegment1 =
        std::make_shared<FakeSegment>(1 | Segment::PUBLIC_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT);

    // empty building segment
    auto buildingSegment1 = std::make_shared<FakeSegment>(2 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                                          Segment::SegmentStatus::ST_BUILDING, Locator(0, 1800));

    auto privateTableData = TabletData("private");
    privateTableData.TEST_PushSegment(mergeSegment0);
    privateTableData.TEST_PushSegment(mergeSegment1);
    privateTableData.TEST_PushSegment(publicSegment0);
    privateTableData.TEST_PushSegment(publicSegment1);
    privateTableData.TEST_PushSegment(buildingSegment1);

    std::vector<std::shared_ptr<Segment>> versionSegments;
    versionSegments.push_back(
        std::make_shared<FakeSegment>(2 | Segment::MERGED_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT));
    versionSegments.push_back(
        std::make_shared<FakeSegment>(1 | Segment::PUBLIC_SEGMENT_ID_MASK, Segment::SegmentStatus::ST_BUILT));
    Version version;
    version.SetLocator(Locator(0, 1800));
    auto [status, segments] = tabletLoader->GetRemainSegments(privateTableData, versionSegments, version);
    ASSERT_TRUE(status.IsOK());
    std::vector<segmentid_t> segmentIds = {2 | Segment::MERGED_SEGMENT_ID_MASK, 1 | Segment::PUBLIC_SEGMENT_ID_MASK,
                                           2 | Segment::PUBLIC_SEGMENT_ID_MASK};
    ASSERT_EQ(segmentIds.size(), segments.size());
    for (size_t i = 0; i < segmentIds.size(); i++) {
        ASSERT_EQ(segmentIds[i], segments[i]->GetSegmentId());
    }
}

} // namespace indexlibv2::framework
