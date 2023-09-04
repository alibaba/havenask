#include "indexlib/index/deletionmap/DeletionMapMerger.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapIndexFactory.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class DeletionMapMergerTest : public TESTBASE
{
public:
    DeletionMapMergerTest() = default;
    ~DeletionMapMergerTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    std::shared_ptr<DeletionMapConfig> _config;
};

void DeletionMapMergerTest::setUp() { _config.reset(new DeletionMapConfig); }

void DeletionMapMergerTest::tearDown() {}

TEST_F(DeletionMapMergerTest, testCreateFromFactory)
{
    DeletionMapIndexFactory factory;
    auto merger = factory.CreateIndexMerger(_config);
    ASSERT_TRUE(merger);
}

TEST_F(DeletionMapMergerTest, testInit)
{
    DeletionMapMerger merger;
    std::map<std::string, std::any> params;
    ASSERT_TRUE(merger.Init(_config, params).IsOK());
}

TEST_F(DeletionMapMergerTest, testMerge)
{
    auto rootDir = indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
    indexlib::file_system::DirectoryOption option;
    //[0, 1, 2] -> [2, 3, 4]
    DeletionMapMerger merger;
    std::map<std::string, std::any> params;
    ASSERT_TRUE(merger.Init(_config, params).IsOK());
    std::vector<std::pair<segmentid_t, std::shared_ptr<DeletionMapDiskIndexer>>> indexers;

    auto dir0 = rootDir->MakeDirectory("segment_0_level_0/deletionmap", option);
    auto dir1 = rootDir->MakeDirectory("segment_1_level_0/deletionmap", option);
    auto dir2 = rootDir->MakeDirectory("segment_2_level_0/deletionmap", option);

    std::shared_ptr<DeletionMapDiskIndexer> indexer0(new DeletionMapDiskIndexer(100, 0));
    std::shared_ptr<DeletionMapDiskIndexer> indexer1(new DeletionMapDiskIndexer(100, 1));
    std::shared_ptr<DeletionMapDiskIndexer> indexer2(new DeletionMapDiskIndexer(100, 2));
    ASSERT_TRUE(indexer0->Open(_config, dir0->GetIDirectory()).IsOK());
    ASSERT_TRUE(indexer1->Open(_config, dir1->GetIDirectory()).IsOK());
    ASSERT_TRUE(indexer2->Open(_config, dir2->GetIDirectory()).IsOK());
    ASSERT_TRUE(indexer0->Delete(0).IsOK());
    ASSERT_TRUE(indexer1->Delete(1).IsOK());
    ASSERT_TRUE(indexer2->Delete(2).IsOK());
    indexers.emplace_back(0, indexer0);
    indexers.emplace_back(1, indexer1);
    indexers.emplace_back(2, indexer2);

    std::unique_ptr<indexlib::util::Bitmap> patchBitmap(new indexlib::util::Bitmap(100, false, nullptr));
    ASSERT_TRUE(patchBitmap->Set(1));
    ASSERT_TRUE(indexer2->ApplyDeletionMapPatch(patchBitmap.get()).IsOK());
    merger.CollectAllDeletionMap(indexers);
    index::IIndexMerger::SegmentMergeInfos segMergeInfos;
    std::shared_ptr<framework::Segment> segment0(
        new framework::FakeSegment(framework::Segment::SegmentStatus::ST_BUILT)),
        segment1(new framework::FakeSegment(framework::Segment::SegmentStatus::ST_BUILT));
    framework::SegmentMeta meta0(0), meta1(1);
    segment0->TEST_SetSegmentMeta(meta0);
    segment1->TEST_SetSegmentMeta(meta1);
    std::shared_ptr<framework::SegmentMeta> meta3(new framework::SegmentMeta(3)), meta4(new framework::SegmentMeta(4));
    meta3->segmentDir = rootDir->MakeDirectory("segment_3_level_0", option);
    meta4->segmentDir = rootDir->MakeDirectory("segment_4_level_0", option);

    IIndexMerger::SourceSegment sourceSegment0, sourceSegment1;
    sourceSegment0.segment = segment0;
    sourceSegment1.segment = segment1;
    segMergeInfos.srcSegments.emplace_back(sourceSegment0);
    segMergeInfos.srcSegments.emplace_back(sourceSegment1);
    segMergeInfos.targetSegments.push_back(meta3);
    segMergeInfos.targetSegments.push_back(meta4);

    std::shared_ptr<framework::IndexTaskResourceManager> taskResourceManager;
    ASSERT_TRUE(merger.Merge(segMergeInfos, taskResourceManager).IsOK());
    auto outputDir = rootDir->GetDirectory("segment_4_level_0", true);
    auto outputDeletionMapDir = outputDir->GetDirectory("deletionmap", true);
    ASSERT_TRUE(outputDeletionMapDir);
    ASSERT_FALSE(outputDeletionMapDir->IsExist("data_0"));
    ASSERT_FALSE(outputDeletionMapDir->IsExist("data_1"));
    ASSERT_TRUE(outputDeletionMapDir->IsExist("data_2"));
    std::shared_ptr<DeletionMapDiskIndexer> newIndexer2(new DeletionMapDiskIndexer(100, 2));
    ASSERT_TRUE(newIndexer2->Open(_config, outputDeletionMapDir->GetIDirectory()).IsOK());

    ASSERT_TRUE(newIndexer2->IsDeleted(2));
    ASSERT_TRUE(newIndexer2->IsDeleted(1));
}

TEST_F(DeletionMapMergerTest, testMergeFailed)
{
    auto rootDir = indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
    indexlib::file_system::DirectoryOption option;

    DeletionMapMerger merger;
    std::map<std::string, std::any> params;
    index::IIndexMerger::SegmentMergeInfos segMergeInfos;
    std::shared_ptr<framework::IndexTaskResourceManager> taskResourceManager;
    std::vector<std::pair<segmentid_t, std::shared_ptr<DeletionMapDiskIndexer>>> indexers;
    ASSERT_TRUE(merger.Init(_config, params).IsOK());
    // has not collect deletion map
    ASSERT_TRUE(merger.Merge(segMergeInfos, taskResourceManager).IsInvalidArgs());
    // target segments empty()
    merger.CollectAllDeletionMap(indexers);
    ASSERT_TRUE(merger.Merge(segMergeInfos, taskResourceManager).IsInvalidArgs());
    // target segment meta nullptr
    segMergeInfos.targetSegments.push_back(nullptr);
    ASSERT_TRUE(merger.Merge(segMergeInfos, taskResourceManager).IsInvalidArgs());
    // target segment dir nullptr
    segMergeInfos.targetSegments.clear();
    std::shared_ptr<framework::SegmentMeta> meta1(new framework::SegmentMeta(1));
    segMergeInfos.targetSegments.push_back(meta1);
    ASSERT_TRUE(merger.Merge(segMergeInfos, taskResourceManager).IsInvalidArgs());
    meta1->segmentDir = rootDir->MakeDirectory("segment_1_level_0", option);

    auto dir0 = rootDir->MakeDirectory("segment_0_level_0/deletionmap", option);
    std::shared_ptr<DeletionMapDiskIndexer> indexer0(new DeletionMapDiskIndexer(100, 0));

    // dump failed
    indexers.emplace_back(0, indexer0);
    merger.CollectAllDeletionMap(indexers);
    ASSERT_FALSE(merger.Merge(segMergeInfos, taskResourceManager).IsOK());
}

} // namespace indexlibv2::index
