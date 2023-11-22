#include "indexlib/index/deletionmap/DeletionMapPatcher.h"

#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletDataInfo.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"
#include "indexlib/index/deletionmap/DeletionMapMemIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapModifier.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlibv2 { namespace index {

namespace {
class FakeSegment : public framework::Segment
{
public:
    FakeSegment(const shared_ptr<DeletionMapDiskIndexer>& indexer, segmentid_t segmentId, int32_t docCount)
        : Segment(Segment::SegmentStatus::ST_BUILT)
        , _indexer(indexer)
    {
        _segmentMeta.segmentId = segmentId;
        _segmentMeta.segmentInfo->docCount = docCount;
    }
    std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>> GetIndexer(const std::string&,
                                                                               const std::string&) override
    {
        return {Status::OK(), _indexer};
    }
    std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override
    {
        return {Status::OK(), 0};
    }
    size_t EvaluateCurrentMemUsed() override { return 0; }

public:
    std::shared_ptr<DeletionMapDiskIndexer> _indexer;
};
} // namespace
class DeletionMapPatcherTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    DeletionMapPatcherTest();
    ~DeletionMapPatcherTest();

    DECLARE_CLASS_NAME(DeletionMapPatcherTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DeletionMapPatcherTest, TestSimpleProcess);
AUTIL_LOG_SETUP(indexlib.index, DeletionMapPatcherTest);

DeletionMapPatcherTest::DeletionMapPatcherTest() {}

DeletionMapPatcherTest::~DeletionMapPatcherTest() {}

void DeletionMapPatcherTest::CaseSetUp() {}

void DeletionMapPatcherTest::CaseTearDown() {}

void DeletionMapPatcherTest::TestSimpleProcess()
{
    auto InnerTest = [this](bool branch) {
        vector<pair<shared_ptr<IIndexer>, segmentid_t>> indexerPairs;
        vector<tuple<std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>> indexers;
        vector<shared_ptr<DeletionMapMemIndexer>> memIndexers;
        vector<indexlib::file_system::DirectoryPtr> dirs;
        shared_ptr<DeletionMapConfig> config(new DeletionMapConfig);
        std::vector<std::shared_ptr<DeletionMapResource>> deletionMapResources;

        // seg0, seg1, seg1->seg0 patch
        auto dir = indexlib::file_system::Directory::Get(
            indexlib::file_system::FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow());
        for (size_t i = 0; i < 2; ++i) {
            auto segmentDir = dir->MakeDirectory(to_string(i) + "_" + to_string(branch));
            dirs.push_back(segmentDir);
            shared_ptr<DeletionMapMemIndexer> memIndexer(new DeletionMapMemIndexer(i, false));
            memIndexers.push_back(memIndexer);
            ASSERT_TRUE(memIndexer->Init(config, nullptr).IsOK());
            document::DocumentBatch docBatch;
            for (size_t i = 0; i < 100; ++i) {
                shared_ptr<indexlibv2::document::NormalDocument> doc(new indexlibv2::document::NormalDocument());
                doc->SetDocOperateType(ADD_DOC);
                docBatch.AddDocument(doc);
            }
            ASSERT_TRUE(memIndexer->Build(&docBatch).IsOK());
            ASSERT_TRUE(memIndexer->Delete(30).IsOK());
        }

        framework::TabletData tabletData("");
        ASSERT_TRUE(tabletData.Init(framework::Version(), {}, std::make_shared<framework::ResourceMap>()).IsOK());
        for (size_t i = 0; i < 2; ++i) {
            memIndexers[i]->Seal();
            ASSERT_TRUE(memIndexers[i]->Dump(nullptr, dirs[1], nullptr).IsOK());
            shared_ptr<DeletionMapDiskIndexer> indexer(new DeletionMapDiskIndexer(100, i));
            ASSERT_TRUE(indexer->Open(config, dirs[i]->GetIDirectory()).IsOK());
            tabletData.TEST_PushSegment(std::make_shared<FakeSegment>(indexer, i, 100));
            indexerPairs.emplace_back(indexer, i);
            indexers.emplace_back(indexer, framework::Segment::SegmentStatus::ST_BUILT, 100);
            deletionMapResources.emplace_back(std::make_shared<DeletionMapResource>(100));
        }

        DeletionMapModifier modifier;
        ASSERT_TRUE(modifier.Open(config, &tabletData).IsOK());
        ASSERT_TRUE(DeletionMapPatcher::LoadPatch(indexerPairs, {0, 1}, branch, &modifier).IsOK());
        DeletionMapIndexReader reader;
        auto tabletDataInfo = new framework::TabletDataInfo;
        tabletDataInfo->SetDocCount(300);
        reader._tabletDataInfo.reset(tabletDataInfo);
        ASSERT_TRUE(reader.DoOpen(indexers).IsOK());
        reader._deletionMapResources = deletionMapResources;
        ASSERT_TRUE(reader.IsDeleted(30));
        ASSERT_FALSE(reader.IsDeleted(31));
        ASSERT_TRUE(reader.IsDeleted(130));
    };
    InnerTest(true);
    InnerTest(false);
}

}} // namespace indexlibv2::index
