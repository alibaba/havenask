#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"

#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapMemIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapModifier.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlibv2::index {

class DeletionMapIndexReaderTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    DeletionMapIndexReaderTest();
    ~DeletionMapIndexReaderTest();

    DECLARE_CLASS_NAME(DeletionMapIndexReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForReadAndWrite();
    void TestCaseForGetDeletedDocCount();

private:
    void InnerTestDeletionmapReadAndWrite(
        uint32_t segmentDocCount, const string& toDeleteDocs, const string& deletedDocs,
        std::vector<tuple<std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>>& indexers);
    void CheckDeletedDocs(
        const string& deletedDocs,
        const std::vector<tuple<std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>>& indexers);

private:
    indexlib::file_system::IFileSystemPtr _fs;
    indexlib::file_system::DirectoryPtr _rootDir;
    shared_ptr<config::IIndexConfig> _config;
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DeletionMapIndexReaderTest, TestCaseForReadAndWrite);
INDEXLIB_UNIT_TEST_CASE(DeletionMapIndexReaderTest, TestCaseForGetDeletedDocCount);
AUTIL_LOG_SETUP(indexlib.index, DeletionMapIndexReaderTest);

DeletionMapIndexReaderTest::DeletionMapIndexReaderTest() {}

DeletionMapIndexReaderTest::~DeletionMapIndexReaderTest() {}

void DeletionMapIndexReaderTest::CaseSetUp()
{
    _config.reset(new DeletionMapConfig);
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.enableAsyncFlush = false;
    _fs = indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(_fs);
}

void DeletionMapIndexReaderTest::CaseTearDown() {}

void DeletionMapIndexReaderTest::TestCaseForReadAndWrite()
{
    std::vector<tuple<std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>> indexers;
    InnerTestDeletionmapReadAndWrite(6, "1,5", "1,5", indexers);
    InnerTestDeletionmapReadAndWrite(1, "2", "1,5,2", indexers);

    // test docId 11, test over-bound doc of building segment
    InnerTestDeletionmapReadAndWrite(4, "8,1", "1,5,2,8,11", indexers);

    // delete doc 6, test delete doc without deletionmap file
    InnerTestDeletionmapReadAndWrite(5, "4,6,3", "1,5,2,8,4,6,3", indexers);
    InnerTestDeletionmapReadAndWrite(3, "16,2,6", "1,5,2,8,4,6,3,16", indexers);
}

void DeletionMapIndexReaderTest::TestCaseForGetDeletedDocCount()
{
    std::vector<tuple<segmentid_t, std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>> indexers;
    std::vector<tuple<std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>> readerIndexers;
    const uint32_t segmentDocCount = 10;
    for (size_t i = 0; i < 3; ++i) {
        shared_ptr<DeletionMapDiskIndexer> diskIndexer(new DeletionMapDiskIndexer(segmentDocCount, indexers.size()));
        auto directory = _rootDir;
        ASSERT_TRUE(diskIndexer->Open(_config, directory->GetIDirectory()).IsOK());
        indexers.emplace_back(i, diskIndexer, framework::Segment::SegmentStatus::ST_BUILT, segmentDocCount);
        readerIndexers.emplace_back(diskIndexer, framework::Segment::SegmentStatus::ST_BUILT, segmentDocCount);
    }
    shared_ptr<DeletionMapModifier> modifier(new DeletionMapModifier);
    ASSERT_TRUE(
        indexlib::index::DeletionMapIndexerOrganizerUtil::InitFromIndexers(indexers, &(modifier->_buildInfoHolder))
            .IsOK());

    ASSERT_TRUE(modifier->Delete(15).IsOK());
    ASSERT_TRUE(modifier->Delete(25).IsOK());

    shared_ptr<DeletionMapIndexReader> reader(new DeletionMapIndexReader);
    ASSERT_TRUE(reader->DoOpen(readerIndexers).IsOK());
    auto diskIndexer = dynamic_pointer_cast<DeletionMapDiskIndexer>(get<1>(indexers[0]));
    ASSERT_TRUE(diskIndexer);
    ASSERT_EQ((uint32_t)0, diskIndexer->GetDeletedDocCount());
    ASSERT_EQ((uint32_t)0, reader->GetSegmentDeletedDocCount(0));
    diskIndexer = dynamic_pointer_cast<DeletionMapDiskIndexer>(get<1>(indexers[1]));
    ASSERT_TRUE(diskIndexer);
    ASSERT_EQ((uint32_t)1, diskIndexer->GetDeletedDocCount());
    ASSERT_EQ((uint32_t)1, reader->GetSegmentDeletedDocCount(1));
    diskIndexer = dynamic_pointer_cast<DeletionMapDiskIndexer>(get<1>(indexers[2]));
    ASSERT_TRUE(diskIndexer);
    ASSERT_EQ((uint32_t)1, diskIndexer->GetDeletedDocCount());
    ASSERT_EQ((uint32_t)1, reader->GetSegmentDeletedDocCount(2));
}

void DeletionMapIndexReaderTest::InnerTestDeletionmapReadAndWrite(
    uint32_t segmentDocCount, const string& toDeleteDocs, const string& deletedDocs,
    std::vector<tuple<std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>>& indexers)
{
    shared_ptr<DeletionMapMemIndexer> buildingIndexer(new DeletionMapMemIndexer(indexers.size(), false));
    ASSERT_TRUE(buildingIndexer->Init(_config, nullptr).IsOK());
    document::DocumentBatch docBatch;
    for (size_t i = 0; i < segmentDocCount; ++i) {
        shared_ptr<indexlibv2::document::NormalDocument> doc(new indexlibv2::document::NormalDocument());
        doc->SetDocOperateType(ADD_DOC);
        docBatch.AddDocument(doc);
    }
    ASSERT_TRUE(buildingIndexer->Build(&docBatch).IsOK());
    auto currentIndexers = indexers;
    currentIndexers.emplace_back(buildingIndexer, framework::Segment::SegmentStatus::ST_BUILDING, segmentDocCount);
    segmentid_t segId = 0;
    std::vector<std::tuple<segmentid_t, std::shared_ptr<indexlibv2::index::IIndexer>,
                           indexlibv2::framework::Segment::SegmentStatus, size_t>>
        indexerInfoWithSegId;
    for (auto [indexer, segmentStatus, docCount] : currentIndexers) {
        indexerInfoWithSegId.emplace_back(segId++, indexer, segmentStatus, docCount);
    }

    shared_ptr<DeletionMapModifier> deletionMapModifier(new DeletionMapModifier);
    ASSERT_TRUE(indexlib::index::DeletionMapIndexerOrganizerUtil::InitFromIndexers(
                    indexerInfoWithSegId, &(deletionMapModifier->_buildInfoHolder))
                    .IsOK());

    vector<docid_t> toDeleteDocIds;
    autil::StringUtil::fromString(toDeleteDocs, toDeleteDocIds, ",");
    for (size_t i = 0; i < toDeleteDocIds.size(); i++) {
        ASSERT_TRUE(deletionMapModifier->Delete(toDeleteDocIds[i]).IsOK());
    }

    // test read in building mode
    CheckDeletedDocs(deletedDocs, currentIndexers);

    auto directory = _rootDir;
    autil::mem_pool::Pool dumpPool;
    buildingIndexer->Seal();
    ASSERT_TRUE(buildingIndexer->Dump(&dumpPool, directory, nullptr).IsOK());
    shared_ptr<DeletionMapDiskIndexer> dumpedIndexer(new DeletionMapDiskIndexer(segmentDocCount, indexers.size()));
    ASSERT_TRUE(dumpedIndexer->Open(_config, directory->GetIDirectory()).IsOK());
    indexers.emplace_back(dumpedIndexer, framework::Segment::SegmentStatus::ST_BUILT, segmentDocCount);
    buildingIndexer.reset();
    // test read in built mode
    CheckDeletedDocs(deletedDocs, indexers);
}

void DeletionMapIndexReaderTest::CheckDeletedDocs(
    const string& deletedDocs,
    const std::vector<tuple<std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>>& indexers)
{
    vector<docid_t> deletedDocIds;
    autil::StringUtil::fromString(deletedDocs, deletedDocIds, ",");
    shared_ptr<DeletionMapIndexReader> reader(new DeletionMapIndexReader);
    auto tabletDataInfo = new framework::TabletDataInfo;
    tabletDataInfo->SetDocCount(300);
    reader->_tabletDataInfo.reset(tabletDataInfo);
    ASSERT_TRUE(reader->DoOpen(indexers).IsOK());
    for (size_t i = 0; i < deletedDocIds.size(); i++) {
        ASSERT_TRUE(reader->IsDeleted(deletedDocIds[i]));
    }
}
} // namespace indexlibv2::index
