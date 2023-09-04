#include "indexlib/index/deletionmap/DeletionMapMemIndexer.h"

#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/DocMapDumpParams.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlibv2::index {

class DeletionMapMemIndexerTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    DeletionMapMemIndexerTest();
    ~DeletionMapMemIndexerTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSortDump();
    void TestSealAndDump();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DeletionMapMemIndexerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DeletionMapMemIndexerTest, TestSortDump);
INDEXLIB_UNIT_TEST_CASE(DeletionMapMemIndexerTest, TestSealAndDump);
AUTIL_LOG_SETUP(indexlib.index, DeletionMapMemIndexerTest);

DeletionMapMemIndexerTest::DeletionMapMemIndexerTest() {}

DeletionMapMemIndexerTest::~DeletionMapMemIndexerTest() {}

void DeletionMapMemIndexerTest::CaseSetUp() {}

void DeletionMapMemIndexerTest::CaseTearDown() {}

void DeletionMapMemIndexerTest::TestSimpleProcess()
{
    DeletionMapMemIndexer memIndexer(0, false);
    shared_ptr<DeletionMapConfig> config(new DeletionMapConfig);
    ASSERT_TRUE(memIndexer.Init(config, nullptr).IsOK());
    document::DocumentBatch docBatch;
    for (size_t i = 0; i < 100; ++i) {
        shared_ptr<indexlibv2::document::NormalDocument> doc(new indexlibv2::document::NormalDocument());
        doc->SetDocOperateType(ADD_DOC);
        docBatch.AddDocument(doc);
    }
    ASSERT_TRUE(memIndexer.Build(&docBatch).IsOK());

    ASSERT_TRUE(memIndexer.Delete(0).IsOK());
    ASSERT_TRUE(memIndexer.Delete(99).IsOK());
    ASSERT_TRUE(memIndexer.Delete(100).IsOK());
    ASSERT_TRUE(memIndexer.IsDeleted(0));
    ASSERT_TRUE(memIndexer.IsDeleted(99));
    ASSERT_TRUE(memIndexer.IsDeleted(100));
    ASSERT_TRUE(memIndexer.IsDeleted(101));
    ASSERT_EQ((uint32_t)3, memIndexer.GetDeletedDocCount());
}

void DeletionMapMemIndexerTest::TestSealAndDump()
{
    DeletionMapMemIndexer memIndexer(0, false);
    shared_ptr<DeletionMapConfig> config(new DeletionMapConfig);
    ASSERT_TRUE(memIndexer.Init(config, nullptr).IsOK());
    memIndexer._docCount = 10 * 1024 * 1024 + 1;
    ASSERT_TRUE(memIndexer.Delete(0).IsOK());
    memIndexer.Seal();
    std::shared_ptr<indexlib::file_system::Directory> indexDirectory =
        indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
    ASSERT_TRUE(memIndexer.Dump(nullptr, indexDirectory, nullptr).IsOK());
    DeletionMapDiskIndexer diskIndexer(10 * 1024 * 1024 + 1, 0);
    ASSERT_TRUE(diskIndexer.Open(config, indexDirectory->GetIDirectory()).IsOK());
}

void DeletionMapMemIndexerTest::TestSortDump()
{
    DeletionMapMemIndexer memIndexer(0, false);
    int32_t totalDocCount = 19;
    shared_ptr<DeletionMapConfig> config(new DeletionMapConfig);
    ASSERT_TRUE(memIndexer.Init(config, nullptr).IsOK());
    document::DocumentBatch docBatch;
    for (size_t i = 0; i < totalDocCount; ++i) {
        shared_ptr<indexlibv2::document::NormalDocument> doc(new indexlibv2::document::NormalDocument());
        doc->SetDocOperateType(ADD_DOC);
        docBatch.AddDocument(doc);
    }
    ASSERT_TRUE(memIndexer.Build(&docBatch).IsOK());
    ASSERT_TRUE(memIndexer.Delete(0).IsOK());
    ASSERT_TRUE(memIndexer.Delete(10).IsOK());
    ASSERT_TRUE(memIndexer.Delete(17).IsOK());
    memIndexer.Seal();

    auto dumpParams = std::make_shared<index::DocMapDumpParams>();
    dumpParams->new2old.resize(totalDocCount);
    dumpParams->old2new.resize(totalDocCount);
    for (docid_t docId = 0; docId < totalDocCount; ++docId) {
        dumpParams->old2new[docId] = (docId + 1) % totalDocCount;
    }

    std::shared_ptr<indexlib::file_system::Directory> indexDirectory =
        indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
    ASSERT_TRUE(memIndexer.Dump(nullptr, indexDirectory, dumpParams).IsOK());
    set<docid_t> expectDeleteDoc = {1, 11, 18};

    DeletionMapDiskIndexer diskIndexer(19, 0);
    ASSERT_TRUE(diskIndexer.Open(config, indexDirectory->GetIDirectory()).IsOK());

    for (docid_t docId = 0; docId < totalDocCount; ++docId) {
        if (expectDeleteDoc.find(docId) != expectDeleteDoc.end()) {
            ASSERT_TRUE(diskIndexer.IsDeleted(docId));
        } else {
            ASSERT_FALSE(diskIndexer.IsDeleted(docId));
        }
    }
}

} // namespace indexlibv2::index
