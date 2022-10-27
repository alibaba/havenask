#include "indexlib/index/normal/deletionmap/test/in_mem_deletion_map_reader_unittest.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemDeletionMapReaderTest);

InMemDeletionMapReaderTest::InMemDeletionMapReaderTest()
{
}

InMemDeletionMapReaderTest::~InMemDeletionMapReaderTest()
{
}

void InMemDeletionMapReaderTest::CaseSetUp()
{
}

void InMemDeletionMapReaderTest::CaseTearDown()
{
}

void InMemDeletionMapReaderTest::TestSimpleProcess()
{
    SegmentInfo segInfo;
    ExpandableBitmap bitmap;
    InMemDeletionMapReader reader(&bitmap, &segInfo, INVALID_SEGMENTID);
    docid_t docId = 100;
    reader.Delete(docId);

    uint32_t currentValidCount = bitmap.GetValidItemCount();

    segInfo.docCount = currentValidCount + 200;
    INDEXLIB_TEST_TRUE(reader.IsDeleted(docId));
    INDEXLIB_TEST_TRUE(!reader.IsDeleted(segInfo.docCount - 1));
    INDEXLIB_TEST_TRUE(reader.IsDeleted(segInfo.docCount));
    INDEXLIB_TEST_TRUE(reader.IsDeleted(segInfo.docCount + 1));

    INDEXLIB_TEST_TRUE(!reader.IsDeleted(currentValidCount - 1));
    INDEXLIB_TEST_TRUE(!reader.IsDeleted(currentValidCount));
    INDEXLIB_TEST_TRUE(!reader.IsDeleted(currentValidCount + 1));

    INDEXLIB_TEST_EQUAL((uint32_t)1, reader.GetDeletedDocCount());
}

IE_NAMESPACE_END(index);

