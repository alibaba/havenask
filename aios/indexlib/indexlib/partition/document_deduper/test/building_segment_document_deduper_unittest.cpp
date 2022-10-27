#include <autil/LongHashValue.h>
#include "indexlib/partition/document_deduper/test/building_segment_document_deduper_unittest.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/partition/test/mock_partition_modifier.h"
#include "indexlib/test/single_field_partition_data_provider.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, BuildingSegmentDocumentDeduperTest);

BuildingSegmentDocumentDeduperTest::BuildingSegmentDocumentDeduperTest()
{
}

BuildingSegmentDocumentDeduperTest::~BuildingSegmentDocumentDeduperTest()
{
}

void BuildingSegmentDocumentDeduperTest::CaseSetUp()
{
}

void BuildingSegmentDocumentDeduperTest::CaseTearDown()
{
}

void BuildingSegmentDocumentDeduperTest::TestRecordDocids()
{
    InMemPrimaryKeySegmentReaderTyped<uint64_t>::HashMapTyped hashMap(100);
    hashMap.FindAndInsert(0, 0);
    hashMap.FindAndInsert(1, 1);
    hashMap.FindAndInsert(2, 2);
    hashMap.FindAndInsert(3, 3);
    hashMap.FindAndInsert(0, 4);
    hashMap.FindAndInsert(3, 5);

    util::ExpandableBitmap bitmap;
    InMemPrimaryKeySegmentReaderTyped<uint64_t>::Iterator iter =
        hashMap.CreateIterator();

    docid_t maxDocid;
    BuildingSegmentDocumentDeduper::RecordDocids<uint64_t>(iter, bitmap, maxDocid);
    ASSERT_EQ((docid_t)5, maxDocid);
    ASSERT_FALSE(bitmap.Test(0));
    ASSERT_FALSE(bitmap.Test(3));
    ASSERT_TRUE(bitmap.Test(1));
    ASSERT_TRUE(bitmap.Test(2));
    ASSERT_TRUE(bitmap.Test(4));
    ASSERT_TRUE(bitmap.Test(5));
}

void BuildingSegmentDocumentDeduperTest::TestDedup128Pks()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_128_INDEX);
    string docsStr = "1,2,3,4,5,1,3,5"; 
    provider.Build("", SFP_OFFLINE);
    provider.Build(docsStr, SFP_REALTIME);

    InMemorySegmentPtr inMemSegment = provider.GetPartitionData()->GetInMemorySegment();
    inMemSegment->SetBaseDocId(100);

    IndexPartitionSchemaPtr schema = provider.GetSchema();
    MockPartitionModifierPtr mockModifier(new MockPartitionModifier(schema));

    BuildingSegmentDocumentDeduper deduper(schema);
    deduper.Init(inMemSegment, mockModifier);

    EXPECT_CALL(*mockModifier, RemoveDocument(100))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(102))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(104))
        .WillOnce(Return(true));

    deduper.Dedup();
}

void BuildingSegmentDocumentDeduperTest::TestDedup64Pks()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_INDEX);
    string docsStr = "1,2,3,4,5,1,3,5"; 
    provider.Build("", SFP_OFFLINE);
    provider.Build(docsStr, SFP_REALTIME);

    InMemorySegmentPtr inMemSegment = provider.GetPartitionData()->GetInMemorySegment();
    inMemSegment->SetBaseDocId(100);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    MockPartitionModifierPtr mockModifier(new MockPartitionModifier(schema));

    BuildingSegmentDocumentDeduper deduper(schema);
    deduper.Init(inMemSegment, mockModifier);

    EXPECT_CALL(*mockModifier, RemoveDocument(100))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(102))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(104))
        .WillOnce(Return(true));

    deduper.Dedup();
}

IE_NAMESPACE_END(partition);

