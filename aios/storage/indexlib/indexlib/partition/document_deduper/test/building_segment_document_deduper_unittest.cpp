#include "autil/LongHashValue.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/document_deduper/building_segment_document_deduper.h"
#include "indexlib/partition/test/mock_partition_modifier.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
class BuildingSegmentDocumentDeduperTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    BuildingSegmentDocumentDeduperTest();
    ~BuildingSegmentDocumentDeduperTest();

    DECLARE_CLASS_NAME(BuildingSegmentDocumentDeduperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRecordDocids();
    void TestDedup64Pks();
    void TestDedup128Pks();

private:
    IE_LOG_DECLARE();

private:
    config::IndexPartitionOptions _options;
};

INSTANTIATE_TEST_CASE_P(BuildMode, BuildingSegmentDocumentDeduperTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(BuildingSegmentDocumentDeduperTest, TestRecordDocids);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(BuildingSegmentDocumentDeduperTest, TestDedup64Pks);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(BuildingSegmentDocumentDeduperTest, TestDedup128Pks);

IE_LOG_SETUP(partition, BuildingSegmentDocumentDeduperTest);

BuildingSegmentDocumentDeduperTest::BuildingSegmentDocumentDeduperTest() {}

BuildingSegmentDocumentDeduperTest::~BuildingSegmentDocumentDeduperTest() {}

void BuildingSegmentDocumentDeduperTest::CaseSetUp()
{
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &_options);
}

void BuildingSegmentDocumentDeduperTest::CaseTearDown() {}

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
    InMemPrimaryKeySegmentReaderTyped<uint64_t>::Iterator iter = hashMap.CreateIterator();

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
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "uint64", SFP_PK_128_INDEX);
    string docsStr = "1,2,3,4,5,1,3,5";
    provider.Build("", SFP_OFFLINE);
    provider.Build(docsStr, SFP_REALTIME);

    InMemorySegmentPtr inMemSegment = provider.GetPartitionData()->GetInMemorySegment();
    inMemSegment->SetBaseDocId(100);

    IndexPartitionSchemaPtr schema = provider.GetSchema();
    MockPartitionModifierPtr mockModifier(new MockPartitionModifier(schema));

    BuildingSegmentDocumentDeduper deduper(schema);
    deduper.Init(inMemSegment, mockModifier);

    EXPECT_CALL(*mockModifier, RemoveDocument(100)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(102)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(104)).WillOnce(Return(true));

    deduper.Dedup();
}

void BuildingSegmentDocumentDeduperTest::TestDedup64Pks()
{
    SingleFieldPartitionDataProvider provider(_options);
    provider.Init(GET_TEMP_DATA_PATH(), "uint64", SFP_PK_INDEX);
    string docsStr = "1,2,3,4,5,1,3,5";
    provider.Build("", SFP_OFFLINE);
    provider.Build(docsStr, SFP_REALTIME);

    InMemorySegmentPtr inMemSegment = provider.GetPartitionData()->GetInMemorySegment();
    inMemSegment->SetBaseDocId(100);
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    MockPartitionModifierPtr mockModifier(new MockPartitionModifier(schema));

    BuildingSegmentDocumentDeduper deduper(schema);
    deduper.Init(inMemSegment, mockModifier);

    EXPECT_CALL(*mockModifier, RemoveDocument(100)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(102)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(104)).WillOnce(Return(true));

    deduper.Dedup();
}
}} // namespace indexlib::partition
