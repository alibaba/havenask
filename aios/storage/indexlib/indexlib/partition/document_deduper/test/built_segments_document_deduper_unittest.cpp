#include "indexlib/partition/document_deduper/test/built_segments_document_deduper_unittest.h"

#include "autil/LongHashValue.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/partition/test/mock_partition_modifier.h"
#include "indexlib/test/single_field_partition_data_provider.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, BuiltSegmentsDocumentDeduperTest);

BuiltSegmentsDocumentDeduperTest::BuiltSegmentsDocumentDeduperTest() {}

BuiltSegmentsDocumentDeduperTest::~BuiltSegmentsDocumentDeduperTest() {}

void BuiltSegmentsDocumentDeduperTest::CaseSetUp() {}

void BuiltSegmentsDocumentDeduperTest::CaseTearDown() {}

void BuiltSegmentsDocumentDeduperTest::TestDedup64Pks()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEMP_DATA_PATH(), "uint64", SFP_PK_INDEX);
    string docsStr = "1,2,3#1,2,4,5#4,6,7,1#3,2,6";
    provider.Build(docsStr, SFP_OFFLINE);

    auto pkReader = std::make_shared<index::LegacyPrimaryKeyReader<uint64_t>>();
    pkReader->Open(provider.GetIndexConfig(), provider.GetPartitionData());
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    MockPartitionModifierPtr mockModifier(new MockPartitionModifier(schema));
    mockModifier->SetPrimaryKeyIndexReader(pkReader);

    BuiltSegmentsDocumentDeduper deduper(schema);
    deduper.Init(mockModifier);

    EXPECT_CALL(*mockModifier, RemoveDocument(0)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(3)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(1)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(4)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(2)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(5)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(8)).WillOnce(Return(true));

    deduper.Dedup();
}

void BuiltSegmentsDocumentDeduperTest::TestDedup128Pks()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEMP_DATA_PATH(), "uint64", SFP_PK_128_INDEX);
    string docsStr = "1,2,3#1,2,4,5#4,6,7,1#3,2,6";
    provider.Build(docsStr, SFP_OFFLINE);

    auto pkReader = std::make_shared<index::LegacyPrimaryKeyReader<autil::uint128_t>>();
    pkReader->Open(provider.GetIndexConfig(), provider.GetPartitionData());
    IndexPartitionSchemaPtr schema = provider.GetSchema();
    MockPartitionModifierPtr mockModifier(new MockPartitionModifier(schema));
    mockModifier->SetPrimaryKeyIndexReader(pkReader);

    BuiltSegmentsDocumentDeduper deduper(schema);
    deduper.Init(mockModifier);

    EXPECT_CALL(*mockModifier, RemoveDocument(0)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(3)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(1)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(4)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(2)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(5)).WillOnce(Return(true));
    EXPECT_CALL(*mockModifier, RemoveDocument(8)).WillOnce(Return(true));

    deduper.Dedup();
}
}} // namespace indexlib::partition
