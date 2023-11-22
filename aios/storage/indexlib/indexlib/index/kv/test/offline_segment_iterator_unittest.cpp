#include "indexlib/index/kv/test/offline_segment_iterator_unittest.h"

#include "indexlib/config/test/region_schema_maker.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/kv/hash_table_fix_creator.h"
#include "indexlib/index/kv/hash_table_var_creator.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::test;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OfflineSegmentIteratorTest);

OfflineSegmentIteratorTest::OfflineSegmentIteratorTest() {}

OfflineSegmentIteratorTest::~OfflineSegmentIteratorTest() {}

void OfflineSegmentIteratorTest::CaseSetUp() {}

void OfflineSegmentIteratorTest::CaseTearDown() {}

void OfflineSegmentIteratorTest::TestCreateHashTableIterator()
{
    {
        // fix value
        string field = "key1:string;"
                       "value1:int8;value2:uint8;"
                       "value3:int16;value4:uint16;"
                       "value5:int32;value6:uint32;"
                       "value7:int64;value8:uint64;";
        InnerTestCreateFixSegment<int8_t, true>(field, "key1", "value1");
        InnerTestCreateFixSegment<uint8_t, false>(field, "key1", "value2");
        InnerTestCreateFixSegment<int16_t, true>(field, "key1", "value3");
        InnerTestCreateFixSegment<uint16_t, false>(field, "key1", "value4");
        InnerTestCreateFixSegment<int32_t, true>(field, "key1", "value5");
        InnerTestCreateFixSegment<uint32_t, false>(field, "key1", "value6");
        InnerTestCreateFixSegment<int64_t, true>(field, "key1", "value7");
        InnerTestCreateFixSegment<uint64_t, false>(field, "key1", "value8");
    };

    {
        // var value
        string field = "key1:string;key2:int32;iValue:uint32;"
                       "sValue:string;miValue:int32:true";
        InnerTestCreateVarSegment<offset_t, true>(field, "key1", "sValue");
        InnerTestCreateVarSegment<offset_t, false>(field, "key1", "sValue");
    };
}

template <typename FieldType, bool hasTTL>
void OfflineSegmentIteratorTest::InnerTestCreateFixSegment(const string& field, const string& key, const string& value)
{
    auto schema = SchemaMaker::MakeKVSchema(field, key, value);
    auto indexSchema = schema->GetIndexSchema();
    auto kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    KVFormatOptionsPtr kvOptions(new KVFormatOptions());
    if (hasTTL) {
        kvConfig->SetTTL(100);
    }

    common::KVMap nameInfo;
    auto hashTableInfo =
        HashTableFixCreator::CreateHashTableForReader(kvConfig, false, kvOptions->UseCompactBucket(), false, nameInfo);
    auto iterator = hashTableInfo.hashTableFileIterator.get();

    typedef typename KVValueTypeTraits<FieldType, hasTTL>::ValueType HashTableValue;
    typedef common::DenseHashTableTraits<keytype_t, HashTableValue, false> HashTableTraits;
    using IteratorType = typename HashTableTraits::BufferedFileIterator;
    ASSERT_TRUE(typeid(IteratorType) == typeid(*iterator));
}

template <typename FieldType, bool hasTTL>
void OfflineSegmentIteratorTest::InnerTestCreateVarSegment(const string& field, const string& key, const string& value)
{
    auto schema = SchemaMaker::MakeKVSchema(field, key, value);
    auto indexSchema = schema->GetIndexSchema();
    auto kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    KVFormatOptionsPtr kvOptions(new KVFormatOptions());
    if (hasTTL) {
        kvConfig->SetTTL(100);
    }

    common::KVMap nameInfo;
    auto hashTableInfo =
        HashTableVarCreator::CreateHashTableForReader(kvConfig, false, false, kvOptions->IsShortOffset(), nameInfo);
    auto iterator = hashTableInfo.hashTableFileIterator.get();

    typedef typename VarKVValueTypeTraits<FieldType, hasTTL>::ValueType HashTableValue;
    typedef common::DenseHashTableTraits<keytype_t, HashTableValue, false> HashTableTraits;

    using IteratorType = typename HashTableTraits::template FileIterator<true>;
    ASSERT_TRUE(typeid(IteratorType) == typeid(*iterator));
}

void OfflineSegmentIteratorTest::TestCreateMultiRegionHashTableIterator()
{
    string field = "key1:string;key2:int32;"
                   "value1:int8;value2:uint8;"
                   "value3:int16;value4:uint16;"
                   "value5:int32;value6:uint32;"
                   "value7:int64;value8:uint64;"
                   "value9:string;";
    // fixed value, total length > 8 bytes
    vector<vector<string>> kvPairs = {{"region1", "key1", "value1;value2;value7;"}, {"region2", "key2", "value3;"}};
    MultiRegionInnerTestCreate<offset_t, true>(kvPairs, field);
    MultiRegionInnerTestCreate<offset_t, false>(kvPairs, field);
    // fixed value, total length < 8 bytes
    kvPairs = {{"region1", "key1", "value7;"}, {"region2", "key2", "value4;"}};
    MultiRegionInnerTestCreate<offset_t, true>(kvPairs, field);
    MultiRegionInnerTestCreate<offset_t, false>(kvPairs, field);
    // var value and fix value(total length < 8 bytes)
    kvPairs = {
        {"region1", "key1", "value9;"},
        {"region2", "key2", "value4;"},
        {"region3", "value4", "value7;"},
    };
    MultiRegionInnerTestCreate<offset_t, true>(kvPairs, field);
    MultiRegionInnerTestCreate<offset_t, false>(kvPairs, field);
    // var value
    kvPairs = {{"region1", "key1", "value9;"}, {"region2", "key2", "key1;"}};
    MultiRegionInnerTestCreate<offset_t, true>(kvPairs, field);
    MultiRegionInnerTestCreate<offset_t, false>(kvPairs, field);
}

template <typename FieldType, bool hasTTL>
void OfflineSegmentIteratorTest::MultiRegionInnerTestCreate(vector<vector<string>>& kvPairs, const string& field)
{
    RegionSchemaMaker maker(field, "kv");
    for (auto& kv : kvPairs) {
        maker.AddRegion(kv[0], kv[1], kv[2], "");
    }
    IndexPartitionSchemaPtr schema = maker.GetSchema();
    KVIndexConfigPtr kvConfig = CreateKVIndexConfigForMultiRegionData(schema);

    KVFormatOptionsPtr kvOptions(new KVFormatOptions());
    if (hasTTL) {
        kvConfig->SetTTL(100);
    }

    common::KVMap nameInfo;
    auto hashTableInfo =
        HashTableVarCreator::CreateHashTableForReader(kvConfig, false, false, kvOptions->IsShortOffset(), nameInfo);
    auto iterator = hashTableInfo.hashTableFileIterator.get();

    typedef typename MultiRegionVarKVValueTypeTraits<FieldType, hasTTL>::ValueType HashTableValue;
    typedef common::DenseHashTableTraits<keytype_t, HashTableValue, false> HashTableTraits;

    using IteratorType = typename HashTableTraits::template FileIterator<true>;
    ASSERT_TRUE(typeid(IteratorType) == typeid(*iterator));
}
}} // namespace indexlib::index
