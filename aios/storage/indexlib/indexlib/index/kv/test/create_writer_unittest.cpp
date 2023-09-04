#include "indexlib/index/kv/test/create_writer_unittest.h"

#include "indexlib/common/hash_table/cuckoo_hash_table_traits.h"
#include "indexlib/common/hash_table/dense_hash_table_traits.h"
#include "indexlib/common/hash_table/special_value.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/framework/SegmentGroupMetrics.h"
#include "indexlib/index/kv/kv_factory.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/memory_control/QuotaControl.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, CreateKVWriterTest);

CreateKVWriterTest::CreateKVWriterTest() {}

CreateKVWriterTest::~CreateKVWriterTest() {}

void CreateKVWriterTest::CaseSetUp() {}

void CreateKVWriterTest::CaseTearDown() {}

// Sample: "key:float;value:int8"
KVIndexConfigPtr CreateKVWriterTest::CreateKVConfig(const string& field)
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    return DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
}

template <typename Traits>
void CreateKVWriterTest::InnerCheckHashWriter(const KVWriterPtr& kvWriter, bool isVarLen)
{
    ASSERT_TRUE(kvWriter.get() != NULL);
    auto writer = kvWriter.get();
    using HashTableType = typename Traits::HashTable;
    if (isVarLen) {
        auto& writer_raw = *writer;
        ASSERT_TRUE(typeid(HashTableVarWriter) == typeid(writer_raw)) << typeid(HashTableVarWriter).name() << std::endl
                                                                      << typeid(writer_raw).name();

        auto& hashTable = DYNAMIC_POINTER_CAST(HashTableVarWriter, kvWriter)->mHashTable;
        auto& hashTable_raw = *hashTable;

        ASSERT_TRUE(typeid(HashTableType) == typeid(hashTable_raw)) << typeid(HashTableType).name() << std::endl
                                                                    << typeid(hashTable_raw).name();
    } else {
        auto& writer_raw = *writer;
        ASSERT_TRUE(typeid(HashTableFixWriter) == typeid(writer_raw)) << typeid(HashTableVarWriter).name() << std::endl
                                                                      << typeid(writer_raw).name();

        auto& hashTable = DYNAMIC_POINTER_CAST(HashTableFixWriter, kvWriter)->mHashTable;
        auto& hashTable_raw = *hashTable;

        ASSERT_TRUE(typeid(HashTableType) == typeid(hashTable_raw)) << typeid(HashTableType).name() << std::endl
                                                                    << typeid(hashTable_raw).name();
    }
}

template <typename T, bool compactKey, bool shortOffset>
void CreateKVWriterTest::CheckCreate(const string& configStr, bool enableCompactKey)
{
    tearDown();
    setUp();

    IndexPartitionOptions onlineOptions;
    onlineOptions.SetIsOnline(true);
    IndexPartitionOptions offlineOptions;
    offlineOptions.SetIsOnline(false);
    DirectoryPtr directory;
    size_t maxMemUse = 10 * 1024;
    const KVIndexConfigPtr& kvIndexConfig = CreateKVConfig(configStr);
    const KVIndexPreference& kvIndexPreference = kvIndexConfig->GetIndexPreference();
    const KVIndexPreference::HashDictParam& dictParam = kvIndexPreference.GetHashDictParam();

    KVIndexPreference::HashDictParam newDictParam(dictParam.GetHashType(), dictParam.GetOccupancyPct(),
                                                  dictParam.UsePreciseCountWhenMerge(), enableCompactKey, shortOffset);
    kvIndexConfig->GetIndexPreference().SetHashDictParam(newDictParam);

    std::shared_ptr<framework::SegmentGroupMetrics> groupMetrics;

    typedef typename HashKeyTypeTraits<compactKey>::HashKeyType HashKeyType;
    typedef DenseHashTableTraits<HashKeyType, TimestampValue<T>, false> Traits;
    typedef DenseHashTableTraits<HashKeyType, Timestamp0Value<T>, false> Traits0;

    typedef typename HashOffsetTypeTraits<shortOffset>::HashOffsetType HashOffsetType;
    typedef typename VarKVValueTypeTraits<HashOffsetType, true>::ValueType VarTsValueType;
    typedef typename VarKVValueTypeTraits<HashOffsetType, false>::ValueType VarNoTsValueType;
    typedef DenseHashTableTraits<HashKeyType, VarTsValueType, false> VarTraits;
    typedef DenseHashTableTraits<HashKeyType, VarNoTsValueType, false> VarTraits0;

    const ValueConfigPtr& valueConfig = kvIndexConfig->GetValueConfig();
    bool isVarLen = false;
    if (valueConfig->GetAttributeCount() == 1) {
        AttributeConfigPtr attrConfig = valueConfig->GetAttributeConfig(0);
        FieldType fieldType = attrConfig->GetFieldType();
        isVarLen = attrConfig->IsMultiValue() || fieldType == ft_string;
    } else {
        isVarLen = true;
    }

    // online + hasTTL
    directory = MAKE_SEGMENT_DIRECTORY(0);
    kvIndexConfig->SetTTL(1);

    auto kvWriter = KVFactory::CreateWriter(kvIndexConfig);
    kvWriter->Init(kvIndexConfig, directory, true, maxMemUse, groupMetrics);

    if (isVarLen) {
        InnerCheckHashWriter<VarTraits>(kvWriter, true);
    } else {
        InnerCheckHashWriter<Traits>(kvWriter, false);
    }

    // online + !hasTTL
    directory = MAKE_SEGMENT_DIRECTORY(1);
    kvIndexConfig->SetTTL(INVALID_TTL);

    kvWriter = KVFactory::CreateWriter(kvIndexConfig);
    kvWriter->Init(kvIndexConfig, directory, true, maxMemUse, groupMetrics);

    if (isVarLen) {
        InnerCheckHashWriter<VarTraits>(kvWriter, true);
    } else {
        InnerCheckHashWriter<Traits>(kvWriter, false);
    }

    // offline + hasTTL
    directory = MAKE_SEGMENT_DIRECTORY(2);
    kvIndexConfig->SetTTL(1);
    kvWriter = KVFactory::CreateWriter(kvIndexConfig);
    kvWriter->Init(kvIndexConfig, directory, false, maxMemUse, groupMetrics);
    if (isVarLen) {
        InnerCheckHashWriter<VarTraits>(kvWriter, true);
    } else {
        InnerCheckHashWriter<Traits>(kvWriter, false);
    }

    // offline + !hasTTL
    directory = MAKE_SEGMENT_DIRECTORY(3);
    kvIndexConfig->SetTTL(INVALID_TTL);
    kvWriter = KVFactory::CreateWriter(kvIndexConfig);
    kvWriter->Init(kvIndexConfig, directory, false, maxMemUse, groupMetrics);
    if (isVarLen) {
        InnerCheckHashWriter<VarTraits0>(kvWriter, true);
    } else {
        InnerCheckHashWriter<Traits0>(kvWriter, false);
    }
}

void CreateKVWriterTest::TestCreate()
{
    CheckCreate<int8_t, false>("key:int64;value:int8", true);
    CheckCreate<int8_t, false>("key:int64;value:int8", false);

    CheckCreate<int16_t, true>("key:int32;value:int16", true);
    CheckCreate<int16_t, false>("key:int32;value:int16", false);

    CheckCreate<int32_t, true>("key:int16;value:int32", true);
    CheckCreate<int32_t, false>("key:int16;value:int32", false);

    CheckCreate<int64_t, true>("key:int8;value:int64", true);
    CheckCreate<int64_t, false>("key:int8;value:int64", false);

    CheckCreate<uint8_t, false>("key:int64;value:uint8", true);
    CheckCreate<uint8_t, false>("key:int64;value:uint8", false);

    CheckCreate<uint16_t, true>("key:int32;value:uint16", true);
    CheckCreate<uint16_t, false>("key:int32;value:uint16", false);

    CheckCreate<uint32_t, true>("key:int16;value:uint32", true);
    CheckCreate<uint32_t, false>("key:int16;value:uint32", false);

    CheckCreate<uint64_t, true>("key:int8;value:uint64", true);
    CheckCreate<uint64_t, false>("key:int8;value:uint64", false);

    CheckCreate<float, false>("key:int64;value:float", false);
    CheckCreate<float, false>("key:int64;value:float", true);

    CheckCreate<double, false>("key:uint64;value:double", false);
    CheckCreate<double, false>("key:uint64;value:double", true);

    // check enable shortOffset
    CheckCreate<offset_t, false>("key:int64;value:int8:true", false);
    CheckCreate<short_offset_t, false, true>("key:int64;value:int8:true", false);
    CheckCreate<offset_t, false>("key:int64;value:float:true", true);
    CheckCreate<short_offset_t, false, true>("key:int64;value:float:true", true);
    CheckCreate<offset_t, false>("key:int64;value:string:false", true);
    CheckCreate<short_offset_t, false, true>("key:int64;value:string:false", true);
}

template <typename T, typename Writer>
void CreateKVWriterTest::CheckCreateWithCuckoo(const string& configStr)
{
    tearDown();
    setUp();

    IndexPartitionOptions onlineOptions;
    onlineOptions.SetIsOnline(true);
    IndexPartitionOptions offlineOptions;
    offlineOptions.SetIsOnline(false);
    DirectoryPtr directory;
    size_t maxMemUse = 10 * 1024;

    const KVIndexConfigPtr& kvIndexConfig = CreateKVConfig(configStr);
    KVIndexPreference::HashDictParam keyParam("cuckoo", 90);
    kvIndexConfig->GetIndexPreference().SetHashDictParam(keyParam);

    std::shared_ptr<framework::SegmentGroupMetrics> groupMetrics;
    typedef DenseHashTableTraits<keytype_t, TimestampValue<T>, false> TraitsD;
    typedef CuckooHashTableTraits<keytype_t, TimestampValue<T>, false> TraitsC;

    // online + hasTTL
    directory = MAKE_SEGMENT_DIRECTORY(0);
    kvIndexConfig->SetTTL(1);

    auto kvWriter = KVFactory::CreateWriter(kvIndexConfig);
    kvWriter->Init(kvIndexConfig, directory, true, maxMemUse, groupMetrics);

    ASSERT_TRUE(kvWriter.get() != NULL);
    {
        auto writer = kvWriter.get();
        auto& writer_raw = *writer;
        ASSERT_TRUE(typeid(Writer) == typeid(writer_raw)) << typeid(Writer).name() << endl << typeid(writer_raw).name();
        auto& hashTable = DYNAMIC_POINTER_CAST(Writer, kvWriter)->mHashTable;
        using HashTableType = typename TraitsD::HashTable;
        auto& hashTable_raw = *hashTable;
        ASSERT_TRUE(typeid(HashTableType) == typeid(hashTable_raw)) << typeid(HashTableType).name() << std::endl
                                                                    << typeid(hashTable_raw).name();
    }
    // test actual hashtable occpancy
    auto DenseWriter = dynamic_cast<Writer*>(kvWriter.get());
    ASSERT_TRUE(DenseWriter);
    ASSERT_TRUE(DenseWriter->mIsOnline);
    ASSERT_EQ(80, DenseWriter->mHashTable->GetOccupancyPct());

    // offline + hasTTL
    directory = MAKE_SEGMENT_DIRECTORY(2);
    kvIndexConfig->SetTTL(1);
    kvWriter = KVFactory::CreateWriter(kvIndexConfig);
    kvWriter->Init(kvIndexConfig, directory, false, maxMemUse, groupMetrics);
    ASSERT_TRUE(kvWriter.get() != NULL);
    {
        auto writer = kvWriter.get();
        ASSERT_TRUE(typeid(Writer) == typeid(*writer)) << typeid(Writer).name() << endl << typeid(*writer).name();
        auto& hashTable = DYNAMIC_POINTER_CAST(Writer, kvWriter)->mHashTable;
        using HashTableType = typename TraitsC::HashTable;
        ASSERT_TRUE(typeid(HashTableType) == typeid(*hashTable)) << typeid(HashTableType).name() << std::endl
                                                                 << typeid(*hashTable).name();
    }
    // test actual hashtable occpancy

    auto CuckooWriter = dynamic_cast<Writer*>(kvWriter.get());
    ASSERT_TRUE(CuckooWriter);
    ASSERT_FALSE(CuckooWriter->mIsOnline);
    ASSERT_EQ(90, CuckooWriter->mHashTable->GetOccupancyPct());
}

void CreateKVWriterTest::TestOccupancy()
{
    // key: uint64_t value: double
    CheckCreateWithCuckoo<double, HashTableFixWriter>("key:uint64;value:double");
    // key: int64_t value: offset of multi values
    CheckCreateWithCuckoo<short_offset_t, HashTableVarWriter>("key:int64;value:float:true");
}
}} // namespace indexlib::index
