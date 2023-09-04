#include "indexlib/index/kv/test/create_hash_table_unittest.h"

#include "indexlib/common/hash_table/dense_hash_table_file_reader.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, CreateHashTableTest);

CreateHashTableTest::CreateHashTableTest() {}

CreateHashTableTest::~CreateHashTableTest() {}

void CreateHashTableTest::CaseSetUp() { setenv("DISABLE_CODEGEN", "true", 1); }

void CreateHashTableTest::CaseTearDown() { unsetenv("DISABLE_CODEGEN"); }

void CreateHashTableTest::TestHashTableFixSegmentReader()
{
    string field = "key:int32;value:uint32;";
    mSchema = SchemaMaker::MakeKVSchema(field, "key", "value");
    mKVConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    // FixInnerTest(tableType, compactHashKey, enableTTL, isOnline, useCompactBucket);
    InnerTest("HashTableFixSegmentReader", "DENSE_TABLE");
    InnerTest("HashTableFixSegmentReader", "CUCKOO_TABLE");
    InnerTest("HashTableFixSegmentReader", "DENSE_READER");
    InnerTest("HashTableFixSegmentReader", "CUCKOO_READER");
}

void CreateHashTableTest::TestKVSegmentOffsetReader()
{
    string field = "key:int32;value:uint32;";
    mSchema = SchemaMaker::MakeKVSchema(field, "key", "value");
    mKVConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    // OffsetInnerTest(tableType, isMultiRegion, compactHashKey, enableTTL, isOnline, isShortOffset);
    InnerTest("KVSegmentOffsetReader", "DENSE_TABLE");
    InnerTest("KVSegmentOffsetReader", "CUCKOO_TABLE");
    InnerTest("KVSegmentOffsetReader", "DENSE_READER");
    InnerTest("KVSegmentOffsetReader", "CUCKOO_READER");
}

void CreateHashTableTest::InnerTest(string readerType, string tableTypeStr)
{
    int argsCount = 0;
    if (readerType == "HashTableFixSegmentReader") {
        argsCount = 4;
    } else if (readerType == "KVSegmentOffsetReader") {
        argsCount = 5;
    } else {
        assert(false);
    }
    bool v[10];
    for (int i = 0; i < (1 << argsCount); i++) {
        // make v[0]~v[argsCount-1] from 0000[0] to 1111[1]
        for (int j = 0; j < argsCount; j++) {
            v[j] = (bool)(i & (1 << j));
        }
        /*
        for (int j = 0; j < argsCount; j++) {
            cout << v[j];
        }
        cout << endl;
        */
        if (readerType == "HashTableFixSegmentReader") {
            HashTableAccessType tableType;
            if (tableTypeStr == "DENSE_TABLE") {
                tableType = HashTableAccessType::DENSE_TABLE;
            } else if (tableTypeStr == "DENSE_READER") {
                tableType = HashTableAccessType::DENSE_READER;
            } else if (tableTypeStr == "CUCKOO_TABLE") {
                tableType = HashTableAccessType::CUCKOO_TABLE;
            } else { // tableTypeStr == "CUCKOO_READER"
                tableType = HashTableAccessType::CUCKOO_READER;
            }
            FixInnerTest(tableType, v[0], v[1], v[2], v[3]);
        }
        if (readerType == "KVSegmentOffsetReader") {
            HashTableAccessType tableType;
            if (tableTypeStr == "DENSE_TABLE") {
                tableType = HashTableAccessType::DENSE_TABLE;
            } else if (tableTypeStr == "DENSE_READER") {
                tableType = HashTableAccessType::DENSE_READER;
            } else if (tableTypeStr == "CUCKOO_TABLE") {
                tableType = HashTableAccessType::CUCKOO_TABLE;
            } else { // tableTypeStr == "CUCKOO_READER"
                tableType = HashTableAccessType::CUCKOO_READER;
            }
            OffsetInnerTest(tableType, v[0], v[1], v[2], v[3], v[4]);
        }
    }
}

void CreateHashTableTest::FixInnerTest(HashTableAccessType tableType, bool compactHashKey, bool enableTTL,
                                       bool isOnline, bool useCompactBucket)
{
    if (compactHashKey) {
        mKVConfig->GetIndexPreference()._hashParam.SetEnableCompactHashKey(true);
    } else {
        mKVConfig->GetIndexPreference()._hashParam.SetEnableCompactHashKey(false);
    }
    if (tableType == HashTableAccessType::DENSE_READER || tableType == HashTableAccessType::DENSE_TABLE) {
        mKVConfig->GetIndexPreference()._hashParam._hashType = "dense";
    } else if (tableType == HashTableAccessType::CUCKOO_READER || tableType == HashTableAccessType::CUCKOO_TABLE) {
        mKVConfig->GetIndexPreference()._hashParam._hashType = "cuckoo";
    }

    if (enableTTL) {
        mKVConfig->SetTTL(10);
    } else {
        mKVConfig->SetTTL(INVALID_TTL);
    }
    string createType;
    KVMap nameInfo;
    HashTableFixSegmentReader reader;
    bool useFileReader =
        tableType == HashTableAccessType::DENSE_READER || tableType == HashTableAccessType::CUCKOO_READER;
    auto _ =
        HashTableFixCreator::CreateHashTableForReader(mKVConfig, useFileReader, useCompactBucket, isOnline, nameInfo);
    (void)_;
    if (tableType == HashTableAccessType::DENSE_TABLE || tableType == HashTableAccessType::CUCKOO_TABLE) {
        createType = reader.GetTableName(nameInfo, useCompactBucket, false);
    } else {
        createType = reader.GetTableName(nameInfo, useCompactBucket, true);
    }
    string expectType;
    if (isOnline || tableType == HashTableAccessType::DENSE_TABLE || tableType == HashTableAccessType::DENSE_READER) {
        if (useFileReader) {
            expectType = "common::DenseHashTableFileReader<";
        } else {
            expectType = "common::DenseHashTable<";
        }
    } else {
        if (useFileReader) {
            expectType = "common::CuckooHashTableFileReader<";
        } else {
            expectType = "common::CuckooHashTable<";
        }
    }
    if (compactHashKey) {
        expectType += "compact_keytype_t,";
    } else {
        expectType += "keytype_t,";
    }
    if (enableTTL || isOnline) {
        expectType += "common::TimestampValue<uint32_t>,false,";
    } else {
        expectType += "common::Timestamp0Value<uint32_t>,true,";
    }
    if (useCompactBucket) {
        expectType += "true>";
    } else {
        expectType += "false>";
    }
    ASSERT_EQ(expectType, createType);
}

void CreateHashTableTest::OffsetInnerTest(HashTableAccessType tableType, bool isMultiRegion, bool compactHashKey,
                                          bool enableTTL, bool isOnline, bool isShortOffset)
{
    if (isMultiRegion) {
        mKVConfig->SetRegionInfo(0, 2);
        isShortOffset = false;
    } else {
        mKVConfig->SetRegionInfo(0, 1);
    }
    if (compactHashKey) {
        mKVConfig->GetIndexPreference()._hashParam.SetEnableCompactHashKey(true);
    } else {
        mKVConfig->GetIndexPreference()._hashParam.SetEnableCompactHashKey(false);
    }
    if (tableType == HashTableAccessType::DENSE_READER || tableType == HashTableAccessType::DENSE_TABLE) {
        mKVConfig->GetIndexPreference()._hashParam._hashType = "dense";
    } else if (tableType == HashTableAccessType::CUCKOO_READER || tableType == HashTableAccessType::CUCKOO_TABLE) {
        mKVConfig->GetIndexPreference()._hashParam._hashType = "cuckoo";
    }

    bool useFileReader = false;
    if (tableType == HashTableAccessType::DENSE_READER || tableType == HashTableAccessType::CUCKOO_READER) {
        useFileReader = true;
    }
    if (enableTTL) {
        mKVConfig->SetTTL(10);
    } else {
        mKVConfig->SetTTL(INVALID_TTL);
    }
    string createType;
    KVMap nameInfo;
    KVSegmentOffsetReader reader;
    auto _ = HashTableVarCreator::CreateHashTableForReader(mKVConfig, useFileReader, isOnline, isShortOffset, nameInfo);
    (void)_;
    if (tableType == HashTableAccessType::DENSE_TABLE || tableType == HashTableAccessType::CUCKOO_TABLE) {
        createType = reader.GetTableName(nameInfo, isShortOffset, false);
    } else {
        createType = reader.GetTableName(nameInfo, isShortOffset, true);
    }

    string expectType;
    if (isOnline || tableType == HashTableAccessType::DENSE_TABLE || tableType == HashTableAccessType::DENSE_READER) {
        if (useFileReader) {
            expectType = "common::DenseHashTableFileReader<";
        } else {
            expectType = "common::DenseHashTable<";
        }
    } else {
        if (useFileReader) {
            expectType = "common::CuckooHashTableFileReader<";
        } else {
            expectType = "common::CuckooHashTable<";
        }
    }
    if (isMultiRegion) {
        expectType += "keytype_t,";
    } else {
        if (compactHashKey) {
            expectType += "compact_keytype_t,";
        } else {
            expectType += "keytype_t,";
        }
    }
    if (isMultiRegion) {
        if (enableTTL || isOnline) {
            expectType += "MultiRegionTimestampValue<offset_t>";
        } else {
            expectType += "MultiRegionTimestamp0Value<offset_t>";
        }
    } else {
        if (enableTTL || isOnline) {
            expectType += "common::TimestampValue<";
            if (isShortOffset) {
                expectType += "short_offset_t>";
            } else {
                expectType += "offset_t>";
            }
        } else {
            if (isShortOffset) {
                expectType += "common::OffsetValue<short_offset_t, std::numeric_limits<short_offset_t>::max(),"
                              " std::numeric_limits<short_offset_t>::max() - 1>";
            } else {
                expectType += "common::OffsetValue<offset_t, std::numeric_limits<offset_t"
                              ">::max(), std::numeric_limits<offset_t>::max() - 1>";
            }
        }
    }
    expectType += ",false,false>";
    ASSERT_EQ(expectType, createType);
}
}} // namespace indexlib::index
