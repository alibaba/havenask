#include "indexlib/index/kv/VarLenHashTableCreator.h"

#include "indexlib/index/common/hash_table/CuckooHashTable.h"
#include "indexlib/index/common/hash_table/DenseHashTable.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class VarLenHashTableCreatorTest : public TESTBASE
{
protected:
    template <typename HashTableType>
    void doTestCreateForWriter(KVIndexType indexType, bool compactHashKey, bool shortOffset, bool hasTTL) const
    {
        KVTypeId typeId;
        typeId.offlineIndexType = indexType;
        typeId.onlineIndexType = KVIndexType::KIT_DENSE_HASH;
        typeId.compactHashKey = compactHashKey;
        typeId.shortOffset = shortOffset;
        typeId.hasTTL = hasTTL;
        typeId.isVarLen = true;

        auto ret = VarLenHashTableCreator::CreateHashTableForWriter(typeId);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(ret->hashTable);
        ASSERT_TRUE(ret->valueUnpacker);
        ASSERT_TRUE(ret->bucketCompressor);
        ASSERT_TRUE(dynamic_cast<HashTableType*>(ret->hashTable.get())) << typeId.ToString();
    }
};

TEST_F(VarLenHashTableCreatorTest, testCreateForWriter)
{
    KVTypeId typeId;
    typeId.isVarLen = true;
    auto ret = VarLenHashTableCreator::CreateHashTableForWriter(typeId);
    ASSERT_FALSE(ret);

    // dense hash
    {
        using HashTableType = DenseHashTable<keytype_t, OffsetValue<offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_DENSE_HASH, false, false, false);
    }

    {
        using HashTableType = DenseHashTable<compact_keytype_t, OffsetValue<offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_DENSE_HASH, true, false, false);
    }

    {
        using HashTableType = DenseHashTable<keytype_t, OffsetValue<short_offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_DENSE_HASH, false, true, false);
    }

    {
        using HashTableType = DenseHashTable<compact_keytype_t, OffsetValue<short_offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_DENSE_HASH, true, true, false);
    }

    {
        using HashTableType = DenseHashTable<keytype_t, TimestampValue<offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_DENSE_HASH, false, false, true);
    }

    {
        using HashTableType = DenseHashTable<compact_keytype_t, TimestampValue<offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_DENSE_HASH, true, false, true);
    }

    {
        using HashTableType = DenseHashTable<keytype_t, TimestampValue<short_offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_DENSE_HASH, false, true, true);
    }

    {
        using HashTableType = DenseHashTable<compact_keytype_t, TimestampValue<short_offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_DENSE_HASH, true, true, true);
    }

    // cuckoo
    {
        using HashTableType = CuckooHashTable<keytype_t, OffsetValue<offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_CUCKOO_HASH, false, false, false);
    }

    {
        using HashTableType = CuckooHashTable<compact_keytype_t, OffsetValue<offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_CUCKOO_HASH, true, false, false);
    }

    {
        using HashTableType = CuckooHashTable<keytype_t, OffsetValue<short_offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_CUCKOO_HASH, false, true, false);
    }

    {
        using HashTableType = CuckooHashTable<compact_keytype_t, OffsetValue<short_offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_CUCKOO_HASH, true, true, false);
    }

    {
        using HashTableType = CuckooHashTable<keytype_t, TimestampValue<offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_CUCKOO_HASH, false, false, true);
    }

    {
        using HashTableType = CuckooHashTable<compact_keytype_t, TimestampValue<offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_CUCKOO_HASH, true, false, true);
    }

    {
        using HashTableType = CuckooHashTable<keytype_t, TimestampValue<short_offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_CUCKOO_HASH, false, true, true);
    }

    {
        using HashTableType = CuckooHashTable<compact_keytype_t, TimestampValue<short_offset_t>>;
        doTestCreateForWriter<HashTableType>(KVIndexType::KIT_CUCKOO_HASH, true, true, true);
    }
}

} // namespace indexlibv2::index
