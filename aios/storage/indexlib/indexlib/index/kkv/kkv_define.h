/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>
#include <unordered_set>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common/chunk/chunk_define.h"
#include "indexlib/common/hash_table/closed_hash_table_traits.h"
#include "indexlib/common/hash_table/cuckoo_hash_table_traits.h"
#include "indexlib/common/hash_table/dense_hash_table_traits.h"
#include "indexlib/common/hash_table/separate_chain_hash_table.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/kkv_docs.h"
#include "indexlib/index/kkv/kkv_value_writer.h"
#include "indexlib/index/kkv/on_disk_skey_node.h"
#include "indexlib/index/kkv/on_disk_value_inline_skey_node.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PrimeNumberTable.h"

namespace indexlib { namespace index {

enum PKeyTableOpenType {
    PKOT_READ,
    PKOT_RW,
    PKOT_WRITE,
    PKOT_UNKNOWN,
};

enum PKeyTableType {
    PKT_SEPARATE_CHAIN,
    PKT_DENSE,
    PKT_CUCKOO,
    PKT_ARRAY,
    PKT_UNKNOWN,
};

typedef uint64_t PKeyType;

typedef KKVValueWriter ValueWriter;
DEFINE_SHARED_PTR(ValueWriter);

template <typename ValueType, PKeyTableType Type>
struct ClosedHashPrefixKeyTableTraits;
template <typename ValueType>
struct ClosedHashPrefixKeyTableTraits<ValueType, PKT_DENSE> {
    typedef common::DenseHashTableTraits<PKeyType, ValueType, false> Traits;
};
template <typename ValueType>
struct ClosedHashPrefixKeyTableTraits<ValueType, PKT_CUCKOO> {
    typedef common::CuckooHashTableTraits<PKeyType, ValueType, false> Traits;
};

inline FieldType GetSKeyDictKeyType(FieldType orgType)
{
    if (orgType == ft_string) {
        return ft_uint64;
    }
    return orgType;
}

constexpr uint32_t INVALID_SKEY_OFFSET = std::numeric_limits<uint32_t>::max();
constexpr uint64_t INVALID_VALUE_OFFSET = std::numeric_limits<uint64_t>::max();
constexpr uint64_t SKEY_ALL_DELETED_OFFSET = std::numeric_limits<uint64_t>::max() - 1;
constexpr uint64_t ON_DISK_INVALID_SKEY_OFFSET = std::numeric_limits<uint64_t>::max();

constexpr uint64_t KKV_CHUNK_SIZE_THRESHOLD = (uint64_t)4 * 1024;
constexpr uint64_t MAX_KKV_VALUE_LEN = common::MAX_CHUNK_DATA_LEN - KKV_CHUNK_SIZE_THRESHOLD;

extern const std::string KKV_SEGMENT_MEM_USE;
extern const std::string KKV_PKEY_MEM_USE;
extern const std::string KKV_PKEY_COUNT;
extern const std::string KKV_SKEY_COUNT;
extern const std::string KKV_MAX_VALUE_LEN;
extern const std::string KKV_MAX_SKEY_COUNT;
extern const std::string KKV_COMPRESS_RATIO_GROUP_NAME;

extern const std::string KKV_SKEY_MEM_USE;
extern const std::string KKV_VALUE_MEM_USE;
extern const std::string KKV_SKEY_VALUE_MEM_RATIO;

#pragma pack(push, 4)
struct SKeyListInfo {
    uint32_t skeyHeader;
    uint32_t blockHeader;
    uint32_t count;
    regionid_t regionId;

    SKeyListInfo()
        : skeyHeader(INVALID_SKEY_OFFSET)
        , blockHeader(INVALID_SKEY_OFFSET)
        , count(0)
        , regionId(DEFAULT_REGIONID)
    {
    }

    SKeyListInfo(uint32_t _skeyHeader, uint32_t _blockHeader, uint32_t _count, regionid_t _regionId = DEFAULT_REGIONID)
        : skeyHeader(_skeyHeader)
        , blockHeader(_blockHeader)
        , count(_count)
        , regionId(_regionId)
    {
    }

    inline bool operator==(const SKeyListInfo& rhs) const
    {
        return skeyHeader == rhs.skeyHeader && blockHeader == rhs.blockHeader && count == rhs.count &&
               regionId == rhs.regionId;
    }
};
#pragma pack(pop)

struct OnDiskPKeyOffset {
    OnDiskPKeyOffset()
        : chunkOffset(INVALID_PKEY_CHUNK_OFFSET)
        , inChunkOffset(0)
        , regionId(DEFAULT_REGIONID)
        , blockHint(0)
    {
    }

    OnDiskPKeyOffset(uint64_t _chunkOffset, uint64_t _inChunkOffset, uint64_t _regionId = DEFAULT_REGIONID)
        : chunkOffset(_chunkOffset)
        , inChunkOffset(_inChunkOffset)
        , regionId(_regionId)
    {
    }

    OnDiskPKeyOffset(uint64_t value)
    {
        static_assert(sizeof(*this) == sizeof(value), "to guarantee equal size");
        void* addr = &value;
        *this = *((OnDiskPKeyOffset*)addr);
    }

    inline bool operator==(const OnDiskPKeyOffset& rhs) const
    {
        return chunkOffset == rhs.chunkOffset && inChunkOffset == rhs.inChunkOffset && regionId == rhs.regionId;
    }

    inline bool operator<(const OnDiskPKeyOffset& other) const
    {
        if (chunkOffset != other.chunkOffset) {
            return chunkOffset < other.chunkOffset;
        }
        return inChunkOffset < other.inChunkOffset;
    }

    bool IsValidOffset() const { return chunkOffset != INVALID_PKEY_CHUNK_OFFSET; }

    uint64_t ToU64Value() const { return *((uint64_t*)this); }

    void SetBlockHint(uint64_t offset) { blockHint = CalculateBlockHint(chunkOffset, offset); }

    uint64_t GetBlockOffset() const { return chunkOffset - chunkOffset % HINT_BLOCK_SIZE; }

    uint64_t GetHintSize() const { return (blockHint + 1) * HINT_BLOCK_SIZE; }

    uint64_t chunkOffset   : 40;
    uint64_t inChunkOffset : 12;
    uint64_t regionId      : 10;
    uint64_t blockHint     : 2;

public:
    static constexpr uint64_t INVALID_PKEY_CHUNK_OFFSET = ((uint64_t)1 << 40) - 1;
    static constexpr uint64_t MAX_VALID_CHUNK_OFFSET = INVALID_PKEY_CHUNK_OFFSET - 1;
    static constexpr uint64_t HINT_BLOCK_SIZE = 4 * 1024; // 4 KB

public:
    static uint64_t CalculateBlockHint(uint64_t startOffset, uint64_t endOffset)
    {
        assert(endOffset > startOffset);
        int64_t endBlockIdx = endOffset / HINT_BLOCK_SIZE;
        if (endOffset % HINT_BLOCK_SIZE != 0) {
            endBlockIdx++;
        }
        int64_t hint = endBlockIdx - startOffset / HINT_BLOCK_SIZE;
        if (hint > 4) {
            hint = 4;
        }
        return hint - 1;
    }

} __attribute__((packed));

extern const std::string KKV_PKEY_DENSE_TABLE_NAME;
extern const std::string KKV_PKEY_CUCKOO_TABLE_NAME;
extern const std::string KKV_PKEY_SEPARATE_CHAIN_TABLE_NAME;
extern const std::string KKV_PKEY_ARRAR_TABLE_NAME;

inline PKeyTableType GetPrefixKeyTableType(const std::string& typeStr)
{
    if (typeStr == KKV_PKEY_DENSE_TABLE_NAME) {
        return PKT_DENSE;
    }
    if (typeStr == KKV_PKEY_CUCKOO_TABLE_NAME) {
        return PKT_CUCKOO;
    }
    if (typeStr == KKV_PKEY_SEPARATE_CHAIN_TABLE_NAME) {
        return PKT_SEPARATE_CHAIN;
    }
    if (typeStr == KKV_PKEY_ARRAR_TABLE_NAME) {
        return PKT_ARRAY;
    }
    INDEXLIB_THROW(util::UnSupportedException, "invalid hash type [%s]!", typeStr.c_str());
    return PKT_UNKNOWN;
}

inline std::string PrefixKeyTableType2Str(PKeyTableType type)
{
    if (type == PKT_DENSE) {
        return KKV_PKEY_DENSE_TABLE_NAME;
    }
    if (type == PKT_CUCKOO) {
        return KKV_PKEY_CUCKOO_TABLE_NAME;
    }
    if (type == PKT_SEPARATE_CHAIN) {
        return KKV_PKEY_SEPARATE_CHAIN_TABLE_NAME;
    }
    if (type == PKT_ARRAY) {
        return KKV_PKEY_ARRAR_TABLE_NAME;
    }
    return std::string("");
}

inline config::KKVIndexConfigPtr CreateKKVIndexConfigForMultiRegionData(const config::IndexPartitionSchemaPtr& schema)
{
    assert(schema->GetRegionCount() > 1);
    const config::IndexSchemaPtr& indexSchema = schema->GetIndexSchema(DEFAULT_REGIONID);
    config::KKVIndexConfigPtr firstRegionKKVConfig =
        DYNAMIC_POINTER_CAST(config::KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    // use first region kkv index config as default, and change skey field type to uint64
    config::IndexConfigPtr config(firstRegionKKVConfig->Clone());
    config::KKVIndexConfigPtr dataKKVConfig = DYNAMIC_POINTER_CAST(config::KKVIndexConfig, config);

    config::FieldConfigPtr fieldConfig(dataKKVConfig->GetSuffixFieldConfig()->Clone());
    fieldConfig->SetFieldType(ft_uint64);
    dataKKVConfig->SetSuffixFieldConfig(fieldConfig);
    return dataKKVConfig;
}

inline config::KKVIndexConfigPtr CreateDataKKVIndexConfig(const config::IndexPartitionSchemaPtr& schema)
{
    if (schema->GetRegionCount() > 1) {
        return CreateKKVIndexConfigForMultiRegionData(schema);
    }
    return DYNAMIC_POINTER_CAST(config::KKVIndexConfig,
                                schema->GetIndexSchema(DEFAULT_REGIONID)->GetPrimaryKeyIndexConfig());
}

inline void ResolveSkeyOffset(const OnDiskSKeyOffset& skeyOffset, uint64_t& chunkOffset, uint32_t& inChunkOffset)
{
    chunkOffset = skeyOffset.chunkOffset << OnDiskValueChunkAlignBit;
    inChunkOffset = skeyOffset.inChunkOffset;
}

template <typename SKeyType>
class SKeyHashContainerTraits
{
public:
    typedef std::vector<SKeyType> SKeyVector;
    typedef std::vector<SKeyType, autil::mem_pool::pool_allocator<SKeyType>> PooledSKeyVector;

    typedef std::unordered_set<SKeyType> SKeySet;
    typedef std::unordered_set<SKeyType, std::hash<SKeyType>, std::equal_to<SKeyType>,
                               autil::mem_pool::pool_allocator<SKeyType>>
        PooledSKeySet;
};

class KKVResultBuffer
{
public:
    KKVResultBuffer(size_t bufferSize, uint32_t skeyCountLimit, autil::mem_pool::Pool* pool)
        : mBufferSize(bufferSize)
        , mCursor(0)
        , mBuffer(pool)
        , mResultLimit(skeyCountLimit)
        , mIsEof(false)
    {
        assert(pool);
    }

public:
    // for read
    bool IsEof() const { return mIsEof && !IsValid(); }
    uint64_t GetCurrentSkey() const { return mBuffer[mCursor].skey; }

    void GetCurrentValue(autil::StringView& value) const
    {
        assert(!mBuffer[mCursor].skeyDeleted);
        value = mBuffer[mCursor].value;
    }

    bool IsCurrentDocInCache() const { return mBuffer[mCursor].inCache; }
    bool IsValid() const { return mCursor < mBuffer.size(); }
    void MoveToNext()
    {
        while (true) {
            mCursor++;
            if (!mBuffer[mCursor].duplicatedKey) {
                break;
            }
            if (!IsValid()) {
                break;
            }
        }
    }
    uint64_t GetCurrentTimestamp() const { return mBuffer[mCursor].timestamp; }
    uint64_t GetResultCount() const { return mBuffer.size(); }

public:
    // for write
    void swap(KKVDocs& docs) { mBuffer.swap(docs); }
    bool IsFull() const { return mBuffer.size() >= mBufferSize; }
    void Clear()
    {
        mBuffer.clear();
        mCursor = 0;
    }
    void SetEof() { mIsEof = true; }
    size_t GetCapacity() { return mBufferSize; }
    void EmplaceBack(KKVDoc&& doc)
    {
        mResultLimit--;
        mBuffer.emplace_back(std::move(doc));
    }
    bool ReachLimit() { return mResultLimit <= 0; }
    KKVDoc& operator[](size_t idx) { return mBuffer[idx]; }
    const KKVDoc& operator[](size_t idx) const { return mBuffer[idx]; }

    KKVDoc& Back() { return mBuffer.back(); }
    const KKVDoc& Back() const { return mBuffer.back(); }

    size_t Size() const noexcept { return mBuffer.size(); }
    // std::vector<KKVResult>& GetInnerBuffer() { return mBuffer; }

private:
    size_t mBufferSize;
    size_t mCursor;
    KKVDocs mBuffer;
    uint32_t mResultLimit;
    bool mIsEof;
};

inline bool SKeyExpired(const config::KVIndexConfig& kvConfig, uint64_t currentTsInSecond, uint64_t docTsInSecond,
                        uint64_t docExpireTime)
{
    if (!kvConfig.StoreExpireTime()) {
        return false;
    }
    if (docExpireTime == UNINITIALIZED_EXPIRE_TIME) {
        int64_t configTTL = kvConfig.GetTTL();
        assert(configTTL > 0); // setted by SchemaRewriter
        docExpireTime = docTsInSecond + configTTL;
    }
    return docExpireTime < currentTsInSecond;
}

}} // namespace indexlib::index

namespace indexlib { namespace common {

// for non delete value
template <typename _RealVT>
class NonDeleteValue
{
public:
    NonDeleteValue() : mValue(std::numeric_limits<_RealVT>::max()) {}

    NonDeleteValue(const _RealVT& value) : mValue(value) {}

public:
    // for User
    typedef _RealVT ValueType;
    const _RealVT& Value() const { return mValue; }
    _RealVT& Value() { return mValue; }

public:
    // for NonDeleteValueBucket
    bool IsEmpty() const { return mValue == std::numeric_limits<_RealVT>::max(); }
    bool IsDeleted() const { return false; }
    void SetValue(const NonDeleteValue<_RealVT>& value) { mValue = value.Value(); }
    void SetDelete(const NonDeleteValue<_RealVT>& value) { assert(false); }
    void SetEmpty() { mValue = std::numeric_limits<_RealVT>::max(); }
    bool operator<(const NonDeleteValue<_RealVT>& other) const { return mValue < other.mValue; }
    bool operator==(const NonDeleteValue<_RealVT>& other) const { return mValue == other.mValue; }

private:
    _RealVT mValue;
};

template <>
inline NonDeleteValue<index::SKeyListInfo>::NonDeleteValue() : mValue(index::SKeyListInfo())
{
}

template <>
inline bool NonDeleteValue<index::SKeyListInfo>::IsEmpty() const
{
    return mValue == index::SKeyListInfo();
}

template <>
inline void NonDeleteValue<index::SKeyListInfo>::SetEmpty()
{
    mValue = index::SKeyListInfo();
}

// for non delete value
template <typename _KT, typename _RealVT>
struct ClosedHashTableTraits<_KT, NonDeleteValue<_RealVT>, false> {
    static const bool HasSpecialKey = false;
    static const bool UseCompactBucket = false;
    typedef NonDeleteValue<_RealVT> _VT;
    typedef SpecialValueBucket<_KT, _VT> Bucket;
};
}} // namespace indexlib::common
