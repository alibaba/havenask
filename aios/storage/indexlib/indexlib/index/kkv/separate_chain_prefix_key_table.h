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
#ifndef __INDEXLIB_SEPARATE_CHAIN_PREFIX_KEY_TABLE_H
#define __INDEXLIB_SEPARATE_CHAIN_PREFIX_KEY_TABLE_H

#include <memory>

#include "indexlib/common/hash_table/hash_table_reader.h"
#include "indexlib/common/hash_table/hash_table_writer.h"
#include "indexlib/common/hash_table/separate_chain_hash_table.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/prefix_key_table_base.h"
#include "indexlib/index/kkv/separate_chain_prefix_key_table_iterator.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename ValueType>
class SeparateChainPrefixKeyTable : public PrefixKeyTableBase<ValueType>
{
private:
    static constexpr double SEPARATE_CHAIN_HASH_TABLE_FACTOR = 1.33;
    static constexpr size_t SEPARATE_CHAIN_HASH_TABLE_INIT_SIZE = 1024 * 1024;

    typedef common::SeparateChainHashTable<uint64_t, ValueType> RWTable;
    typedef common::HashTableReader<uint64_t, ValueType> RTable;
    typedef common::HashTableWriter<uint64_t, ValueType> WTable;
    typedef typename PrefixKeyTableBase<ValueType>::IteratorPtr IteratorPtr;

public:
    // for Read Open
    SeparateChainPrefixKeyTable()
        : PrefixKeyTableBase<ValueType>(PKT_SEPARATE_CHAIN)
        , mTable(NULL)
        , mMaxMemUse(0)
        , mBucketCount(0)
        , mRecommendedBucketCount(0)
    {
    }

    // for RW & Write Open
    SeparateChainPrefixKeyTable(uint32_t bucketCount, size_t maxMemUse)
        : PrefixKeyTableBase<ValueType>(PKT_SEPARATE_CHAIN)
        , mTable(NULL)
        , mMaxMemUse(maxMemUse)
        , mBucketCount(bucketCount)
        , mRecommendedBucketCount(bucketCount)
    {
    }

    ~SeparateChainPrefixKeyTable() { Release(); }

public:
    bool Open(const file_system::DirectoryPtr& dir, PKeyTableOpenType openType) override final;

    void Close() override final;
    size_t GetTotalMemoryUse() const override final;

    size_t Size() const override final;
    ValueType* Find(const uint64_t& key) const override final;
    bool Insert(const uint64_t& key, const ValueType& value) override final;
    IteratorPtr CreateIterator() override final;
    size_t GetRecommendBucketCount() const
    {
        size_t pkeyCount = Size();
        if (pkeyCount * SEPARATE_CHAIN_HASH_TABLE_FACTOR > mRecommendedBucketCount) {
            mRecommendedBucketCount = GetRecommendBucketCount(pkeyCount);
        }
        return mRecommendedBucketCount;
    }

    ValueType* FindForRead(const uint64_t& key) const override final __attribute__((always_inline))
    {
        assert(this->mOpenType == PKOT_READ);
        if (unlikely(!mTable)) {
            return NULL;
        }
        return const_cast<ValueType*>(((RTable*)mTable)->Find(key));
    }

    FL_LAZY(bool) FindForRead(const uint64_t& key, ValueType& value, KVMetricsCollector* collector) const override final
    {
        auto ret = FindForRead(key);
        if (ret) {
            value = *ret;
        }
        FL_CORETURN ret != NULL;
    }

    ValueType* FindForRW(const uint64_t& key) const override final __attribute__((always_inline))
    {
        assert(this->mOpenType == PKOT_RW);
        if (unlikely(!mTable)) {
            return NULL;
        }
        RWTable* table = (RWTable*)mTable;
        typename RWTable::KeyNode* node = table->Find(key);
        return node ? &(node->value) : NULL;
    }

public:
    static size_t GetRecommendBucketCount(size_t pkeyCount)
    {
        if (pkeyCount < SEPARATE_CHAIN_HASH_TABLE_INIT_SIZE) {
            pkeyCount = SEPARATE_CHAIN_HASH_TABLE_INIT_SIZE;
        }
        return util::PrimeNumberTable::FindPrimeNumber(pkeyCount * SEPARATE_CHAIN_HASH_TABLE_FACTOR);
    }

    static size_t GetDumpSize(size_t pkeyCount, size_t bucketCount)
    {
        return common::HashTableWriter<uint64_t, ValueType>::GetDumpSize(bucketCount, pkeyCount);
    }

    static size_t EstimateCapacity(PKeyTableOpenType type, size_t maxMemUse)
    {
        size_t nodeSize =
            (type == PKOT_RW) ? sizeof(typename RWTable::KeyNode) : sizeof(common::HashTableNode<uint64_t, ValueType>);
        size_t spacePerItem = SEPARATE_CHAIN_HASH_TABLE_FACTOR * sizeof(uint32_t) + nodeSize;
        return maxMemUse / spacePerItem;
    }

private:
    void Release();

private:
    void* mTable;
    size_t mMaxMemUse;
    uint32_t mBucketCount;
    mutable uint32_t mRecommendedBucketCount;

private:
    IE_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////////////
template <typename ValueType>
bool SeparateChainPrefixKeyTable<ValueType>::Open(const file_system::DirectoryPtr& dir, PKeyTableOpenType openType)
{
    switch (openType) {
    case PKOT_READ: {
        std::unique_ptr<RTable> table(new RTable);
        table->Open(dir, PREFIX_KEY_FILE_NAME);
        mMaxMemUse = table->GetFileLength();
        mBucketCount = table->GetBucketCount();
        mRecommendedBucketCount = mBucketCount;
        mTable = table.release();
        break;
    }
    case PKOT_RW: {
        std::unique_ptr<RWTable> table(new RWTable);
        table->Init(mBucketCount, mMaxMemUse);
        mTable = table.release();
        break;
    }
    case PKOT_WRITE: {
        std::unique_ptr<WTable> table(new WTable);
        table->Init(dir, PREFIX_KEY_FILE_NAME, mBucketCount);
        table->GetFileWriter()->ReserveFile(mMaxMemUse).GetOrThrow();
        mTable = table.release();
        break;
    }
    default:
        assert(false);
        return false;
    }
    this->mOpenType = openType;
    return true;
}

template <typename ValueType>
inline void SeparateChainPrefixKeyTable<ValueType>::Release()
{
    if (!mTable) {
        return;
    }

    switch (this->mOpenType) {
    case PKOT_READ: {
        RTable* table = (RTable*)mTable;
        DELETE_AND_SET_NULL(table);
        break;
    }
    case PKOT_RW: {
        RWTable* table = (RWTable*)mTable;
        DELETE_AND_SET_NULL(table);
        break;
    }
    case PKOT_WRITE: {
        WTable* table = (WTable*)mTable;
        DELETE_AND_SET_NULL(table);
        break;
    }
    default:
        assert(false);
    }
    mTable = NULL;
    this->mOpenType = PKOT_UNKNOWN;
}

template <typename ValueType>
void SeparateChainPrefixKeyTable<ValueType>::Close()
{
    if (!mTable) {
        return;
    }

    if (this->mOpenType == PKOT_WRITE) {
        WTable* table = (WTable*)mTable;
        table->Close();
    }
    Release();
}

template <typename ValueType>
size_t SeparateChainPrefixKeyTable<ValueType>::GetTotalMemoryUse() const
{
    if (!mTable) {
        return 0;
    }

    switch (this->mOpenType) {
    case PKOT_READ: {
        return ((RTable*)mTable)->GetFileLength();
    }
    case PKOT_RW: {
        return ((RWTable*)mTable)->GetUsedBytes();
    }
    case PKOT_WRITE: {
        size_t itemCount = ((WTable*)mTable)->Size();
        return WTable::GetDumpSize(mBucketCount, itemCount);
    }
    default:
        assert(false);
    }
    return 0;
}

template <typename ValueType>
size_t SeparateChainPrefixKeyTable<ValueType>::Size() const
{
    if (!mTable) {
        return 0;
    }

    switch (this->mOpenType) {
    case PKOT_READ: {
        return ((RTable*)mTable)->GetNodeCount();
    }
    case PKOT_RW: {
        return ((RWTable*)mTable)->Size();
    }
    case PKOT_WRITE: {
        return ((WTable*)mTable)->Size();
    }
    default:
        assert(false);
    }
    return 0;
}

template <typename ValueType>
ValueType* SeparateChainPrefixKeyTable<ValueType>::Find(const uint64_t& key) const
{
    if (!mTable) {
        return NULL;
    }

    switch (this->mOpenType) {
    case PKOT_READ: {
        return FindForRead(key);
    }
    case PKOT_RW: {
        return FindForRW(key);
    }
    default:
        assert(false);
    }
    return NULL;
}

template <typename ValueType>
bool SeparateChainPrefixKeyTable<ValueType>::Insert(const uint64_t& key, const ValueType& value)
{
    if (!mTable) {
        return false;
    }

    switch (this->mOpenType) {
    case PKOT_RW: {
        ((RWTable*)mTable)->Insert(key, value);
        return true;
    }
    case PKOT_WRITE: {
        ((WTable*)mTable)->Add(key, value);
        return true;
    }
    default:
        assert(false);
    }
    return false;
}

template <typename ValueType>
typename SeparateChainPrefixKeyTable<ValueType>::IteratorPtr SeparateChainPrefixKeyTable<ValueType>::CreateIterator()
{
    switch (this->mOpenType) {
    case PKOT_RW: {
        typedef SeparateChainPrefixKeyTableIterator<ValueType> Iterator;
        return IteratorPtr(new Iterator(((RWTable*)mTable)->CreateKeyNodeIterator()));
    }
    case PKOT_WRITE:
    case PKOT_READ:
    default:
        assert(false);
    }
    return IteratorPtr();
}
}} // namespace indexlib::index

#endif //__INDEXLIB_SEPARATE_CHAIN_PREFIX_KEY_TABLE_H
