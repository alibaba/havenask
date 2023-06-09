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

#include "indexlib/index/common/hash_table/HashTableReader.h"
#include "indexlib/index/common/hash_table/HashTableWriter.h"
#include "indexlib/index/common/hash_table/SeparateChainHashTable.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableBase.h"
#include "indexlib/index/kkv/pkey_table/SeparateChainPrefixKeyTableIterator.h"
#include "indexlib/util/PrimeNumberTable.h"

namespace indexlibv2::index {

template <typename ValueType>
class SeparateChainPrefixKeyTable : public PrefixKeyTableBase<ValueType>
{
private:
    static constexpr double SEPARATE_CHAIN_HASH_TABLE_FACTOR = 1.33;
    static constexpr size_t SEPARATE_CHAIN_HASH_TABLE_INIT_SIZE = 1024 * 1024;
    using RWTable = SeparateChainHashTable<uint64_t, ValueType>;
    using RTable = HashTableReader<uint64_t, ValueType>;
    using WTable = HashTableWriter<uint64_t, ValueType>;
    using Iterator = typename PrefixKeyTableBase<ValueType>::Iterator;

public:
    // for Read Open
    SeparateChainPrefixKeyTable()
        : PrefixKeyTableBase<ValueType>(PKeyTableType::SEPARATE_CHAIN)
        , _table(nullptr)
        , _maxMemUse(0)
        , _bucketCount(0)
        , _recommendedBucketCount(0)
    {
    }

    // for RW & Write Open
    SeparateChainPrefixKeyTable(uint32_t bucketCount, size_t maxMemUse)
        : PrefixKeyTableBase<ValueType>(PKeyTableType::SEPARATE_CHAIN)
        , _table(nullptr)
        , _maxMemUse(maxMemUse)
        , _bucketCount(bucketCount)
        , _recommendedBucketCount(bucketCount)
    {
    }

    ~SeparateChainPrefixKeyTable() { Release(); }

public:
    bool Open(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, PKeyTableOpenType openType) override final;

    void Close() override final;
    size_t GetTotalMemoryUse() const override final;

    size_t Size() const override final;
    ValueType* Find(const uint64_t& key) const override final;
    bool Insert(const uint64_t& key, const ValueType& value) override final;
    std::shared_ptr<Iterator> CreateIterator() const override final;
    size_t GetRecommendBucketCount() const
    {
        size_t pkeyCount = Size();
        if (pkeyCount * SEPARATE_CHAIN_HASH_TABLE_FACTOR > _recommendedBucketCount) {
            _recommendedBucketCount = GetRecommendBucketCount(pkeyCount);
        }
        return _recommendedBucketCount;
    }

    ValueType* FindForRead(const uint64_t& key) const override final __attribute__((always_inline))
    {
        assert(this->_openType == PKeyTableOpenType::READ);
        if (unlikely(!_table)) {
            return nullptr;
        }
        return const_cast<ValueType*>(((RTable*)_table)->Find(key));
    }

    FL_LAZY(bool)
    FindForRead(const uint64_t& key, ValueType& value, KKVMetricsCollector* collector) const override final
    {
        auto ret = FindForRead(key);
        if (ret) {
            value = *ret;
        }
        FL_CORETURN ret != nullptr;
    }

    ValueType* FindForRW(const uint64_t& key) const override final __attribute__((always_inline))
    {
        assert(this->_openType == PKeyTableOpenType::RW);
        if (unlikely(!_table)) {
            return nullptr;
        }
        RWTable* table = (RWTable*)_table;
        typename RWTable::KeyNode* node = table->Find(key);
        return node ? &(node->value) : nullptr;
    }

public:
    static size_t GetRecommendBucketCount(size_t pkeyCount)
    {
        if (pkeyCount < SEPARATE_CHAIN_HASH_TABLE_INIT_SIZE) {
            pkeyCount = SEPARATE_CHAIN_HASH_TABLE_INIT_SIZE;
        }
        return indexlib::util::PrimeNumberTable::FindPrimeNumber(pkeyCount * SEPARATE_CHAIN_HASH_TABLE_FACTOR);
    }

    static size_t GetDumpSize(size_t pkeyCount, size_t bucketCount)
    {
        return HashTableWriter<uint64_t, ValueType>::GetDumpSize(bucketCount, pkeyCount);
    }

    static size_t EstimateCapacity(PKeyTableOpenType type, size_t maxMemUse)
    {
        size_t nodeSize = (type == PKeyTableOpenType::RW) ? sizeof(typename RWTable::KeyNode)
                                                          : sizeof(HashTableNode<uint64_t, ValueType>);
        size_t spacePerItem = SEPARATE_CHAIN_HASH_TABLE_FACTOR * sizeof(uint32_t) + nodeSize;
        return maxMemUse / spacePerItem;
    }

private:
    void Release();

private:
    void* _table;
    size_t _maxMemUse;
    uint32_t _bucketCount;
    mutable uint32_t _recommendedBucketCount;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SeparateChainPrefixKeyTable, T);

/////////////////////////////////////////////////////////////////////////
template <typename ValueType>
bool SeparateChainPrefixKeyTable<ValueType>::Open(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                                  PKeyTableOpenType openType)
{
    switch (openType) {
    case PKeyTableOpenType::READ: {
        std::unique_ptr<RTable> table(new RTable);
        table->Open(dir, PREFIX_KEY_FILE_NAME);
        _maxMemUse = table->GetFileLength();
        _bucketCount = table->GetBucketCount();
        _recommendedBucketCount = _bucketCount;
        _table = table.release();
        break;
    }
    case PKeyTableOpenType::RW: {
        std::unique_ptr<RWTable> table(new RWTable);
        table->Init(_bucketCount, _maxMemUse);
        _table = table.release();
        break;
    }
    case PKeyTableOpenType::WRITE: {
        std::unique_ptr<WTable> table(new WTable);
        table->Init(dir, PREFIX_KEY_FILE_NAME, _bucketCount);
        table->GetFileWriter()->ReserveFile(_maxMemUse).GetOrThrow();
        _table = table.release();
        break;
    }
    default:
        assert(false);
        return false;
    }
    this->_openType = openType;
    return true;
}

template <typename ValueType>
inline void SeparateChainPrefixKeyTable<ValueType>::Release()
{
    if (!_table) {
        return;
    }

    switch (this->_openType) {
    case PKeyTableOpenType::READ: {
        RTable* table = (RTable*)_table;
        DELETE_AND_SET_NULL(table);
        break;
    }
    case PKeyTableOpenType::RW: {
        RWTable* table = (RWTable*)_table;
        DELETE_AND_SET_NULL(table);
        break;
    }
    case PKeyTableOpenType::WRITE: {
        WTable* table = (WTable*)_table;
        DELETE_AND_SET_NULL(table);
        break;
    }
    default:
        assert(false);
    }
    _table = nullptr;
    this->_openType = PKeyTableOpenType::UNKNOWN;
}

template <typename ValueType>
void SeparateChainPrefixKeyTable<ValueType>::Close()
{
    if (!_table) {
        return;
    }

    if (this->_openType == PKeyTableOpenType::WRITE) {
        WTable* table = (WTable*)_table;
        table->Close();
    }
    Release();
}

template <typename ValueType>
size_t SeparateChainPrefixKeyTable<ValueType>::GetTotalMemoryUse() const
{
    if (!_table) {
        return 0;
    }

    switch (this->_openType) {
    case PKeyTableOpenType::READ: {
        return ((RTable*)_table)->GetFileLength();
    }
    case PKeyTableOpenType::RW: {
        return ((RWTable*)_table)->GetUsedBytes();
    }
    case PKeyTableOpenType::WRITE: {
        size_t itemCount = ((WTable*)_table)->Size();
        return WTable::GetDumpSize(_bucketCount, itemCount);
    }
    default:
        assert(false);
    }
    return 0;
}

template <typename ValueType>
size_t SeparateChainPrefixKeyTable<ValueType>::Size() const
{
    if (!_table) {
        return 0;
    }

    switch (this->_openType) {
    case PKeyTableOpenType::READ: {
        return ((RTable*)_table)->GetNodeCount();
    }
    case PKeyTableOpenType::RW: {
        return ((RWTable*)_table)->Size();
    }
    case PKeyTableOpenType::WRITE: {
        return ((WTable*)_table)->Size();
    }
    default:
        assert(false);
    }
    return 0;
}

template <typename ValueType>
ValueType* SeparateChainPrefixKeyTable<ValueType>::Find(const uint64_t& key) const
{
    if (!_table) {
        return nullptr;
    }

    switch (this->_openType) {
    case PKeyTableOpenType::READ: {
        return FindForRead(key);
    }
    case PKeyTableOpenType::RW: {
        return FindForRW(key);
    }
    default:
        assert(false);
    }
    return nullptr;
}

template <typename ValueType>
bool SeparateChainPrefixKeyTable<ValueType>::Insert(const uint64_t& key, const ValueType& value)
{
    if (!_table) {
        return false;
    }

    switch (this->_openType) {
    case PKeyTableOpenType::RW: {
        ((RWTable*)_table)->Insert(key, value);
        return true;
    }
    case PKeyTableOpenType::WRITE: {
        ((WTable*)_table)->Add(key, value);
        return true;
    }
    default:
        assert(false);
    }
    return false;
}

template <typename ValueType>
std::shared_ptr<typename SeparateChainPrefixKeyTable<ValueType>::Iterator>
SeparateChainPrefixKeyTable<ValueType>::CreateIterator() const
{
    switch (this->_openType) {
    case PKeyTableOpenType::RW: {
        using Iterator = SeparateChainPrefixKeyTableIterator<ValueType>;
        return std::shared_ptr<Iterator>(new Iterator(((RWTable*)_table)->CreateKeyNodeIterator()));
    }
    case PKeyTableOpenType::WRITE:
    case PKeyTableOpenType::READ:
    default:
        assert(false);
    }
    return nullptr;
}

} // namespace indexlibv2::index
