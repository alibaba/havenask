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
#ifndef __INDEXLIB_CLOSED_HASH_PREFIX_KEY_TABLE_H
#define __INDEXLIB_CLOSED_HASH_PREFIX_KEY_TABLE_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/kkv/closed_hash_prefix_key_table_iterator.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/prefix_key_table_base.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/MmapPool.h"

namespace indexlib { namespace index {

template <typename ValueType, PKeyTableType Type>
class ClosedHashPrefixKeyTable : public PrefixKeyTableBase<ValueType>
{
private:
    static const size_t HASH_TABLE_INIT_SIZE = 256 * 1024;

public:
    typedef typename ClosedHashPrefixKeyTableTraits<ValueType, Type>::Traits Traits;
    typedef typename Traits::HashTable RTable;
    typedef typename Traits::HashTable WTable;
    typedef typename Traits::FileReader RTableReader;
    typedef typename ClosedPKeyTableSpecialValueTraits<ValueType, Type>::TableType RWTable;
    typedef typename PrefixKeyTableBase<ValueType>::IteratorPtr IteratorPtr;

public:
    // for Read Open
    ClosedHashPrefixKeyTable(int32_t occupancyPct)
        : PrefixKeyTableBase<ValueType>(Type)
        , mTable(NULL)
        , mTableBaseAddr(NULL)
        , mMaxMemUse(0)
        , mOccupancyPct(occupancyPct)
    {
    }

    // for RW & Write Open
    ClosedHashPrefixKeyTable(size_t maxMemUse, int32_t occupancyPct)
        : PrefixKeyTableBase<ValueType>(Type)
        , mTable(NULL)
        , mTableBaseAddr(NULL)
        , mMaxMemUse(maxMemUse)
        , mOccupancyPct(occupancyPct)
    {
    }

    ~ClosedHashPrefixKeyTable() { Release(); }

public:
    bool Open(const file_system::DirectoryPtr& dir, PKeyTableOpenType openType) override final;

    void Close() override final;

    ValueType* Find(const uint64_t& key) const override final;

    bool Insert(const uint64_t& key, const ValueType& value) override final;
    IteratorPtr CreateIterator() override final;

    size_t GetTotalMemoryUse() const override final
    {
        switch (this->mOpenType) {
        case PKOT_RW:
            return ((RWTable*)mTable)->BuildAssistantMemoryUse() + mMaxMemUse;
        case PKOT_READ:
            return mMaxMemUse;
        case PKOT_WRITE:
            return ((WTable*)mTable)->BuildAssistantMemoryUse() + mMaxMemUse;
        default:
            assert(false);
        }
        return mMaxMemUse;
    }

    size_t Size() const override final;
    bool IsFull() const override final;

    ValueType* FindForRead(const uint64_t& key) const override final __attribute__((always_inline))
    {
        assert(this->mOpenType == PKOT_READ);
        if (unlikely(!mTable)) {
            return NULL;
        }
        const ValueType* value = NULL;
        assert(mTableBaseAddr);
        if (util::OK == ((const RTable*)mTable)->Find(key, value)) {
            return const_cast<ValueType*>(value);
        }
        return NULL;
    }

    FL_LAZY(bool) FindForRead(const uint64_t& key, ValueType& value, KVMetricsCollector* collector) const override final
    {
        assert(this->mOpenType == PKOT_READ);
        if (unlikely(!mTable)) {
            FL_CORETURN false;
        }
        if (mTableBaseAddr) {
            const ValueType* valuePtr = NULL;
            if (util::OK == ((RTable*)mTable)->Find(key, valuePtr)) {
                value = *valuePtr;
                FL_CORETURN true;
            }
            FL_CORETURN false;
        }
        util::BlockAccessCounter* blockCounter = collector ? collector->GetBlockCounter() : nullptr;
        FL_CORETURN(util::OK == FL_COAWAIT((RTableReader*)mTable)->Find(key, value, blockCounter, nullptr));
    }

    ValueType* FindForRW(const uint64_t& key) const override final __attribute__((always_inline))
    {
        assert(this->mOpenType == PKOT_RW);
        if (unlikely(!mTable)) {
            return NULL;
        }
        const typename ClosedPKeyTableSpecialValueTraits<ValueType, Type>::TableValueType* value = NULL;
        if (util::OK == ((RWTable*)mTable)->Find(key, value)) {
            return (ValueType*)&(value->Value());
        }
        return NULL;
    }

    int32_t GetOccupancyPct() const { return mOccupancyPct; }

public:
    static size_t CalculateMemoryUse(PKeyTableOpenType openType, size_t maxKeyCount, int32_t occupancyPct)
    {
        if (maxKeyCount == 0) {
            maxKeyCount = HASH_TABLE_INIT_SIZE;
        }

        switch (openType) {
        case PKOT_RW:
            return RWTable::DoCapacityToTableMemory(maxKeyCount, occupancyPct);
        case PKOT_READ:
            return RTable::DoCapacityToTableMemory(maxKeyCount, occupancyPct);
        case PKOT_WRITE: {
            typename Traits::Options options(occupancyPct);
            options.mayStretch = true;
            return WTable::DoCapacityToTableMemory(maxKeyCount, options);
        }
        default:
            assert(false);
        }
        return 0;
    }

    static size_t CalculateBuildMemoryUse(PKeyTableOpenType openType, size_t maxKeyCount, int32_t occupancyPct)
    {
        if (maxKeyCount == 0) {
            maxKeyCount = HASH_TABLE_INIT_SIZE;
        }

        switch (openType) {
        case PKOT_RW:
            return RWTable::DoCapacityToBuildMemory(maxKeyCount, occupancyPct);
        case PKOT_WRITE: {
            typename Traits::Options options(occupancyPct);
            options.mayStretch = true;
            return WTable::DoCapacityToBuildMemory(maxKeyCount, options);
        }
        default:
            assert(false);
        }
        return 0;
    }

    static size_t EstimateCapacity(PKeyTableOpenType openType, size_t maxMemUse, int32_t occupancyPct)
    {
        switch (openType) {
        case PKOT_RW:
            return RWTable::DoBuildMemoryToCapacity(maxMemUse, occupancyPct);
        case PKOT_WRITE:
            return WTable::DoBuildMemoryToCapacity(maxMemUse, occupancyPct);
        default:
            assert(false);
        }
        return 0;
    }

private:
    void Release();

private:
    void* mTable;
    char* mTableBaseAddr;
    size_t mMaxMemUse;
    int32_t mOccupancyPct;
    file_system::FileWriterPtr mInMemFile;
    file_system::FileReaderPtr mFileReader;
    util::MmapPool mPool;

private:
    IE_LOG_DECLARE();
};
///////////////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE2_CUSTOM(typename, PKeyTableType, index, ClosedHashPrefixKeyTable);

template <typename ValueType, PKeyTableType Type>
bool ClosedHashPrefixKeyTable<ValueType, Type>::Open(const file_system::DirectoryPtr& dir, PKeyTableOpenType openType)
{
    switch (openType) {
    case PKOT_READ: {
        mFileReader = dir->CreateFileReader(PREFIX_KEY_FILE_NAME, file_system::FSOT_LOAD_CONFIG);
        mTableBaseAddr = (char*)mFileReader->GetBaseAddress();
        if (!mTableBaseAddr) {
            mMaxMemUse = mFileReader->GetLength(); // TODO
            std::unique_ptr<RTableReader> table(new RTableReader);
            if (!table->Init(dir, mFileReader)) {
                return false;
            }
            mTable = table.release();
        } else {
            mMaxMemUse = mFileReader->GetLength();
            std::unique_ptr<RTable> table(new RTable);
            if (!table->MountForRead(mTableBaseAddr, mMaxMemUse)) {
                return false;
            }
            mTable = table.release();
        }
        break;
    }
    case PKOT_RW: {
        assert(mMaxMemUse > 0);
        mTableBaseAddr = (char*)mPool.allocate(mMaxMemUse);
        assert(mTableBaseAddr);
        std::unique_ptr<RWTable> table(new RWTable);
        if (!table->MountForWrite(mTableBaseAddr, mMaxMemUse, mOccupancyPct)) {
            return false;
        }
        mTable = table.release();
        break;
    }
    case PKOT_WRITE: {
        assert(mMaxMemUse > 0);
        mInMemFile = dir->CreateFileWriter(PREFIX_KEY_FILE_NAME, file_system::WriterOption::Mem(mMaxMemUse));
        mInMemFile->Truncate(mMaxMemUse).GetOrThrow(PREFIX_KEY_FILE_NAME);
        mTableBaseAddr = (char*)mInMemFile->GetBaseAddress();
        assert(mTableBaseAddr);
        std::unique_ptr<WTable> table(new WTable);
        typename Traits::Options options(mOccupancyPct);
        options.mayStretch = true;
        if (!table->MountForWrite(mTableBaseAddr, mMaxMemUse, options)) {
            return false;
        }
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

template <typename ValueType, PKeyTableType Type>
inline void ClosedHashPrefixKeyTable<ValueType, Type>::Release()
{
    if (!mTable) {
        return;
    }

    switch (this->mOpenType) {
    case PKOT_READ: {
        if (mTableBaseAddr) {
            RTable* table = (RTable*)mTable;
            DELETE_AND_SET_NULL(table);
        } else {
            RTableReader* table = (RTableReader*)mTable;
            DELETE_AND_SET_NULL(table);
        }
        break;
    }
    case PKOT_RW: {
        RWTable* table = (RWTable*)mTable;
        DELETE_AND_SET_NULL(table);

        if (mTableBaseAddr) {
            mPool.deallocate((void*)mTableBaseAddr, mMaxMemUse);
            mTableBaseAddr = NULL;
        }
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
    this->mOpenType = PKOT_UNKNOWN;
    mTable = NULL;
    mTableBaseAddr = NULL;
}

template <typename ValueType, PKeyTableType Type>
void ClosedHashPrefixKeyTable<ValueType, Type>::Close()
{
    if (!mTable) {
        return;
    }

    if (this->mOpenType == PKOT_WRITE && mInMemFile != NULL) {
        WTable* table = (WTable*)mTable;
        if (!table->Shrink()) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Shrink failed, file[%s]", mInMemFile->DebugString().c_str());
        }
        mInMemFile->Truncate(table->MemoryUse()).GetOrThrow();
        mInMemFile->Close().GetOrThrow();
        mInMemFile.reset();
    }
    Release();
}

template <typename ValueType, PKeyTableType Type>
ValueType* ClosedHashPrefixKeyTable<ValueType, Type>::Find(const uint64_t& key) const
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

template <typename ValueType, PKeyTableType Type>
bool ClosedHashPrefixKeyTable<ValueType, Type>::Insert(const uint64_t& key, const ValueType& value)
{
    if (!mTable) {
        return false;
    }

    switch (this->mOpenType) {
    case PKOT_RW: {
        return ((RWTable*)mTable)->Insert(key, ClosedPKeyTableSpecialValueTraits<ValueType, Type>::ToStoreValue(value));
    }
    case PKOT_WRITE: {
        WTable* table = (WTable*)mTable;
        if (likely(table->Insert(key, value))) {
            return true;
        }
        IE_LOG(WARN, "Insert key[%lu] failed, trigger stretch and retry, file[%s]", key,
               (mInMemFile ? mInMemFile->DebugString().c_str() : ""));
        if (table->Stretch() && table->Insert(key, value)) {
            return true;
        }
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Insert[%lu] failed, file[%s]", key,
                             (mInMemFile ? mInMemFile->DebugString().c_str() : ""));
    }
    default:
        assert(false);
    }
    return false;
}

template <typename ValueType, PKeyTableType Type>
typename ClosedHashPrefixKeyTable<ValueType, Type>::IteratorPtr
ClosedHashPrefixKeyTable<ValueType, Type>::CreateIterator()
{
    switch (this->mOpenType) {
    case PKOT_RW: {
        typedef ClosedHashPrefixKeyTableIterator<ValueType, Type> Iterator;
        return IteratorPtr(new Iterator(((RWTable*)mTable)->CreateIterator()));
    }
    case PKOT_WRITE:
    case PKOT_READ:
    default:
        assert(false);
    }
    return IteratorPtr();
}

template <typename ValueType, PKeyTableType Type>
size_t ClosedHashPrefixKeyTable<ValueType, Type>::Size() const
{
    if (!mTable) {
        return 0;
    }

    switch (this->mOpenType) {
    case PKOT_RW:
        return ((RWTable*)mTable)->Size();
    case PKOT_READ:
        return mTableBaseAddr ? ((RTable*)mTable)->Size() : ((RTableReader*)mTable)->Size();
    case PKOT_WRITE:
        return ((WTable*)mTable)->Size();
    default:
        assert(false);
    }
    return 0;
}

template <typename ValueType, PKeyTableType Type>
bool ClosedHashPrefixKeyTable<ValueType, Type>::IsFull() const
{
    if (!mTable) {
        return 0;
    }

    switch (this->mOpenType) {
    case PKOT_RW:
        return ((RWTable*)mTable)->IsFull();
    case PKOT_READ:
        return mTableBaseAddr ? ((RTable*)mTable)->IsFull() : ((RTableReader*)mTable)->IsFull();
    case PKOT_WRITE:
        return ((WTable*)mTable)->IsFull();
    default:
        assert(false);
    }
    return 0;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_CLOSED_HASH_PREFIX_KEY_TABLE_H
