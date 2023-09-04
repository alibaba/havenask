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

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/kkv/common/KKVMetricsCollector.h"
#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTableIterator.h"
#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTableTraits.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableBase.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/MmapPool.h"

namespace indexlibv2::index {

template <typename ValueType, PKeyTableType Type>
class ClosedHashPrefixKeyTable : public PrefixKeyTableBase<ValueType>
{
private:
    static constexpr size_t HASH_TABLE_INIT_SIZE = 256 * 1024;

public:
    using Traits = typename ClosedHashPrefixKeyTableTraits<ValueType, Type>::Traits;
    using RTable = typename Traits::HashTable;
    using WTable = typename Traits::HashTable;
    using RTableReader = typename Traits::FileReader;
    using RWTable = typename ClosedPKeyTableSpecialValueTraits<ValueType, Type>::TableType;
    using Iterator = typename PrefixKeyTableBase<ValueType>::Iterator;

public:
    // for Read Open
    ClosedHashPrefixKeyTable(int32_t occupancyPct)
        : PrefixKeyTableBase<ValueType>(Type)
        , _table(nullptr)
        , _tableBaseAddr(nullptr)
        , _maxMemUse(0)
        , _occupancyPct(occupancyPct)
    {
    }

    // for RW & Write Open
    ClosedHashPrefixKeyTable(size_t maxMemUse, int32_t occupancyPct)
        : PrefixKeyTableBase<ValueType>(Type)
        , _table(nullptr)
        , _tableBaseAddr(nullptr)
        , _maxMemUse(maxMemUse)
        , _occupancyPct(occupancyPct)
    {
    }

    ~ClosedHashPrefixKeyTable() { Release(); }

public:
    bool Open(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, PKeyTableOpenType openType) override final;

    void Close() override final;

    ValueType* Find(const uint64_t& key) const override final;

    bool Insert(const uint64_t& key, const ValueType& value) override final;
    std::shared_ptr<Iterator> CreateIterator() const override final;

    size_t GetTotalMemoryUse() const override final
    {
        switch (this->_openType) {
        case PKeyTableOpenType::RW:
            return ((RWTable*)_table)->BuildAssistantMemoryUse() + _maxMemUse;
        case PKeyTableOpenType::READ:
            return _maxMemUse;
        case PKeyTableOpenType::WRITE:
            return ((WTable*)_table)->BuildAssistantMemoryUse() + _maxMemUse;
        default:
            assert(false);
        }
        return _maxMemUse;
    }

    size_t Size() const override final;
    bool IsFull() const override final;

    ValueType* FindForRead(const uint64_t& key) const override final __attribute__((always_inline))
    {
        assert(this->_openType == PKeyTableOpenType::READ);
        if (unlikely(!_table)) {
            return nullptr;
        }
        const ValueType* value = nullptr;
        assert(_tableBaseAddr);
        if (indexlib::util::OK == ((const RTable*)_table)->Find(key, value)) {
            return const_cast<ValueType*>(value);
        }
        return nullptr;
    }

    FL_LAZY(bool)
    FindForRead(const uint64_t& key, ValueType& value, KKVMetricsCollector* collector) const override final
    {
        assert(this->_openType == PKeyTableOpenType::READ);
        if (unlikely(!_table)) {
            FL_CORETURN false;
        }
        if (_tableBaseAddr) {
            const ValueType* valuePtr = nullptr;
            if (indexlib::util::OK == ((RTable*)_table)->Find(key, valuePtr)) {
                value = *valuePtr;
                FL_CORETURN true;
            }
            FL_CORETURN false;
        }
        indexlib::util::BlockAccessCounter* blockCounter = collector ? collector->GetBlockCounter() : nullptr;
        FL_CORETURN(indexlib::util::OK == FL_COAWAIT((RTableReader*)_table)->Find(key, value, blockCounter, nullptr));
    }

    ValueType* FindForRW(const uint64_t& key) const override final __attribute__((always_inline))
    {
        assert(this->_openType == PKeyTableOpenType::RW);
        if (unlikely(!_table)) {
            return nullptr;
        }
        const typename ClosedPKeyTableSpecialValueTraits<ValueType, Type>::TableValueType* value = nullptr;
        if (indexlib::util::OK == ((RWTable*)_table)->Find(key, value)) {
            return (ValueType*)&(value->Value());
        }
        return nullptr;
    }

    int32_t GetOccupancyPct() const { return _occupancyPct; }

public:
    static size_t CalculateMemoryUse(PKeyTableOpenType openType, size_t maxKeyCount, int32_t occupancyPct)
    {
        if (maxKeyCount == 0) {
            maxKeyCount = HASH_TABLE_INIT_SIZE;
        }

        switch (openType) {
        case PKeyTableOpenType::RW:
            return RWTable::DoCapacityToTableMemory(maxKeyCount, occupancyPct);
        case PKeyTableOpenType::READ:
            return RTable::DoCapacityToTableMemory(maxKeyCount, occupancyPct);
        case PKeyTableOpenType::WRITE: {
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
        case PKeyTableOpenType::RW:
            return RWTable::DoCapacityToBuildMemory(maxKeyCount, occupancyPct);
        case PKeyTableOpenType::WRITE: {
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
        case PKeyTableOpenType::RW:
            return RWTable::DoBuildMemoryToCapacity(maxMemUse, occupancyPct);
        case PKeyTableOpenType::WRITE:
            return WTable::DoBuildMemoryToCapacity(maxMemUse, occupancyPct);
        default:
            assert(false);
        }
        return 0;
    }

private:
    void Release();

private:
    void* _table;
    char* _tableBaseAddr;
    size_t _maxMemUse;
    int32_t _occupancyPct;
    indexlib::file_system::FileWriterPtr _inMemFile;
    indexlib::file_system::FileReaderPtr _fileReader;
    indexlib::util::MmapPool _pool;

private:
    AUTIL_LOG_DECLARE();
};
///////////////////////////////////////////////////////////////////////////////////
template <typename ValueType, PKeyTableType Type>
alog::Logger* ClosedHashPrefixKeyTable<ValueType, Type>::_logger =
    alog::Logger::getLogger("indexlib.index.ClosedHashPrefixKeyTable");

template <typename ValueType, PKeyTableType Type>
bool ClosedHashPrefixKeyTable<ValueType, Type>::Open(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                                     PKeyTableOpenType openType)
{
    switch (openType) {
    case PKeyTableOpenType::READ: {
        _fileReader = dir->CreateFileReader(PREFIX_KEY_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG).GetOrThrow();
        _tableBaseAddr = (char*)_fileReader->GetBaseAddress();
        if (!_tableBaseAddr) {
            _maxMemUse = _fileReader->EvaluateCurrentMemUsed();
            std::unique_ptr<RTableReader> table(new RTableReader);
            if (!table->Init(dir, _fileReader)) {
                return false;
            }
            _table = table.release();
        } else {
            _maxMemUse = _fileReader->GetLength();
            std::unique_ptr<RTable> table(new RTable);
            if (!table->MountForRead(_tableBaseAddr, _maxMemUse)) {
                return false;
            }
            _table = table.release();
        }
        break;
    }
    case PKeyTableOpenType::RW: {
        assert(_maxMemUse > 0);
        _tableBaseAddr = (char*)_pool.allocate(_maxMemUse);
        assert(_tableBaseAddr);
        std::unique_ptr<RWTable> table(new RWTable);
        if (!table->MountForWrite(_tableBaseAddr, _maxMemUse, _occupancyPct)) {
            return false;
        }
        _table = table.release();
        break;
    }
    case PKeyTableOpenType::WRITE: {
        assert(_maxMemUse > 0);
        _inMemFile = dir->CreateFileWriter(PREFIX_KEY_FILE_NAME, indexlib::file_system::WriterOption::Mem(_maxMemUse))
                         .GetOrThrow();
        // TODO(xinfei.sxf) check v1 version debug mode core?
        _inMemFile->Truncate(0).GetOrThrow(PREFIX_KEY_FILE_NAME);
        _tableBaseAddr = (char*)_inMemFile->GetBaseAddress();
        assert(_tableBaseAddr);
        std::unique_ptr<WTable> table(new WTable);
        typename Traits::Options options(_occupancyPct);
        options.mayStretch = true;
        if (!table->MountForWrite(_tableBaseAddr, _maxMemUse, options)) {
            return false;
        }
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

template <typename ValueType, PKeyTableType Type>
inline void ClosedHashPrefixKeyTable<ValueType, Type>::Release()
{
    if (!_table) {
        return;
    }

    switch (this->_openType) {
    case PKeyTableOpenType::READ: {
        if (_tableBaseAddr) {
            RTable* table = (RTable*)_table;
            DELETE_AND_SET_NULL(table);
        } else {
            RTableReader* table = (RTableReader*)_table;
            DELETE_AND_SET_NULL(table);
        }
        break;
    }
    case PKeyTableOpenType::RW: {
        RWTable* table = (RWTable*)_table;
        DELETE_AND_SET_NULL(table);

        if (_tableBaseAddr) {
            _pool.deallocate((void*)_tableBaseAddr, _maxMemUse);
            _tableBaseAddr = nullptr;
        }
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
    this->_openType = PKeyTableOpenType::UNKNOWN;
    _table = nullptr;
    _tableBaseAddr = nullptr;
}

template <typename ValueType, PKeyTableType Type>
void ClosedHashPrefixKeyTable<ValueType, Type>::Close()
{
    if (!_table) {
        return;
    }

    if (this->_openType == PKeyTableOpenType::WRITE && _inMemFile != nullptr) {
        WTable* table = (WTable*)_table;
        if (!table->Shrink()) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Shrink failed, file[%s]", _inMemFile->DebugString().c_str());
        }
        _inMemFile->Truncate(table->MemoryUse()).GetOrThrow();
        _inMemFile->Close().GetOrThrow();
        _inMemFile.reset();
    }
    Release();
}

template <typename ValueType, PKeyTableType Type>
ValueType* ClosedHashPrefixKeyTable<ValueType, Type>::Find(const uint64_t& key) const
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

template <typename ValueType, PKeyTableType Type>
bool ClosedHashPrefixKeyTable<ValueType, Type>::Insert(const uint64_t& key, const ValueType& value)
{
    if (!_table) {
        return false;
    }

    switch (this->_openType) {
    case PKeyTableOpenType::RW: {
        return ((RWTable*)_table)->Insert(key, ClosedPKeyTableSpecialValueTraits<ValueType, Type>::ToStoreValue(value));
    }
    case PKeyTableOpenType::WRITE: {
        WTable* table = (WTable*)_table;
        if (likely(table->Insert(key, value))) {
            return true;
        }
        AUTIL_LOG(WARN, "Insert key[%lu] failed, trigger stretch and retry, file[%s]", key,
                  (_inMemFile ? _inMemFile->DebugString().c_str() : ""));
        if (table->Stretch() && table->Insert(key, value)) {
            return true;
        }
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Insert[%lu] failed, file[%s]", key,
                             (_inMemFile ? _inMemFile->DebugString().c_str() : ""));
    }
    default:
        assert(false);
    }
    return false;
}

template <typename ValueType, PKeyTableType Type>
typename std::shared_ptr<typename ClosedHashPrefixKeyTable<ValueType, Type>::Iterator>
ClosedHashPrefixKeyTable<ValueType, Type>::CreateIterator() const
{
    switch (this->_openType) {
    case PKeyTableOpenType::RW: {
        using Iterator = ClosedHashPrefixKeyTableIterator<ValueType, Type>;
        return std::shared_ptr<Iterator>(new Iterator(((RWTable*)_table)->CreateIterator()));
    }
    case PKeyTableOpenType::WRITE:
    case PKeyTableOpenType::READ:
    default:
        assert(false);
    }
    return nullptr;
}

template <typename ValueType, PKeyTableType Type>
size_t ClosedHashPrefixKeyTable<ValueType, Type>::Size() const
{
    if (!_table) {
        return 0;
    }

    switch (this->_openType) {
    case PKeyTableOpenType::RW:
        return ((RWTable*)_table)->Size();
    case PKeyTableOpenType::READ:
        return _tableBaseAddr ? ((RTable*)_table)->Size() : ((RTableReader*)_table)->Size();
    case PKeyTableOpenType::WRITE:
        return ((WTable*)_table)->Size();
    default:
        assert(false);
    }
    return 0;
}

template <typename ValueType, PKeyTableType Type>
bool ClosedHashPrefixKeyTable<ValueType, Type>::IsFull() const
{
    if (!_table) {
        return 0;
    }

    switch (this->_openType) {
    case PKeyTableOpenType::RW:
        return ((RWTable*)_table)->IsFull();
    case PKeyTableOpenType::READ:
        return _tableBaseAddr ? ((RTable*)_table)->IsFull() : ((RTableReader*)_table)->IsFull();
    case PKeyTableOpenType::WRITE:
        return ((WTable*)_table)->IsFull();
    default:
        assert(false);
    }
    return 0;
}

} // namespace indexlibv2::index
