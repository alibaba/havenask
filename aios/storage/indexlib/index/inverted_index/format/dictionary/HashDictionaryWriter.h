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

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/inverted_index/format/dictionary/UtilDefine.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PrimeNumberTable.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/slice_array/ByteAlignedSliceArray.h"

namespace indexlib::index {

template <typename DictKeyType>
class HashDictionaryWriter : public DictionaryWriter
{
public:
    static const uint32_t INITIAL_BLOCK_COUNT = 32 * 1024;
    static constexpr double MAX_REHASH_DATA_RATIO = 1.2f;
    static constexpr double MIN_REHASH_DATA_RATIO = 0.2f;
    using AllocatorType = typename autil::mem_pool::pool_allocator<uint32_t>;
    using HashItem = HashDictionaryItemTyped<DictKeyType>;

    HashDictionaryWriter(autil::mem_pool::PoolBase* pool);
    ~HashDictionaryWriter();

    // will use memory buffer to rehash, then write to _file
    void Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName) override;

    // will directly write to _file
    void Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
              size_t itemCount) override;

    void Close() override;
    void AddItem(index::DictKeyInfo key, dictvalue_t offset) override;

    static size_t GetInitialMemUse()
    {
        return util::PrimeNumberTable::FindPrimeNumber(INITIAL_BLOCK_COUNT) * sizeof(uint32_t);
    }

private:
    void Reserve(size_t itemCount, size_t blockNum);
    void ResetBlock(size_t blockNum);
    bool NeedRehash() const;
    void Rehash();

private:
    std::vector<uint32_t, AllocatorType> _blockIndex;
    std::shared_ptr<file_system::FileWriter> _file;
    autil::mem_pool::PoolBase* _pool;
    util::ByteAlignedSliceArray* _dataSlice;
    static const size_t SLICE_LEN = 512 * 1024;

    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, HashDictionaryWriter, DictKeyType);

template <typename DictKeyType>
HashDictionaryWriter<DictKeyType>::HashDictionaryWriter(autil::mem_pool::PoolBase* pool)
    : _blockIndex(AllocatorType(pool))
    , _pool(pool)
    , _dataSlice(nullptr)
{
    ResetBlock(util::PrimeNumberTable::FindPrimeNumber(INITIAL_BLOCK_COUNT));
}

template <typename DictKeyType>
HashDictionaryWriter<DictKeyType>::~HashDictionaryWriter()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _dataSlice);
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                             const std::string& fileName)
{
    _file = directory->CreateFileWriter(fileName, _writerOption);
    _dataSlice = IE_POOL_COMPATIBLE_NEW_CLASS(_pool, util::ByteAlignedSliceArray, SLICE_LEN, 10, nullptr, _pool);
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                             const std::string& fileName, size_t itemCount)
{
    size_t blockNum = util::PrimeNumberTable::FindPrimeNumber(itemCount);
    ResetBlock(blockNum);
    _file = directory->CreateFileWriter(fileName, _writerOption);
    Reserve(itemCount, blockNum);
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::AddItem(index::DictKeyInfo key, dictvalue_t offset)
{
    CheckKeySequence<DictKeyType>(key);
    if (key.IsNull()) {
        AddNullTermKey(offset);
    } else {
        HashItem item;
        item.key = (DictKeyType)key.GetKey();
        uint32_t blockNum = (uint32_t)_blockIndex.size();
        uint32_t blockIdx = item.key % blockNum;
        item.next = _blockIndex[blockIdx];
        item.value = offset;
        if (_dataSlice) {
            _dataSlice->SetTypedValue(_itemCount * sizeof(HashItem), item);
        } else {
            assert(_file != nullptr);
            _file->Write((void*)(&item), sizeof(HashItem)).GetOrThrow();
        }
        _blockIndex[blockIdx] = _itemCount;
        ++_itemCount;
        _lastKey = key;
    }
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::Reserve(size_t itemCount, size_t blockNum)
{
    if (!_file) {
        return;
    }

    size_t reserveSize = itemCount * sizeof(HashItem);
    size_t blockSize = blockNum * sizeof(uint32_t);
    reserveSize += blockSize;
    reserveSize += (sizeof(uint32_t) * 2); // blocknum & magic tail
    reserveSize += sizeof(dictvalue_t);    // nullTerm dictValue
    _file->ReserveFile(reserveSize).GetOrThrow();
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::ResetBlock(size_t blockNum)
{
    _blockIndex.resize(blockNum);
    size_t batchSize = blockNum / 8;
    size_t remainSize = blockNum % 8;
    uint32_t* cursor = (uint32_t*)_blockIndex.data();
    for (size_t i = 0; i < batchSize; i++) {
        cursor[0] = std::numeric_limits<uint32_t>::max();
        cursor[1] = std::numeric_limits<uint32_t>::max();
        cursor[2] = std::numeric_limits<uint32_t>::max();
        cursor[3] = std::numeric_limits<uint32_t>::max();
        cursor[4] = std::numeric_limits<uint32_t>::max();
        cursor[5] = std::numeric_limits<uint32_t>::max();
        cursor[6] = std::numeric_limits<uint32_t>::max();
        cursor[7] = std::numeric_limits<uint32_t>::max();
        cursor += 8;
    }
    for (size_t i = 0; i < remainSize; i++) {
        *cursor = std::numeric_limits<uint32_t>::max();
        ++cursor;
    }
}

template <typename DictKeyType>
void HashDictionaryWriter<DictKeyType>::Close()
{
    if (_file == nullptr) {
        return;
    }

    if (_dataSlice) {
        if (NeedRehash()) {
            Rehash();
        }
        for (size_t i = 0; i < _dataSlice->GetSliceNum(); ++i) {
            _file->Write(_dataSlice->GetSlice(i), _dataSlice->GetSliceDataLen(i)).GetOrThrow();
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _dataSlice);
        _dataSlice = nullptr;
    }

    void* data = _blockIndex.data();
    uint32_t blockNum = (uint32_t)_blockIndex.size();
    uint32_t blockSize = blockNum * sizeof(uint32_t);
    _file->Write(data, blockSize).GetOrThrow();
    _file->Write((void*)(&blockNum), sizeof(uint32_t)).GetOrThrow();
    if (_hasNullTerm) {
        _file->Write((void*)(&_nullTermOffset), sizeof(dictvalue_t)).GetOrThrow();
    }
    uint32_t magicNum = _hasNullTerm ? DICTIONARY_WITH_NULLTERM_MAGIC_NUMBER : DICTIONARY_MAGIC_NUMBER;
    _file->Write((void*)(&magicNum), sizeof(uint32_t)).GetOrThrow();
    if (NeedRehash()) {
        AUTIL_LOG(INFO, "need rehash for file [%s], itemCount [%u], bucketNum [%u]", _file->DebugString().c_str(),
                  _itemCount, blockNum);
    }
    _file->Close().GetOrThrow();
    _file.reset();
}

template <typename DictKeyType>
inline bool HashDictionaryWriter<DictKeyType>::NeedRehash() const
{
    double capacityRatio = (double)_itemCount / (double)_blockIndex.size();
    return capacityRatio > MAX_REHASH_DATA_RATIO || capacityRatio < MIN_REHASH_DATA_RATIO;
}

template <typename DictKeyType>
inline void HashDictionaryWriter<DictKeyType>::Rehash()
{
    AUTIL_LOG(INFO, "rehash for file [%s], old blockNum [%lu] itemCount [%u]", _file->DebugString().c_str(),
              _blockIndex.size(), _itemCount);
    assert(_dataSlice);
    size_t blockNum = util::PrimeNumberTable::FindPrimeNumber(_itemCount);
    ResetBlock(blockNum);
    HashItem item;
    for (uint32_t i = 0; i < _itemCount; i++) {
        _dataSlice->GetTypedValue(i * sizeof(HashItem), item);
        uint32_t blockIdx = item.key % blockNum;
        item.next = _blockIndex[blockIdx];
        _dataSlice->SetTypedValue(i * sizeof(HashItem), item);
        _blockIndex[blockIdx] = i;
    }
    AUTIL_LOG(INFO, "rehash for file [%s], new blockNum [%lu] itemCount [%u]", _file->DebugString().c_str(),
              _blockIndex.size(), _itemCount);
}

} // namespace indexlib::index
