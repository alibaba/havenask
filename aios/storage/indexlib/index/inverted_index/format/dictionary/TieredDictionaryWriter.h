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
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/inverted_index/format/dictionary/UtilDefine.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::index {

template <typename DictKeyType>
class TieredDictionaryWriter : public DictionaryWriter
{
public:
    static const uint32_t INITIAL_BLOCK_COUNT = 32 * 1024;
    using AllocatorType = typename autil::mem_pool::pool_allocator<DictKeyType>;

    TieredDictionaryWriter(autil::mem_pool::PoolBase* pool);
    ~TieredDictionaryWriter();

    void Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName) override;

    void Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
              size_t itemCount) override;

    void Close() override;
    void AddItem(index::DictKeyInfo key, dictvalue_t offset) override;

    static size_t GetInitialMemUse() { return INITIAL_BLOCK_COUNT * sizeof(DictKeyType); }

private:
    void DoAddItem(const index::DictKeyInfo& key, dictvalue_t offset);
    void Reserve(size_t itemCount);

    uint32_t _itemCountInBlock;
    std::vector<DictKeyType, AllocatorType> _blockIndex;
    std::shared_ptr<file_system::FileWriter> _file;

    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, TieredDictionaryWriter, DictKeyType);

template <typename DictKeyType>
TieredDictionaryWriter<DictKeyType>::TieredDictionaryWriter(autil::mem_pool::PoolBase* pool)
    : _itemCountInBlock(0)
    , _blockIndex(AllocatorType(pool))
{
    _blockIndex.reserve(INITIAL_BLOCK_COUNT);
}

template <typename DictKeyType>
TieredDictionaryWriter<DictKeyType>::~TieredDictionaryWriter()
{
}

template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                               const std::string& fileName)
{
    _file = directory->CreateFileWriter(fileName, _writerOption);
}

template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                               const std::string& fileName, size_t itemCount)
{
    Open(directory, fileName);
    Reserve(itemCount);
}

template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::AddItem(index::DictKeyInfo key, dictvalue_t offset)
{
    CheckKeySequence<DictKeyType>(key);
    DoAddItem(key, offset);
}

template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::Reserve(size_t itemCount)
{
    if (!_file) {
        return;
    }

    size_t reserveSize = itemCount * (sizeof(DictKeyType) + sizeof(dictvalue_t));

    size_t blockNum = (itemCount + ITEM_COUNT_PER_BLOCK - 1) / ITEM_COUNT_PER_BLOCK;
    size_t blockSize = blockNum * sizeof(DictKeyType);
    reserveSize += blockSize;
    reserveSize += (sizeof(uint32_t) * 2); // blocknum & magic tail
    reserveSize += sizeof(dictvalue_t);    // nullTerm dictValue
    _file->ReserveFile(reserveSize).GetOrThrow();
}

template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::DoAddItem(const index::DictKeyInfo& key, dictvalue_t offset)
{
    assert(_file != nullptr);
    if (key.IsNull()) {
        AddNullTermKey(offset);
    } else {
        DictKeyType tmp = (DictKeyType)key.GetKey();
        _file->Write((void*)&tmp, sizeof(DictKeyType)).GetOrThrow();
        _file->Write((void*)&offset, sizeof(dictvalue_t)).GetOrThrow();
        _itemCountInBlock++;
        if (_itemCountInBlock == ITEM_COUNT_PER_BLOCK) {
            _blockIndex.push_back(key.GetKey());
            _itemCountInBlock = 0;
        }
        _itemCount++;
        _lastKey = key;
    }
}

// Close will write some meta info to file
// Tiered Dictionary Layout:
// |-----------------|-------------| blockNum(4 byte) | [nullTermOffset(4 byte)] | magic number(4 byte) |
//   Dictionary Data   Block Index
//
// Ditionary Data Layout: (few items means maybe less than @ITEM_COUNT_PER_BLOCK)
// |-----items-----|-----items-----|-----items-----|---few items---|
//       Block           Block           Block         Last Block
//
// BlockIndex is a sorted key array
template <typename DictKeyType>
void TieredDictionaryWriter<DictKeyType>::Close()
{
    if (_file == nullptr) {
        return;
    }

    if (_itemCountInBlock != 0) {
        _blockIndex.push_back(_lastKey.GetKey());
    }

    void* data = &(_blockIndex[0]);
    uint32_t blockNum = _blockIndex.size();
    uint32_t magicNum = _hasNullTerm ? DICTIONARY_WITH_NULLTERM_MAGIC_NUMBER : DICTIONARY_MAGIC_NUMBER;

    uint32_t blockSize = blockNum * sizeof(DictKeyType);
    _file->Write(data, blockSize).GetOrThrow();
    _file->Write((void*)(&blockNum), sizeof(uint32_t)).GetOrThrow();
    if (_hasNullTerm) {
        _file->Write((void*)&_nullTermOffset, sizeof(dictvalue_t)).GetOrThrow();
    }
    _file->Write((void*)(&magicNum), sizeof(uint32_t)).GetOrThrow();
    _file->Close().GetOrThrow();
    _file.reset();
}

} // namespace indexlib::index
