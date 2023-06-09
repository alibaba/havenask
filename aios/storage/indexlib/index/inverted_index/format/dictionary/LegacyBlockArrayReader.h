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

#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/index/common/block_array/BlockArrayReader.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskTieredDictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryIterator.h"

namespace indexlib::index {
// Tiered Dictionary Layout:
// |-----------------|-------------| blockNum(4 byte) | [nullTermOffset(4 byte)] | magic number(4 byte) |
//   Dictionary Data   Block Index
//
// Ditionary Data Layout: (few items means maybe less than @ITEM_COUNT_PER_BLOCK)
// |-----items-----|-----items-----|-----items-----|---few items---|
//       Block           Block           Block         Last Block
//
// BlockIndex is a sorted key array
//
// LegacyBlockArrayReader is an legacy reader to read TieredDictionary see "TieredDictionaryWriter.h"
//

enum DictionaryIterType {
    DIT_UNKNOWN,
    DIT_MEM,
    DIT_DISK,
};

template <typename Key, typename Value>
class LegacyBlockArrayReader final : public indexlibv2::index::BlockArrayReader<Key, Value>
{
public:
    using DictionaryItem = DictionaryItemTyped<Key>;
    LegacyBlockArrayReader(uint32_t blockCount) { this->_bottomMetaKeyCount = blockCount; }
    virtual ~LegacyBlockArrayReader() {}

    LegacyBlockArrayReader(const LegacyBlockArrayReader&) = delete;
    LegacyBlockArrayReader& operator=(const LegacyBlockArrayReader&) = delete;
    LegacyBlockArrayReader(LegacyBlockArrayReader&&) = delete;
    LegacyBlockArrayReader& operator=(LegacyBlockArrayReader&&) = delete;

    DictionaryIterType GetDictionaryIterType() const;
    DictionaryReader::DictionaryIteratorPtr CreateIteratorForDictionary(bool hasNullTermm, dictvalue_t nullTermValue,
                                                                        bool disableCache) const;

private:
    virtual std::pair<Status, bool> InitMeta(bool loadMetaIndex, uint64_t readLength) override;

    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, LegacyBlockArrayReader, Key, Value);

template <typename Key, typename Value>
std::pair<Status, bool> LegacyBlockArrayReader<Key, Value>::InitMeta(bool loadMetaIndex, uint64_t readLength)
{
    assert(readLength <= this->_fileReader->GetLogicLength());
    uint64_t offset = readLength;
    this->_metaKeyCountPerUnit = this->_bottomMetaKeyCount;
    this->FillMetaHelpInfo();
    offset -= this->_metaKeyCount * sizeof(Key);
    if (loadMetaIndex) {
        auto [status, ret] = this->LoadMetaIndex(offset);
        RETURN2_IF_STATUS_ERROR(status, false, "load meta index fail");
        if (!ret) {
            return std::make_pair(Status::OK(), false);
        }
    }
    this->_dataBlockSize = ITEM_COUNT_PER_BLOCK * sizeof(DictionaryItem);
    this->_itemCount = offset / sizeof(DictionaryItem);
    this->_itemCountPerBlock = ITEM_COUNT_PER_BLOCK;
    this->_blockCount =
        (this->_itemCount + this->_itemCountPerBlock - 1) / this->_itemCountPerBlock; // for check integrity
    return std::make_pair(Status::OK(), this->CheckIntegrity(offset));
}

template <typename Key, typename Value>
DictionaryIterType LegacyBlockArrayReader<Key, Value>::GetDictionaryIterType() const
{
    if (!this->_fileReader) {
        return DIT_UNKNOWN;
    }
    return this->_fileReader->GetBaseAddress() ? DIT_MEM : DIT_DISK;
}

template <typename Key, typename Value>
DictionaryReader::DictionaryIteratorPtr
LegacyBlockArrayReader<Key, Value>::CreateIteratorForDictionary(bool hasNullTerm, dictvalue_t nullTermValue,
                                                                bool disableCache) const
{
    uint64_t blockIndexOffset = sizeof(DictionaryItem) * this->_itemCount;
    DictionaryIterator* iter;
    DictionaryItem* data = (DictionaryItem*)this->_fileReader->GetBaseAddress();
    if (data == nullptr) {
        auto fileReader = this->_fileReader;
        if (disableCache && this->_fileReader->GetOpenType() == file_system::FSOT_CACHE) {
            std::string fileName;
            util::PathUtil::GetRelativePath(this->_directory->GetLogicalPath(), this->_fileReader->GetLogicalPath(),
                                            fileName);
            auto compressReader = std::dynamic_pointer_cast<file_system::CompressFileReader>(this->_fileReader);
            auto option = file_system::ReaderOption::NoCache(file_system::FSOT_BUFFERED);
            option.supportCompress = (compressReader != nullptr);
            fileReader = this->_directory->CreateFileReader(fileName, option);
            assert(fileReader);
        }
        using IterType = CommonDiskTieredDictionaryIteratorTyped<Key>;
        iter = hasNullTerm ? new IterType(fileReader, blockIndexOffset, this->_metaKeyBaseAddress, this->_blockCount,
                                          nullTermValue)
                           : new IterType(fileReader, blockIndexOffset, this->_metaKeyBaseAddress, this->_blockCount);
    } else {
        using IterType = TieredDictionaryIteratorTyped<Key>;
        iter = hasNullTerm
                   ? new IterType(this->_metaKeyBaseAddress, this->_blockCount, data, this->_itemCount, nullTermValue)
                   : new IterType(this->_metaKeyBaseAddress, this->_blockCount, data, this->_itemCount);
    }
    return DictionaryReader::DictionaryIteratorPtr(iter);
}
} // namespace indexlib::index
