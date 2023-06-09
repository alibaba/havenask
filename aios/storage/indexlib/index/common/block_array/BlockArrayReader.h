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

#include "autil/EnvUtil.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/block_array/BlockArrayCacheDataAccessor.h"
#include "indexlib/index/common/block_array/BlockArrayCompressDataAccessor.h"
#include "indexlib/index/common/block_array/BlockArrayDataAccessor.h"
#include "indexlib/index/common/block_array/BlockArrayIterator.h"
#include "indexlib/index/common/block_array/BlockArrayMemoryDataAccessor.h"
#include "indexlib/util/PathUtil.h"
namespace indexlibv2 { namespace index {

template <typename Key, typename Value>
class BlockArrayReader
{
public:
    using Iterator = BlockArrayIterator<Key, Value>;
    using IteratorPtr = std::shared_ptr<Iterator>;
    using KVItem = indexlib::index::KeyValueItem<Key, Value>;
    using BlockArrayDataAccessorPtr = std::shared_ptr<BlockArrayDataAccessor<Key, Value>>;
    using BlockArrayMemoryDataAccessorPtr = std::shared_ptr<BlockArrayMemoryDataAccessor<Key, Value>>;
    using BlockArrayCacheDataAccessorPtr = std::shared_ptr<BlockArrayCacheDataAccessor<Key, Value>>;
    // we should guarantee KVItem is packed, because writer write Key and Value, reader read KVItem
    static_assert(sizeof(KVItem) == sizeof(Key) + sizeof(Value), "KVItem should be packed");
    BlockArrayReader()
        : _dataBlockSize(0)
        , _itemCount(0)
        , _bottomMetaKeyCount(0)
        , _metaKeyCountPerUnit(0)
        , _blockCount(0)
        , _itemCountPerBlock(0)
        , _metaKeyBaseAddress(nullptr)
        , _metaKeyCount(0)
    {
    }

    virtual ~BlockArrayReader() {}

    BlockArrayReader(const BlockArrayReader&) = delete;
    BlockArrayReader& operator=(const BlockArrayReader&) = delete;
    BlockArrayReader(BlockArrayReader&&) = delete;
    BlockArrayReader& operator=(BlockArrayReader&&) = delete;

public:
    // Init by file reader, if you need call method @Find, your parm @loadMetaIndex must be "true"
    // if @loadMetaIndex is "false", @directory is useless, and we support any file reader type
    // if @loadMetaIndex is "true", we will use file reader to access data, we support memory access and block cache
    virtual std::pair<Status, bool> Init(const indexlib::file_system::FileReaderPtr& fileReader,
                                         const indexlib::file_system::DirectoryPtr& directory, uint64_t readLength,
                                         bool loadMetaIndex);

    // Find key, if existed fill value and return true, otherwise return false without changing value
    indexlib::index::Result<bool> Find(const Key& key, indexlib::file_system::ReadOption option,
                                       Value* value) noexcept __ALWAYS_INLINE;

    inline future_lite::coro::Lazy<indexlib::index::Result<bool>>
    FindAsync(const Key& key, indexlib::file_system::ReadOption option, Value* value) noexcept;

    inline uint64_t GetItemCount() const;

    // Create Iterator, you should call method @Init before
    // @loadMetaIndex in @Init can be "false" if not use @Find
    std::pair<Status, Iterator*> CreateIterator() const;
    uint64_t EstimateMetaSize() const;

protected:
    virtual std::pair<Status, bool> InitMeta(bool loadMetaIndex, uint64_t readLength);
    std::pair<Status, bool> LoadMetaIndex(uint64_t offset);
    uint64_t CaculateDataLength() const;
    bool CheckIntegrity(uint64_t actualDataLength) const;
    bool CheckMeta(const uint64_t& offset) const;
    void FillMetaHelpInfo();
    bool GetBlockId(const Key& key, uint64_t* blockId) const noexcept __ALWAYS_INLINE;
    BlockArrayDataAccessor<Key, Value>* CreateBlockArrayDataAccessor(indexlib::file_system::FileReaderPtr fileReader);

protected:
    indexlib::file_system::FileReaderPtr _fileReader;
    indexlib::file_system::DirectoryPtr _directory;

    BlockArrayDataAccessorPtr _accessor;
    BlockArrayMemoryDataAccessorPtr _memoryAccessor;

    // loaded from file
    uint64_t _dataBlockSize;
    uint64_t _itemCount;
    uint64_t _bottomMetaKeyCount;
    uint64_t _metaKeyCountPerUnit;

    // calculated by meta
    uint64_t _blockCount;
    uint64_t _itemCountPerBlock;

    indexlib::file_system::FileReaderPtr _sliceFileReader;
    Key* _metaKeyBaseAddress;
    uint64_t _metaKeyCount; // the meta key count (sum of the all level)

    //[left, right)
    std::vector<uint64_t> _leftBound;           // level -> the header's position in @_metaKeyBaseAddress in this level
    std::vector<uint64_t> _rightBound;          // level -> the end's position in @_metaKeyBaseAddress in this level
    std::vector<uint64_t> _levelToMetaKeyCount; // level -> the meta key count in this level
    indexlib::file_system::BlockFileNodePtr _blockFileNode; // used for reading block on disk
private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, BlockArrayReader, Key, Value);

template <typename Key, typename Value>
std::pair<Status, bool> BlockArrayReader<Key, Value>::Init(const indexlib::file_system::FileReaderPtr& fileReader,
                                                           const indexlib::file_system::DirectoryPtr& directory,
                                                           uint64_t readLength, bool loadMetaIndex)
{
    if (loadMetaIndex && !directory) {
        AUTIL_LOG(ERROR, "Unsupport loadMetaIndex with null directory");
        return std::make_pair(Status::OK(), false);
    }
    _fileReader = fileReader;
    _directory = directory;
    // step1 : we load meta from reader into memory
    // include block data info, meta index info (and meta index if need),
    // while we should check integrity
    auto [status, ret] = InitMeta(loadMetaIndex, readLength);
    RETURN2_IF_STATUS_ERROR(status, false, "init meta fail");
    if (!ret) {
        return std::make_pair(Status::OK(), false);
    }
    if (!loadMetaIndex) {
        return std::make_pair(Status::OK(), true);
    }

    _accessor.reset(CreateBlockArrayDataAccessor(_fileReader));
    if (!_accessor) {
        return std::make_pair(Status::OK(), false);
    }
    status = _accessor->Init(_fileReader, _dataBlockSize);
    RETURN2_IF_STATUS_ERROR(status, false, "accessor init fail");
    _memoryAccessor = std::dynamic_pointer_cast<BlockArrayMemoryDataAccessor<Key, Value>>(_accessor);
    return std::make_pair(Status::OK(), true);
}

template <typename Key, typename Value>
inline indexlib::index::Result<bool>
BlockArrayReader<Key, Value>::Find(const Key& key, indexlib::file_system::ReadOption option, Value* value) noexcept
{
    // step1 : we locate which block we use to find the Key (inline)
    uint64_t blockId = 0;
    if (!GetBlockId(key, &blockId)) {
        return false;
    }

    // consider block may be the last block
    uint64_t keyCountInBlock =
        blockId + 1 == _blockCount ? _itemCount - _itemCountPerBlock * blockId : _itemCountPerBlock;

    // step2 : we locate the key/value item in the block
    return _memoryAccessor ? _memoryAccessor->GetValueInBlock(key, blockId, keyCountInBlock, option, value)
                           : _accessor->GetValueInBlock(key, blockId, keyCountInBlock, option, value);
}

template <typename Key, typename Value>
inline future_lite::coro::Lazy<indexlib::index::Result<bool>>
BlockArrayReader<Key, Value>::FindAsync(const Key& key, indexlib::file_system::ReadOption option, Value* value) noexcept
{
    // step1 : we locate which block we use to find the Key (inline)
    uint64_t blockId = 0;
    if (!GetBlockId(key, &blockId)) {
        co_return false;
    }

    // consider block may be the last block
    uint64_t keyCountInBlock =
        blockId + 1 == _blockCount ? _itemCount - _itemCountPerBlock * blockId : _itemCountPerBlock;
    if (_memoryAccessor) {
        co_return _memoryAccessor->GetValueInBlock(key, blockId, keyCountInBlock, option, value);
    }
    co_return co_await _accessor->GetValueInBlockAsync(key, blockId, keyCountInBlock, option, value);
}

template <typename Key, typename Value>
inline uint64_t BlockArrayReader<Key, Value>::GetItemCount() const
{
    return _itemCount;
}

template <typename Key, typename Value>
inline bool BlockArrayReader<Key, Value>::GetBlockId(const Key& key, uint64_t* blockId) const noexcept
{
    uint64_t levelNum = _leftBound.size();
    if (levelNum == 1) {
        uint64_t blockIdMay =
            std::lower_bound(_metaKeyBaseAddress, _metaKeyBaseAddress + _metaKeyCount, key) - _metaKeyBaseAddress;
        if (blockIdMay == _metaKeyCount) {
            return false;
        }
        *blockId = blockIdMay;
        return true;
    }
    assert(levelNum > 0);
    uint64_t position = 0;
    for (uint64_t i = 0; i < levelNum; ++i) {
        uint64_t lower = _leftBound[i] + position * _metaKeyCountPerUnit;
        uint64_t upper = std::min(lower + _metaKeyCountPerUnit, _rightBound[i]);
        position = std::lower_bound(_metaKeyBaseAddress + lower, _metaKeyBaseAddress + upper, key) -
                   _metaKeyBaseAddress - _leftBound[i];
        if (_leftBound[i] + position == upper) {
            return false;
        }
    }
    *blockId = position;
    return true;
}

template <typename Key, typename Value>
std::pair<Status, BlockArrayIterator<Key, Value>*> BlockArrayReader<Key, Value>::CreateIterator() const
{
    Iterator* iter = new Iterator();
    auto status = iter->Init(_fileReader, _dataBlockSize, _itemCount);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "block array iterator init fail");
    return std::make_pair(Status::OK(), iter);
}

template <typename Key, typename Value>
std::pair<Status, bool> BlockArrayReader<Key, Value>::InitMeta(bool loadMetaIndex, uint64_t readLength)
{
    assert(readLength <= _fileReader->GetLogicLength());
    uint64_t offset = readLength;
    uint64_t readSize = 0;
    offset -= sizeof(_bottomMetaKeyCount);
    auto pairRet = _fileReader->Read(&_bottomMetaKeyCount, sizeof(_bottomMetaKeyCount), offset).StatusWith();
    RETURN2_IF_STATUS_ERROR(pairRet.first, false, "file[%s] read fail", _fileReader->GetLogicalPath().c_str());
    readSize = pairRet.second;
    if (readSize != sizeof(_bottomMetaKeyCount)) {
        AUTIL_LOG(ERROR, "Read _bottomMetaKeyCount error, expected size[%zu], write size[%zu]",
                  sizeof(_bottomMetaKeyCount), readSize);
        return std::make_pair(Status::OK(), false);
    }

    offset -= sizeof(_metaKeyCountPerUnit);
    pairRet = _fileReader->Read(&_metaKeyCountPerUnit, sizeof(_metaKeyCountPerUnit), offset).StatusWith();
    RETURN2_IF_STATUS_ERROR(pairRet.first, false, "file[%s] read fail", _fileReader->GetLogicalPath().c_str());
    readSize = pairRet.second;
    if (readSize != sizeof(_metaKeyCountPerUnit)) {
        AUTIL_LOG(ERROR, "Read _metaKeyCountPerUnit error, expected size[%zu], write size[%zu]",
                  sizeof(_metaKeyCountPerUnit), readSize);
        return std::make_pair(Status::OK(), false);
    }
    if (!CheckMeta(offset)) {
        return std::make_pair(Status::OK(), false);
    }
    FillMetaHelpInfo();
    offset -= _metaKeyCount * sizeof(Key);
    if (loadMetaIndex) {
        auto [status, ret] = LoadMetaIndex(offset);
        RETURN2_IF_STATUS_ERROR(status, false, "load meta index fail");
        if (!ret) {
            return std::make_pair(Status::OK(), false);
        }
    }

    offset -= sizeof(_dataBlockSize);
    pairRet = _fileReader->Read(&_dataBlockSize, sizeof(_dataBlockSize), offset).StatusWith();
    RETURN2_IF_STATUS_ERROR(pairRet.first, false, "file[%s] read fail", _fileReader->GetLogicalPath().c_str());
    readSize = pairRet.second;
    if (readSize != sizeof(_dataBlockSize)) {
        AUTIL_LOG(ERROR, "Read _dataBlockSize error, expected size[%zu], write size[%zu]", sizeof(_dataBlockSize),
                  readSize);
        return std::make_pair(Status::OK(), false);
    }

    offset -= sizeof(_itemCount);
    pairRet = _fileReader->Read(&_itemCount, sizeof(_itemCount), offset).StatusWith();
    RETURN2_IF_STATUS_ERROR(pairRet.first, false, "file[%s] read fail", _fileReader->GetLogicalPath().c_str());
    readSize = pairRet.second;
    if (readSize != sizeof(_itemCount)) {
        AUTIL_LOG(ERROR, "Read _itemCount error, expected size[%zu], write size[%zu]", sizeof(_itemCount), readSize);
        return std::make_pair(Status::OK(), false);
    }

    _itemCountPerBlock = _dataBlockSize / sizeof(KVItem);
    _blockCount = (_itemCount + _itemCountPerBlock - 1) / _itemCountPerBlock;
    return std::make_pair(Status::OK(), CheckIntegrity(offset));
}

template <typename Key, typename Value>
void BlockArrayReader<Key, Value>::FillMetaHelpInfo()
{
    // fill @_metaKeyCount, @_leftBound @_levelToMetaKeyCount
    uint64_t currentMetaKeyCount = _bottomMetaKeyCount;

    // we calculate every level meta key count
    _levelToMetaKeyCount.clear();
    _levelToMetaKeyCount.push_back(currentMetaKeyCount);
    while (currentMetaKeyCount > _metaKeyCountPerUnit) {
        // 相当于 currentMetaKeyCount = [ currentMetaKeyCount / _metaKeyCountPerUnit ] 上取整
        currentMetaKeyCount = (currentMetaKeyCount + _metaKeyCountPerUnit - 1) / _metaKeyCountPerUnit;
        _levelToMetaKeyCount.push_back(currentMetaKeyCount);
    }
    std::reverse(_levelToMetaKeyCount.begin(), _levelToMetaKeyCount.end());

    _leftBound.clear();
    _leftBound.reserve(_levelToMetaKeyCount.size());
    _metaKeyCount = 0;
    for (auto metaKeyCountInLevel : _levelToMetaKeyCount) {
        _leftBound.push_back(_metaKeyCount);
        _metaKeyCount += metaKeyCountInLevel;
        _rightBound.push_back(_metaKeyCount);
    }
}

template <typename Key, typename Value>
uint64_t BlockArrayReader<Key, Value>::CaculateDataLength() const
{
    if (_blockCount == 0) {
        return 0;
    }
    assert(_blockCount > 0);
    uint64_t lastBlockItemCount = _itemCount - (_blockCount - 1) * _itemCountPerBlock;
    uint64_t lastBlockSize =
        lastBlockItemCount == _itemCountPerBlock ? _dataBlockSize : lastBlockItemCount * sizeof(KVItem);
    return (_blockCount - 1) * _dataBlockSize + lastBlockSize;
}

template <typename Key, typename Value>
bool BlockArrayReader<Key, Value>::CheckIntegrity(uint64_t actualDataLength) const
{
    // block data length should be consistent with @_dataBlockSize and @_blockCount
    uint64_t expectedDataLength = CaculateDataLength();
    if (actualDataLength != expectedDataLength) {
        AUTIL_LOG(ERROR, "Check integrity failed, expected block data length[%zu], actual block data length[%zu]",
                  expectedDataLength, actualDataLength);
        return false;
    }

    if (_blockCount != _bottomMetaKeyCount) {
        AUTIL_LOG(ERROR,
                  "Check integrity failed, calculated  _blockCount [%zu] not equal with _bottomMetaKeyCount[%zu]",
                  _blockCount, _bottomMetaKeyCount);
        return false;
    }
    return true;
}

template <typename Key, typename Value>
bool BlockArrayReader<Key, Value>::CheckMeta(const uint64_t& offset) const
{
    if (_metaKeyCountPerUnit <= 1) {
        AUTIL_LOG(ERROR, "Check meta error, _metaKeyCountPerUnit[%zu] is illegal.", _metaKeyCountPerUnit);
        return false;
    }
    uint64_t metaKeyCount = _bottomMetaKeyCount;
    uint64_t currentCount = _bottomMetaKeyCount;
    while (currentCount > _metaKeyCountPerUnit) {
        currentCount = (currentCount + _metaKeyCountPerUnit - 1) / _metaKeyCountPerUnit;
        metaKeyCount += currentCount;
    }

    if (offset < metaKeyCount * sizeof(Key) * 2) {
        AUTIL_LOG(ERROR, "Check meta error, offset[%zu], metaKeyCount[%zu].", offset, _bottomMetaKeyCount);
        return false;
    }
    return true;
}

template <typename Key, typename Value>
std::pair<Status, bool> BlockArrayReader<Key, Value>::LoadMetaIndex(uint64_t offset)
{
    if (_bottomMetaKeyCount == 0) {
        return std::make_pair(Status::OK(), true);
    }

    // use slice file when mmap-nolock when not-disable slice memlock
    bool disableSliceMemLock = autil::EnvUtil::getEnv<bool>("INDEXLIB_DISABLE_SLICE_MEM_LOCK", false);
    if (_fileReader->GetBaseAddress() && (disableSliceMemLock || _fileReader->IsMemLock())) {
        _metaKeyBaseAddress = (Key*)((char*)_fileReader->GetBaseAddress() + offset);
    } else {
        // we need load Meta Index into slice file if need, initializee @_metaKeyBaseAddress
        // first we should check whether meta index has been written into slice file
        // if not, we should write it from readr (at offset) to the slice file
        std::string fileName;
        indexlib::util::PathUtil::GetRelativePath(_directory->GetLogicalPath(), _fileReader->GetLogicalPath(),
                                                  fileName);
        std::string sliceFileName = fileName + ".block_index";
        auto fileReader = _directory->CreateFileReader(sliceFileName, indexlib::file_system::FSOT_SLICE);
        if (!fileReader) {
            uint64_t sliceLen = sizeof(Key) * _metaKeyCount;
            auto fileWriter =
                _directory->CreateFileWriter(sliceFileName, indexlib::file_system::WriterOption::Slice(sliceLen, 1));
            RETURN2_IF_STATUS_ERROR(fileWriter->Truncate(sliceLen).Status(), false, "file[%s] truncate fail",
                                    fileWriter->GetLogicalPath().c_str());
            fileReader = _directory->CreateFileReader(sliceFileName, indexlib::file_system::FSOT_SLICE);
            RETURN2_IF_STATUS_ERROR(fileWriter->Close().Status(), false, "file[%s] close fail",
                                    fileWriter->GetLogicalPath().c_str());
            auto [status, readLen] = _fileReader->Read(fileReader->GetBaseAddress(), sliceLen, offset).StatusWith();
            RETURN2_IF_STATUS_ERROR(status, false, "file[%s] read fail", _fileReader->GetLogicalPath().c_str());
            if (sliceLen != readLen) {
                RETURN2_IF_STATUS_ERROR(Status::Corruption(), false, "read block index data fail in file [%s]",
                                        _fileReader->DebugString().c_str());
                return std::make_pair(Status::OK(), false);
            }
        }
        // we use the slice file memory address as @keys, then meta index is all in memory
        // hold @_sliceFileReader lifecycle is to guarantee @_metaKeyBaseAddress is always legal
        _sliceFileReader = fileReader;
        _metaKeyBaseAddress = (Key*)_sliceFileReader->GetBaseAddress();
    }
    return std::make_pair(Status::OK(), true);
}

template <typename Key, typename Value>
uint64_t BlockArrayReader<Key, Value>::EstimateMetaSize() const
{
    return _metaKeyCount * sizeof(Key);
}

template <typename Key, typename Value>
BlockArrayDataAccessor<Key, Value>*
BlockArrayReader<Key, Value>::CreateBlockArrayDataAccessor(indexlib::file_system::FileReaderPtr fileReader)
{
    if (fileReader->GetBaseAddress()) {
        return new BlockArrayMemoryDataAccessor<Key, Value>();
    }

    indexlib::file_system::CompressFileReaderPtr compressFileReader =
        std::dynamic_pointer_cast<indexlib::file_system::CompressFileReader>(fileReader);
    if (compressFileReader) {
        return new BlockArrayCompressDataAccessor<Key, Value>();
    }
    if (std::dynamic_pointer_cast<indexlib::file_system::BlockFileNode>(fileReader->GetFileNode())) {
        return new BlockArrayCacheDataAccessor<Key, Value>();
    }
    AUTIL_LOG(ERROR, "fileReader type error, block_array only support memory or blockCache fileReader.");
    return nullptr;
}
}} // namespace indexlibv2::index
