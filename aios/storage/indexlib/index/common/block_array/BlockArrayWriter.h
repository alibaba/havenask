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
#include <cmath>
#include <memory>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/block_array/KeyValueItem.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlibv2 { namespace index {

// BlockArrayWriter write array <key, value> to disk in block format
template <typename Key, typename Value>
class BlockArrayWriter
{
public:
    using AllocatorType = autil::mem_pool::pool_allocator<Key>;
    using KVItem = indexlib::index::KeyValueItem<Key, Value>;

    // we should guarantee KVItem is packed, because writer write Key and Value, reader read KVItem
    static_assert(sizeof(KVItem) == sizeof(Key) + sizeof(Value), "KVItem should be packed");

    BlockArrayWriter(autil::mem_pool::PoolBase* pool);
    ~BlockArrayWriter()
    {
        // ARRAY_DELETE_AND_SET_NULL(_blockBuffer);
        if (_blockBuffer) {
            IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _blockBuffer, (_itemCountPerBlock + 1));
        }
    }
    BlockArrayWriter(const BlockArrayWriter&) = delete;
    BlockArrayWriter& operator=(const BlockArrayWriter&) = delete;
    BlockArrayWriter(BlockArrayWriter&&) = delete;
    BlockArrayWriter& operator=(BlockArrayWriter&&) = delete;

public:
    // @blockSize : data block size
    // @adviceKeyNum: estimate (or exactly) key num, writer will reserver the memory
    bool Init(const uint64_t blockSize, const indexlib::file_system::FileWriterPtr& fileWriter);

    // add key/value, key must be sorted and unique
    Status AddItem(const Key& key, const Value& value);

    // finish add item, sort key/value items, sync meta file
    Status Finish();

private:
    // write block data into file
    Status WriteBlock(bool needPadding);

    // calculate meta and write meta into file
    Status WriteMeta();
    Status RecursiveWriteMeta(const std::vector<Key, AllocatorType>& metaKeyQueue,
                              uint64_t metaCountPerUnit); // write meta recursive

private:
    autil::mem_pool::PoolBase* _pool;
    indexlib::file_system::FileWriterPtr _fileWriter;

    uint64_t _dataBlockSize;     // byte, maybe not equal to block size of block cache
    uint64_t _itemCount;         // added key count
    uint64_t _itemCountPerBlock; // key count of each (data) block

    KVItem* _blockBuffer;
    uint64_t _cursorInBuffer;
    std::vector<Key, AllocatorType> _blockTail; // include the last keys in each block
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, BlockArrayWriter, Key, Value);

const static uint64_t MIN_META_COUNT_PER_BLOCK = 128;

template <typename Key, typename Value>
BlockArrayWriter<Key, Value>::BlockArrayWriter(autil::mem_pool::PoolBase* pool)
    : _pool(pool)
    , _dataBlockSize(0)
    , _itemCount(0)
    , _itemCountPerBlock(0)
    , _blockBuffer(nullptr)
    , _cursorInBuffer(0)
    , _blockTail(AllocatorType(pool))
{
}

template <typename Key, typename Value>
bool BlockArrayWriter<Key, Value>::Init(const uint64_t blockSize,
                                        const indexlib::file_system::FileWriterPtr& fileWriter)
{
    if (blockSize < sizeof(Key) + sizeof(Value)) {
        AUTIL_LOG(ERROR, "blockSize[%lu] is invalid, keySize[%lu], valueSize[%lu].", blockSize, sizeof(Key),
                  sizeof(Value));
        return false;
    }
    _dataBlockSize = blockSize;
    _itemCountPerBlock = blockSize / sizeof(KVItem);
    // @_itemCountPerBlock + 1 ensure allocated memory size >= @_dataBlockSize
    _blockBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, KVItem, (_itemCountPerBlock + 1));
    _cursorInBuffer = 0;

    if (_blockBuffer == nullptr) {
        AUTIL_LOG(ERROR, "new blockBuffer failed, blockSize[%lu]", sizeof(KVItem) * (_itemCountPerBlock + 1));
        return false;
    }
    _fileWriter = fileWriter;
    _blockTail.clear();
    return true;
}

template <typename Key, typename Value>
inline Status BlockArrayWriter<Key, Value>::AddItem(const Key& key, const Value& value)
{
    _blockBuffer[_cursorInBuffer].key = key;
    _blockBuffer[_cursorInBuffer].value = value;
    _itemCount++;
    _cursorInBuffer++;
    if (_cursorInBuffer == _itemCountPerBlock) {
        RETURN_IF_STATUS_ERROR(WriteBlock(false), "write block fail");
    }
    return Status::OK();
}

/// Block Layout:
// (note: kv means key and values)
// | kv0 kv1 kv2 kv3 ......  kvN  padding(zeros) | kv0 kv1 kv2 kv3 ......  kvN  padding(zeros) |
// | -----                 Block       -------   | |-----                 Block       -------  |
template <typename Key, typename Value>
Status BlockArrayWriter<Key, Value>::WriteBlock(bool needPadding)
{
    // write block data [0, _cursorInBuffer)
    assert(_cursorInBuffer > 0);
    // the last block's key may be less then @_itemCountPerBlock, maybe no padding
    // |---------------------------|-----|             Not-Last Block
    //        filled                fixed padding
    // |--------|                                      Last Block (maybe no padding)
    //   filled
    uint64_t dataLength = sizeof(KVItem) * _cursorInBuffer;
    char* paddingOffset = (char*)_blockBuffer + dataLength;
    uint64_t paddingLength = needPadding ? 0 : _dataBlockSize - dataLength;
    memset(paddingOffset, 0, paddingLength);

    auto [status, writeSize] = _fileWriter->Write(_blockBuffer, dataLength + paddingLength).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "file[%s] write fail", _fileWriter->GetLogicalPath().c_str());
    if (writeSize != dataLength + paddingLength) {
        RETURN_IF_STATUS_ERROR(Status::IOError(), "Write block error, needPadding[%d], blockSize[%lu], write size[%lu]",
                               needPadding, dataLength + paddingLength, writeSize);
    }
    _blockTail.push_back(_blockBuffer[_cursorInBuffer - 1].key);
    _cursorInBuffer = 0;
    return Status::OK();
}

// Block Array Layout:
// | Block0 Block1 ... BlockN | item count (8bytes) | data block size(8bytes) | Meta |
// |       Block Data         |              Block Data Info                  | Meta |
template <typename Key, typename Value>
Status BlockArrayWriter<Key, Value>::Finish()
{
    if (_cursorInBuffer > 0) {
        RETURN_IF_STATUS_ERROR(WriteBlock(true), "write block fail");
    }
    auto pairRet = _fileWriter->Write(&_itemCount, sizeof(_itemCount)).StatusWith();
    RETURN_IF_STATUS_ERROR(pairRet.first, "file[%s] write fail", _fileWriter->GetLogicalPath().c_str());
    uint64_t writeSize = pairRet.second;
    if (writeSize != sizeof(_itemCount)) {
        RETURN_IF_STATUS_ERROR(Status::IOError(), "Write _itemCount error, expected size[%lu], write size[%lu]",
                               sizeof(_itemCount), writeSize);
    }

    pairRet = _fileWriter->Write(&_dataBlockSize, sizeof(_dataBlockSize)).StatusWith();
    RETURN_IF_STATUS_ERROR(pairRet.first, "file[%s] write fail", _fileWriter->GetLogicalPath().c_str());
    writeSize = pairRet.second;
    if (writeSize != sizeof(_dataBlockSize)) {
        RETURN_IF_STATUS_ERROR(Status::IOError(), "Write _dataBlockSize error, expected size[%lu], write size[%lu]",
                               sizeof(_dataBlockSize), writeSize);
    }

    RETURN_IF_STATUS_ERROR(WriteMeta(), "write meta fail");
    return Status::OK();
}

// Meta Layout:
// | key0 key1 key2 ----- keyN | meta count per unit (8byte) | bottom metaKey count (8byte) |
// |  Meta Index (no padding)  |                      Meta Index Info                       |

// Meta Index: some keys are redundant, it's a multi-level data struct
//
template <typename Key, typename Value>
Status BlockArrayWriter<Key, Value>::WriteMeta()
{
    uint64_t metaCountPerUnit = std::max((uint64_t)std::sqrt(_blockTail.size()), MIN_META_COUNT_PER_BLOCK);
    if (metaCountPerUnit * metaCountPerUnit < _blockTail.size()) {
        metaCountPerUnit++;
    }
    auto status = RecursiveWriteMeta(_blockTail, metaCountPerUnit);
    RETURN_IF_STATUS_ERROR(status, "recursive write meta fail");

    auto pairRet = _fileWriter->Write(&metaCountPerUnit, sizeof(metaCountPerUnit)).StatusWith();
    RETURN_IF_STATUS_ERROR(pairRet.first, "file[%s] write fail", _fileWriter->GetLogicalPath().c_str());
    uint64_t writeSize = pairRet.second;
    if (writeSize != sizeof(metaCountPerUnit)) {
        RETURN_IF_STATUS_ERROR(Status::IOError(), "Write metaCountPerUnit error, expected size[%lu], write size[%lu]",
                               sizeof(metaCountPerUnit), writeSize);
    }
    uint64_t bottomMetakeyCount = _blockTail.size();
    pairRet = _fileWriter->Write(&bottomMetakeyCount, sizeof(bottomMetakeyCount)).StatusWith();
    RETURN_IF_STATUS_ERROR(pairRet.first, "file[%s] write fail", _fileWriter->GetLogicalPath().c_str());
    writeSize = pairRet.second;
    if (writeSize != sizeof(bottomMetakeyCount)) {
        RETURN_IF_STATUS_ERROR(Status::IOError(), "Write bottomMetakeyCount error, expected size[%lu], write size[%lu]",
                               sizeof(bottomMetakeyCount), writeSize);
    }

    return Status::OK();
}

template <typename Key, typename Value>
Status BlockArrayWriter<Key, Value>::RecursiveWriteMeta(const std::vector<Key, AllocatorType>& metaKeyQueue,
                                                        uint64_t metaCountPerUnit)
{
    AllocatorType allocater(_pool);
    std::vector<Key, AllocatorType> nextQueue(allocater);
    if (metaKeyQueue.size() > metaCountPerUnit) {
        for (uint64_t i = metaCountPerUnit - 1; i < metaKeyQueue.size(); i += metaCountPerUnit) {
            nextQueue.emplace_back(metaKeyQueue[i]);
        }
        if (metaKeyQueue.size() % metaCountPerUnit != 0) {
            nextQueue.emplace_back(metaKeyQueue.back());
        }
    }
    if (!nextQueue.empty()) {
        auto status = RecursiveWriteMeta(nextQueue, metaCountPerUnit);
        RETURN_IF_STATUS_ERROR(status, "recursive write meta fail");
    }

    // write meta key after call method @RecursiveWriteMeta, ensures the high level meta will be written first
    // after load from disk, reader read operations between level are from low address to high memory
    for (auto& key : metaKeyQueue) {
        auto [status, writeSize] = _fileWriter->Write(&key, sizeof(Key)).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "file[%s] write fail", _fileWriter->GetLogicalPath().c_str());
        if (writeSize != sizeof(Key)) {
            RETURN_IF_STATUS_ERROR(Status::IOError(), "Write meta error, meta Size[%lu], write size[%lu]", sizeof(key),
                                   writeSize);
        }
    }
    return Status::OK();
}
}} // namespace indexlibv2::index
