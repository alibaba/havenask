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
#include "indexlib/index/operation_log/FileOperationBlock.h"

#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/operation_log/OperationFactory.h"
#include "indexlib/util/buffer_compressor/SnappyCompressor.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, FileOperationBlock);

FileOperationBlock::FileOperationBlock() : OperationBlock(0), _readOffset(0) {}

FileOperationBlock::~FileOperationBlock() {}

OperationBlock* FileOperationBlock::Clone() const
{
    assert(false);
    return nullptr;
}

size_t FileOperationBlock::Size() const { return _blockMeta.operationCount; }
Status FileOperationBlock::Dump(const std::shared_ptr<file_system::FileWriter>& fileWriter, size_t maxOpSerializeSize)
{
    assert(false);
    return Status::Unimplement("file operation block not support dump");
}

void FileOperationBlock::Init(const std::shared_ptr<file_system::FileReader>& fileReader, size_t beginOffset,
                              const OperationMeta::BlockMeta& blockMeta)
{
    _fileReader = fileReader;
    _readOffset = beginOffset;
    _blockMeta = blockMeta;
    _minTimestamp = _blockMeta.minTimestamp;
    _maxTimestamp = _blockMeta.maxTimestamp;
}

std::pair<Status, std::shared_ptr<OperationBlock>>
FileOperationBlock::CreateOperationBlockForRead(const OperationFactory& opFactory)
{
    std::shared_ptr<OperationBlock> opBlock(new OperationBlock(_blockMeta.operationCount));
    autil::mem_pool::Pool* pool = opBlock->GetPool();

    auto [status, opBuffer] = CreateOperationBuffer(pool);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "create operation buffer failed");
    const char* baseAddr = opBuffer;

    for (size_t i = 0; i < _blockMeta.operationCount; ++i) {
        size_t opSize = 0;
        auto [status, operation] = opFactory.DeserializeOperation(baseAddr, pool, opSize);
        RETURN2_IF_STATUS_ERROR(status, nullptr, "deserialize operation failed");
        opBlock->AddOperation(operation);
        baseAddr += opSize;
    }
    assert((size_t)(baseAddr - opBuffer) == _blockMeta.serializeSize);
    return {Status::OK(), opBlock};
}

std::pair<Status, const char*> FileOperationBlock::CreateOperationBuffer(autil::mem_pool::Pool* pool)
{
    if (!_blockMeta.operationCompress) {
        return {Status::OK(), (char*)(_fileReader->GetBaseAddress()) + _readOffset};
    }
    util::SnappyCompressor compressor;
    compressor.SetBufferInLen(_blockMeta.dumpSize);
    compressor.AddDataToBufferIn((char*)(_fileReader->GetBaseAddress()) + _readOffset, _blockMeta.dumpSize);
    if (!compressor.Decompress()) {
        AUTIL_LOG(ERROR, "fail to decompress dumped operation block");
        return {Status::Corruption("fail to decompress dumped operation block"), nullptr};
    }
    size_t bufLen = compressor.GetBufferOutLen();
    if (bufLen != _blockMeta.serializeSize) {
        AUTIL_LOG(ERROR,
                  "block decompress size[%lu] "
                  "is not equal to block serializeSize[%lu]",
                  bufLen, _blockMeta.serializeSize);
        return {Status::Corruption("block decompress size invalid"), nullptr};
    }
    return {Status::OK(), (char*)memcpy((pool->allocate(bufLen)), compressor.GetBufferOut(), bufLen)};
}

} // namespace indexlib::index
