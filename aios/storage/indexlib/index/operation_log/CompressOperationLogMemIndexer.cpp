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
#include "indexlib/index/operation_log/CompressOperationLogMemIndexer.h"

#include "indexlib/index/operation_log/CompressOperationBlock.h"
#include "indexlib/index/operation_log/OperationDumper.h"
#include "indexlib/util/buffer_compressor/SnappyCompressor.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, CompressOperationLogMemIndexer);

CompressOperationLogMemIndexer::CompressOperationLogMemIndexer(segmentid_t segmentid)
    : OperationLogMemIndexer(segmentid)
{
}

CompressOperationLogMemIndexer::~CompressOperationLogMemIndexer() {}

size_t CompressOperationLogMemIndexer::GetCurrentMemoryUse() const
{
    return OperationLogMemIndexer::GetCurrentMemoryUse() + GetMaxBuildingBufferSize();
}

size_t CompressOperationLogMemIndexer::GetMaxBuildingBufferSize() const
{
    // compressor in-buffer + compressor out-buffer + compressed-block-data
    return 3 * _maxBlockSize;
}

Status CompressOperationLogMemIndexer::CreateNewBlock(size_t maxBlockSize)
{
    RETURN_IF_STATUS_ERROR(FlushBuildingOperationBlock(), "flush building operation failed");
    std::shared_ptr<OperationBlock> newBlock(new OperationBlock(maxBlockSize));
    _opBlocks->push_back(newBlock);
    ResetBlockInfo(*_opBlocks);
    return Status::OK();
}

std::pair<Status, std::shared_ptr<CompressOperationBlock>>
CompressOperationLogMemIndexer::CreateCompressOperationBlock()
{
    size_t blockSerializeSize = _operationMeta.GetBuildingBlockSerializeSize();
    if (blockSerializeSize == 0) {
        return {Status::OK(), std::shared_ptr<CompressOperationBlock>()};
    }

    util::SnappyCompressor compressor;
    compressor.SetBufferInLen(blockSerializeSize);
    autil::DynamicBuf& inBuffer = compressor.GetInBuffer();

    char* buffer = inBuffer.getBuffer();
    size_t bufLen = blockSerializeSize;
    size_t opCount = _blockInfo._curBlock->Size();
    auto hasConcurrentIdx = _operationMeta.GetBlockMetaVec().rbegin()->hasConcurrentIdx;
    assert(opCount > 0);
    for (size_t i = 0; i < opCount; ++i) {
        size_t dumpSize = OperationBlock::DumpSingleOperation(_blockInfo._curBlock->GetOperations()[i], buffer, bufLen,
                                                              hasConcurrentIdx);
        buffer += dumpSize;
        bufLen -= dumpSize;
    }
    if ((size_t)(buffer - inBuffer.getBuffer()) != blockSerializeSize) {
        AUTIL_LOG(ERROR, "fail to serialize operation block[size:%lu]", blockSerializeSize);
        return {Status::Corruption("fail to serialize operation block"), nullptr};
    }

    inBuffer.movePtr(blockSerializeSize);
    if (!compressor.Compress()) {
        AUTIL_LOG(ERROR, "fail to compress operation block");
        return {Status::InternalError("fail to compress operation block"), nullptr};
    }

    const char* outBuffer = compressor.GetBufferOut();
    size_t compressedSize = compressor.GetBufferOutLen();
    std::shared_ptr<CompressOperationBlock> opBlock(new CompressOperationBlock);
    opBlock->Init(opCount, outBuffer, compressedSize, _blockInfo._curBlock->GetMinOffset(),
                  _blockInfo._curBlock->GetMaxOffset(), *(_operationMeta.GetBlockMetaVec().rbegin()));
    return {Status::OK(), opBlock};
}

Status CompressOperationLogMemIndexer::FlushBuildingOperationBlock()
{
    auto [status, opBlock] = CreateCompressOperationBlock();
    RETURN_IF_STATUS_ERROR(status, "create compress operation block failed");
    if (opBlock) {
        // replace curblock to compressblock
        _opBlocks->pop_back();
        _opBlocks->push_back(opBlock);
        _operationMeta.EndOneCompressBlock(opBlock, (int64_t)_opBlocks->size() - 1, opBlock->GetCompressSize());
    }
    return Status::OK();
}

bool CompressOperationLogMemIndexer::NeedCreateNewBlock() const
{
    return _operationMeta.GetBuildingBlockSerializeSize() >= _maxBlockSize ||
           _blockInfo._curBlock->Size() >= _maxBlockSize;
}

} // namespace indexlib::index
