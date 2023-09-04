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

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Progress.h"
#include "indexlib/index/operation_log/OperationBase.h"
#include "indexlib/index/operation_log/OperationBlock.h"

namespace indexlib::index {

class OperationMeta : public autil::legacy::Jsonizable
{
public:
    struct BlockMeta : public autil::legacy::Jsonizable {
    public:
        BlockMeta() {}
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    public:
        indexlibv2::base::Progress::Offset minOffset = indexlibv2::base::Progress::INVALID_OFFSET;
        indexlibv2::base::Progress::Offset maxOffset = indexlibv2::base::Progress::INVALID_OFFSET;
        size_t serializeSize = 0;
        size_t maxOperationSerializeSize = 0;
        size_t dumpSize = 0;
        uint32_t operationCount = 0;
        bool operationCompress = false;
        bool hasConcurrentIdx = false;
    };
    typedef std::vector<BlockMeta> BlockMetaVec;

public:
    OperationMeta() {}
    ~OperationMeta() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::string ToString() const { return autil::legacy::ToJsonString(*this); }

    void InitFromString(const std::string& metaContent) { autil::legacy::FromJsonString(*this, metaContent); }
    void Update(OperationBase* op);
    void EndOneBlock(const std::shared_ptr<OperationBlock>& opBlock, int64_t blockIdx);
    void EndOneCompressBlock(const std::shared_ptr<OperationBlock>& opBlock, int64_t blockIdx, size_t compressSize);
    size_t GetBuildingBlockSerializeSize() const
    {
        if (!_hasBuildingBlock) {
            return 0;
        }
        return _blockMetaVec.rbegin()->serializeSize;
    }
    size_t GetMaxOperationSerializeSize() const { return _maxOperationSerializeSize; }
    size_t GetTotalDumpSize() const { return _totalDumpSize; }
    size_t GetOperationCount() const { return _operationCount; }
    const OperationMeta::BlockMetaVec& GetBlockMetaVec() const { return _blockMetaVec; }
    indexlibv2::base::Progress::Offset GetMaxOffset() const;

private:
    size_t GetOperaionSize(OperationBase* op) const;

private:
    size_t _maxOperationSerializeSize = 0;
    size_t _operationCount = 0;
    size_t _totalSerializeSize = 0;
    size_t _totalDumpSize = 0;
    BlockMetaVec _blockMetaVec;
    bool _hasBuildingBlock = false;
};

inline void OperationMeta::BlockMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("min_operation_timestamp", minOffset.first, minOffset.first);
    json.Jsonize("max_operation_timestamp", maxOffset.first, maxOffset.first);
    json.Jsonize("min_operation_concurrent_idx", minOffset.second, minOffset.second);
    json.Jsonize("max_operation_concurrent_idx", maxOffset.second, maxOffset.second);
    json.Jsonize("block_serialize_size", serializeSize, serializeSize);
    json.Jsonize("max_operation_serialize_size", maxOperationSerializeSize, maxOperationSerializeSize);
    json.Jsonize("block_dump_size", dumpSize, dumpSize);
    json.Jsonize("operation_count", operationCount, operationCount);
    json.Jsonize("operation_compress", operationCompress, operationCompress);
    json.Jsonize("has_concurrent_idx", hasConcurrentIdx, hasConcurrentIdx);
}

inline void OperationMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("total_serialize_size", _totalSerializeSize, _totalSerializeSize);
    json.Jsonize("total_dump_size", _totalDumpSize, _totalDumpSize);
    json.Jsonize("max_operation_serialize_size", _maxOperationSerializeSize, _maxOperationSerializeSize);
    json.Jsonize("operation_count", _operationCount, _operationCount);
    json.Jsonize("operation_block_meta", _blockMetaVec, _blockMetaVec);
}

inline size_t OperationMeta::GetOperaionSize(OperationBase* op) const
{
    return sizeof(uint8_t)    // 1 byte(operation type)
           + sizeof(int64_t)  // 8 byte timestamp
           + sizeof(uint16_t) // 1 byte hash id
           + op->GetSerializeSize();
}

inline void OperationMeta::Update(OperationBase* op)
{
    if (!_hasBuildingBlock) {
        _blockMetaVec.emplace_back();
        _hasBuildingBlock = true;
    }
    assert(_blockMetaVec.size());

    auto opSerializeSize = GetOperaionSize(op);
    auto blockMeta = _blockMetaVec.rbegin();
    if (op->GetDocInfo().concurrentIdx && !blockMeta->hasConcurrentIdx) {
        blockMeta->serializeSize += sizeof(uint32_t) * blockMeta->operationCount;
        _totalSerializeSize += sizeof(uint32_t) * blockMeta->operationCount;
        blockMeta->maxOperationSerializeSize += sizeof(uint32_t);
        blockMeta->hasConcurrentIdx = true;
    }
    if (blockMeta->hasConcurrentIdx) {
        opSerializeSize += sizeof(uint32_t);
    }

    blockMeta->operationCount++;
    blockMeta->serializeSize += opSerializeSize;
    blockMeta->maxOperationSerializeSize = std::max(blockMeta->maxOperationSerializeSize, opSerializeSize);
    _maxOperationSerializeSize = std::max(_maxOperationSerializeSize, blockMeta->maxOperationSerializeSize);
    ++_operationCount;
    _totalSerializeSize += opSerializeSize;
}

inline indexlibv2::base::Progress::Offset OperationMeta::GetMaxOffset() const
{
    auto maxOffset = indexlibv2::base::Progress::INVALID_OFFSET;
    for (auto blockMeta : _blockMetaVec) {
        if (blockMeta.maxOffset > maxOffset) {
            maxOffset = blockMeta.maxOffset;
        }
    }
    return maxOffset;
}

inline void OperationMeta::EndOneBlock(const std::shared_ptr<OperationBlock>& opBlock, int64_t blockIdx)
{
    if (opBlock->Size() == 0) {
        // empty block
        return;
    }

    if (!_hasBuildingBlock) {
        // already end current block
        return;
    }

    assert(blockIdx == (int64_t)_blockMetaVec.size() - 1);
    assert(opBlock->GetMinOffset() <= opBlock->GetMaxOffset());

    auto blockMeta = _blockMetaVec.rbegin();
    assert(blockMeta != _blockMetaVec.rend());
    blockMeta->minOffset = opBlock->GetMinOffset();
    blockMeta->maxOffset = opBlock->GetMaxOffset();
    assert(blockMeta->operationCount == opBlock->Size());
    blockMeta->dumpSize = blockMeta->serializeSize;
    blockMeta->operationCompress = false;

    _totalDumpSize += blockMeta->dumpSize;
    _hasBuildingBlock = false;
}

inline void OperationMeta::EndOneCompressBlock(const std::shared_ptr<OperationBlock>& opBlock, int64_t blockIdx,
                                               size_t compressSize)
{
    if (opBlock->Size() == 0) {
        // empty block
        return;
    }

    if (!_hasBuildingBlock) {
        // already end current block
        return;
    }

    assert(blockIdx == (int64_t)_blockMetaVec.size() - 1);
    assert(opBlock->GetMinOffset() <= opBlock->GetMaxOffset());

    auto blockMeta = _blockMetaVec.rbegin();
    assert(blockMeta != _blockMetaVec.rend());
    blockMeta->minOffset = opBlock->GetMinOffset();
    blockMeta->maxOffset = opBlock->GetMaxOffset();
    assert(blockMeta->operationCount == opBlock->Size());
    blockMeta->dumpSize = compressSize;
    blockMeta->operationCompress = true;

    _totalDumpSize += blockMeta->dumpSize;
    _hasBuildingBlock = false;
}

} // namespace indexlib::index
