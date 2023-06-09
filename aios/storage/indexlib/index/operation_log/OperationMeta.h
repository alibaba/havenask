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
#include "indexlib/index/operation_log/OperationBlock.h"

namespace indexlib::index {

class OperationMeta : public autil::legacy::Jsonizable
{
public:
    struct BlockMeta : public autil::legacy::Jsonizable {
    public:
        BlockMeta();
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    public:
        int64_t minTimestamp;
        int64_t maxTimestamp;
        size_t serializeSize;
        size_t maxOperationSerializeSize;
        size_t dumpSize;
        uint32_t operationCount;
        bool operationCompress;
    };
    typedef std::vector<BlockMeta> BlockMetaVec;

public:
    OperationMeta();
    ~OperationMeta() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::string ToString() const { return autil::legacy::ToJsonString(*this); }

    void InitFromString(const std::string& metaContent) { autil::legacy::FromJsonString(*this, metaContent); }
    void Update(size_t opSerializeSize);
    void EndOneBlock(const std::shared_ptr<OperationBlock>& opBlock, int64_t blockIdx);
    void EndOneCompressBlock(const std::shared_ptr<OperationBlock>& opBlock, int64_t blockIdx, size_t compressSize);
    size_t GetLastBlockSerializeSize() const { return _totalSerializeSize - _noLastBlockSerializeSize; }
    size_t GetMaxOperationSerializeSize() const { return _maxOperationSerializeSize; }
    size_t GetTotalDumpSize() const { return _totalDumpSize; }
    size_t GetTotalSerializeSize() const { return _totalSerializeSize; }
    size_t GetOperationCount() const { return _operationCount; }
    const OperationMeta::BlockMetaVec& GetBlockMetaVec() const { return _blockMetaVec; }
    int64_t GetMaxTimestamp() const;

private:
    size_t _maxOperationSerializeSize;
    size_t _maxOperationSerializeSizeInCurBlock;
    size_t _operationCount;
    size_t _noLastBlockSerializeSize;
    size_t _totalSerializeSize;
    size_t _totalDumpSize;
    BlockMetaVec _blockMetaVec;
};

inline OperationMeta::BlockMeta::BlockMeta()
    : minTimestamp(INVALID_TIMESTAMP)
    , maxTimestamp(INVALID_TIMESTAMP)
    , serializeSize(0)
    , maxOperationSerializeSize(0)
    , dumpSize(0)
    , operationCount(0)
    , operationCompress(false)
{
}

inline void OperationMeta::BlockMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("min_operation_timestamp", minTimestamp, minTimestamp);
    json.Jsonize("max_operation_timestamp", maxTimestamp, maxTimestamp);
    json.Jsonize("block_serialize_size", serializeSize, serializeSize);
    json.Jsonize("max_operation_serialize_size", maxOperationSerializeSize, maxOperationSerializeSize);
    json.Jsonize("block_dump_size", dumpSize, dumpSize);
    json.Jsonize("operation_count", operationCount, operationCount);
    json.Jsonize("operation_compress", operationCompress, operationCompress);
}

inline OperationMeta::OperationMeta()
    : _maxOperationSerializeSize(0)
    , _maxOperationSerializeSizeInCurBlock(0)
    , _operationCount(0)
    , _noLastBlockSerializeSize(0)
    , _totalSerializeSize(0)
    , _totalDumpSize(0)
{
}

inline void OperationMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("total_serialize_size", _totalSerializeSize, _totalSerializeSize);
    json.Jsonize("total_dump_size", _totalDumpSize, _totalDumpSize);
    json.Jsonize("max_operation_serialize_size", _maxOperationSerializeSize, _maxOperationSerializeSize);
    json.Jsonize("operation_count", _operationCount, _operationCount);
    json.Jsonize("operation_block_meta", _blockMetaVec, _blockMetaVec);
}

inline void OperationMeta::Update(size_t opSerializeSize)
{
    _maxOperationSerializeSizeInCurBlock = std::max(_maxOperationSerializeSizeInCurBlock, opSerializeSize);
    _maxOperationSerializeSize = std::max(_maxOperationSerializeSize, opSerializeSize);
    ++_operationCount;
    _totalSerializeSize += opSerializeSize;
}

inline int64_t OperationMeta::GetMaxTimestamp() const
{
    int64_t maxTimestamp = INVALID_TIMESTAMP;
    for (auto blockMeta : _blockMetaVec) {
        if (blockMeta.maxTimestamp > maxTimestamp) {
            maxTimestamp = blockMeta.maxTimestamp;
        }
    }
    return maxTimestamp;
}

inline void OperationMeta::EndOneBlock(const std::shared_ptr<OperationBlock>& opBlock, int64_t blockIdx)
{
    if (opBlock->Size() == 0) {
        // empty block
        return;
    }

    if (blockIdx + 1 == (int64_t)_blockMetaVec.size()) {
        // already end current block
        return;
    }

    assert(blockIdx == (int64_t)_blockMetaVec.size());
    assert(opBlock->GetMinTimestamp() <= opBlock->GetMaxTimestamp());

    BlockMeta blockMeta;
    blockMeta.minTimestamp = opBlock->GetMinTimestamp();
    blockMeta.maxTimestamp = opBlock->GetMaxTimestamp();
    blockMeta.operationCount = opBlock->Size();
    blockMeta.serializeSize = _totalSerializeSize - _noLastBlockSerializeSize;
    blockMeta.maxOperationSerializeSize = _maxOperationSerializeSizeInCurBlock;
    blockMeta.dumpSize = blockMeta.serializeSize;
    blockMeta.operationCompress = false;

    _noLastBlockSerializeSize = _totalSerializeSize;
    _totalDumpSize += blockMeta.dumpSize;
    _blockMetaVec.push_back(blockMeta);
    _maxOperationSerializeSizeInCurBlock = 0;
}

inline void OperationMeta::EndOneCompressBlock(const std::shared_ptr<OperationBlock>& opBlock, int64_t blockIdx,
                                               size_t compressSize)
{
    if (opBlock->Size() == 0) {
        // empty block
        return;
    }

    if (blockIdx + 1 == (int64_t)_blockMetaVec.size()) {
        // already end current block
        return;
    }

    assert(blockIdx == (int64_t)_blockMetaVec.size());
    assert(opBlock->GetMinTimestamp() <= opBlock->GetMaxTimestamp());

    BlockMeta blockMeta;
    blockMeta.minTimestamp = opBlock->GetMinTimestamp();
    blockMeta.maxTimestamp = opBlock->GetMaxTimestamp();
    blockMeta.operationCount = opBlock->Size();
    blockMeta.serializeSize = _totalSerializeSize - _noLastBlockSerializeSize;
    blockMeta.maxOperationSerializeSize = _maxOperationSerializeSizeInCurBlock;
    blockMeta.dumpSize = compressSize;
    blockMeta.operationCompress = true;

    _noLastBlockSerializeSize = _totalSerializeSize;
    _totalDumpSize += blockMeta.dumpSize;
    _blockMetaVec.push_back(blockMeta);
    _maxOperationSerializeSizeInCurBlock = 0;
}

} // namespace indexlib::index
