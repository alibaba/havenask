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
#include "indexlib/index/operation_log/CompressOperationBlock.h"

#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/operation_log/OperationFactory.h"
#include "indexlib/util/buffer_compressor/SnappyCompressor.h"

namespace indexlib::index {
namespace {
}

AUTIL_LOG_SETUP(indexlib.index, CompressOperationBlock);

CompressOperationBlock::CompressOperationBlock()
    : OperationBlock(0)
    , _operationCount(0)
    , _compressDataBuffer(NULL)
    , _compressDataLen(0)
{
}
CompressOperationBlock::CompressOperationBlock(const CompressOperationBlock& other)
    : OperationBlock(other)
    , _operationCount(other._operationCount)
    , _compressDataBuffer(other._compressDataBuffer)
    , _compressDataLen(other._compressDataLen)
{
}

CompressOperationBlock::~CompressOperationBlock() {}

void CompressOperationBlock::Init(size_t operationCount, const char* compressBuffer, size_t compressSize,
                                  int64_t opMinTimeStamp, int64_t opMaxTimeStamp)
{
    _operationCount = operationCount;
    _minTimestamp = opMinTimeStamp;
    _maxTimestamp = opMaxTimeStamp;

    assert(compressBuffer);
    assert(compressSize > 0);
    assert(_pool);

    char* copyBuffer = (char*)_pool->allocate(compressSize);
    assert(copyBuffer);
    memcpy(copyBuffer, compressBuffer, compressSize);
    _compressDataBuffer = copyBuffer;
    _compressDataLen = compressSize;
}

OperationBlock* CompressOperationBlock::Clone() const
{
    assert(_pool);
    assert(_allocator);
    return new CompressOperationBlock(*this);
}

size_t CompressOperationBlock::Size() const { return _operationCount; }

size_t CompressOperationBlock::GetCompressSize() const { return _compressDataLen; }

std::pair<Status, std::shared_ptr<OperationBlock>>
CompressOperationBlock::CreateOperationBlockForRead(const OperationFactory& opFactory)
{
    util::SnappyCompressor compressor;
    compressor.AddDataToBufferIn(_compressDataBuffer, _compressDataLen);
    if (!compressor.Decompress()) {
        AUTIL_LOG(ERROR, "fail to decompress operation block");
        return std::make_pair(Status::Corruption("fail to decompress operation block"), nullptr);
    }
    const char* buffer = compressor.GetBufferOut();
    size_t bufLen = compressor.GetBufferOutLen();
    compressor.GetInBuffer().release();

    std::shared_ptr<OperationBlock> opBlock(new OperationBlock(_operationCount));
    autil::mem_pool::Pool* pool = opBlock->GetPool();

    // copy in-stack buffer to in-pool buffer.
    // Because operation will use this buffer
    char* opBuffer = (char*)(pool->allocate(bufLen));
    char* baseAddr = opBuffer;
    memcpy(opBuffer, buffer, bufLen);
    compressor.GetOutBuffer().release();

    for (size_t i = 0; i < _operationCount; ++i) {
        size_t opSize = 0;
        auto [status, operation] = opFactory.DeserializeOperation(baseAddr, pool, opSize);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "deserialize operation log fail");
            return std::make_pair(status, nullptr);
        }
        opBlock->AddOperation(operation);
        baseAddr += opSize;
    }
    assert((size_t)(baseAddr - opBuffer) == bufLen);
    return std::make_pair(Status::OK(), opBlock);
}

Status CompressOperationBlock::Dump(const std::shared_ptr<file_system::FileWriter>& fileWriter,
                                    size_t maxOpSerializeSize)
{
    assert(fileWriter);
    auto [status, writeSize] = fileWriter->Write(_compressDataBuffer, _compressDataLen).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "write compress dump buffer failed");
    return Status::OK();
}

} // namespace indexlib::index
