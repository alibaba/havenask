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
#include "indexlib/partition/operation_queue/compress_operation_block.h"

#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/buffer_compressor/SnappyCompressor.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, CompressOperationBlock);

CompressOperationBlock::CompressOperationBlock()
    : OperationBlock(0)
    , mOperationCount(0)
    , mCompressDataBuffer(NULL)
    , mCompressDataLen(0)
{
}

CompressOperationBlock::~CompressOperationBlock() {}

void CompressOperationBlock::Init(size_t operationCount, const char* compressBuffer, size_t compressSize,
                                  int64_t opMinTimeStamp, int64_t opMaxTimeStamp)
{
    mOperationCount = operationCount;
    mMinTimestamp = opMinTimeStamp;
    mMaxTimestamp = opMaxTimeStamp;

    assert(compressBuffer);
    assert(compressSize > 0);
    assert(mPool);

    char* copyBuffer = (char*)mPool->allocate(compressSize);
    assert(copyBuffer);
    memcpy(copyBuffer, compressBuffer, compressSize);
    mCompressDataBuffer = copyBuffer;
    mCompressDataLen = compressSize;
}

void CompressOperationBlock::AddOperation(OperationBase* operation)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "compresss operation block not support AddOperation!");
}

OperationBlockPtr CompressOperationBlock::CreateOperationBlockForRead(const OperationFactory& opFactory)
{
    SnappyCompressor compressor;
    compressor.AddDataToBufferIn(mCompressDataBuffer, mCompressDataLen);
    if (!compressor.Decompress()) {
        INDEXLIB_FATAL_ERROR(Runtime, "fail to decompress operation block");
    }
    const char* buffer = compressor.GetBufferOut();
    size_t bufLen = compressor.GetBufferOutLen();
    compressor.GetInBuffer().release();

    OperationBlockPtr opBlock(new OperationBlock(mOperationCount));
    Pool* pool = opBlock->GetPool();

    // copy in-stack buffer to in-pool buffer.
    // Because operation will use this buffer
    char* opBuffer = (char*)(pool->allocate(bufLen));
    char* baseAddr = opBuffer;
    memcpy(opBuffer, buffer, bufLen);
    compressor.GetOutBuffer().release();

    for (size_t i = 0; i < mOperationCount; ++i) {
        size_t opSize = 0;
        OperationBase* operation = opFactory.DeserializeOperation(baseAddr, pool, opSize);
        opBlock->AddOperation(operation);
        baseAddr += opSize;
    }
    assert((size_t)(baseAddr - opBuffer) == bufLen);
    return opBlock;
}

void CompressOperationBlock::Dump(const FileWriterPtr& fileWriter, size_t maxOpSerializeSize, bool releaseAfterDump)
{
    assert(fileWriter);
    fileWriter->Write(mCompressDataBuffer, mCompressDataLen).GetOrThrow();
    if (releaseAfterDump) {
        // release operation memory, avoid dump memory peak
        Reset();
    }
}
}} // namespace indexlib::partition
