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
#ifndef __INDEXLIB_COMPRESS_OPERATION_BLOCK_H
#define __INDEXLIB_COMPRESS_OPERATION_BLOCK_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_block.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace partition {

class CompressOperationBlock : public OperationBlock
{
public:
    CompressOperationBlock();
    CompressOperationBlock(const CompressOperationBlock& other)
        : OperationBlock(other)
        , mOperationCount(other.mOperationCount)
        , mCompressDataBuffer(other.mCompressDataBuffer)
        , mCompressDataLen(other.mCompressDataLen)
    {
    }

    ~CompressOperationBlock();

public:
    void Init(size_t operationCount, const char* compressBuffer, size_t compressSize, int64_t opMinTimeStamp,
              int64_t opMaxTimeStamp);

    OperationBlock* Clone() const override
    {
        assert(mPool);
        assert(mAllocator);
        return new CompressOperationBlock(*this);
    }

    size_t Size() const override { return mOperationCount; }
    void AddOperation(OperationBase* operation) override;

    OperationBlockPtr CreateOperationBlockForRead(const OperationFactory& mOpFactory) override;

    void Dump(const file_system::FileWriterPtr& fileWriter, size_t maxOpSerializeSize, bool releaseAfterDump) override;

    size_t GetCompressSize() const { return mCompressDataLen; }

private:
    size_t mOperationCount;
    const char* mCompressDataBuffer;
    size_t mCompressDataLen;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressOperationBlock);
}} // namespace indexlib::partition

#endif //__INDEXLIB_COMPRESS_OPERATION_BLOCK_H
