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
#include "indexlib/partition/operation_queue/operation_block.h"

#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/partition/operation_queue/operation_dumper.h"
#include "indexlib/util/MMapAllocator.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OperationBlock);

OperationBlock::OperationBlock(size_t maxBlockSize) : mMinTimestamp(INVALID_TIMESTAMP), mMaxTimestamp(INVALID_TIMESTAMP)
{
    mAllocator.reset(new util::MMapAllocator);
    mPool.reset(new autil::mem_pool::Pool(mAllocator.get(), OPERATION_POOL_CHUNK_SIZE));
    if (maxBlockSize > 0) {
        mOperations.reset(new OperationVec);
        mOperations->Init(mAllocator, maxBlockSize);
    }
}

OperationBlock::OperationBlock(const OperationBlock& other)
    : mOperations(other.mOperations)
    , mMinTimestamp(other.mMinTimestamp)
    , mMaxTimestamp(other.mMaxTimestamp)
    , mPool(other.mPool)
    , mAllocator(other.mAllocator)
{
}

void OperationBlock::Dump(const FileWriterPtr& fileWriter, size_t maxOpSerializeSize, bool releaseAfterDump)
{
    assert(fileWriter);
    vector<char> buffer;
    buffer.resize(maxOpSerializeSize);
    for (size_t i = 0; i < Size(); ++i) {
        assert(mOperations);
        OperationBase* op = (*mOperations)[i];
        size_t dumpSize = OperationDumper::DumpSingleOperation(op, (char*)buffer.data(), buffer.size());
        fileWriter->Write(buffer.data(), dumpSize).GetOrThrow();
    }

    if (releaseAfterDump) {
        // release operation memory, avoid dump memory peak
        Reset();
    }
}

OperationBlock* OperationBlock::Clone() const
{
    assert(mPool);
    assert(mAllocator);
    return new OperationBlock(*this);
}

void OperationBlock::Reset()
{
    // OperationBlockForRead & buildOperationBlock shared same mOperations
    // should call ~OperationBase only once
    if (mOperations && mOperations.use_count() == 1) {
        for (size_t i = 0; i < mOperations->Size(); i++) {
            (*mOperations)[i]->~OperationBase();
        }
    }
    mOperations.reset();
    mPool.reset();
    mAllocator.reset();
}

void OperationBlock::AddOperation(OperationBase* operation)
{
    assert(operation);
    assert(mOperations);
    UpdateTimestamp(operation->GetTimestamp());
    mOperations->PushBack(operation);
}

size_t OperationBlock::GetTotalMemoryUse() const
{
    size_t memUse = 0;
    if (mOperations) {
        memUse += mOperations->GetUsedBytes();
    }
    if (mPool) {
        memUse += mPool->getUsedBytes();
    }
    return memUse;
}

size_t OperationBlock::Size() const { return mOperations ? mOperations->Size() : 0; }

const OperationBlock::OperationVec& OperationBlock::GetOperations() const
{
    if (mOperations) {
        return *mOperations;
    }

    static OperationVec emptyOpVec;
    return emptyOpVec;
}

void OperationBlock::UpdateTimestamp(int64_t ts)
{
    assert(mOperations);
    assert(mMinTimestamp <= mMaxTimestamp);
    if (unlikely(mOperations->Empty())) {
        mMaxTimestamp = ts;
        mMinTimestamp = ts;
        return;
    }

    if (ts > mMaxTimestamp) {
        mMaxTimestamp = ts;
        return;
    }

    if (ts < mMinTimestamp) {
        mMinTimestamp = ts;
    }
}
}} // namespace indexlib::partition
