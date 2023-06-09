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
#include "indexlib/index/operation_log/OperationBlock.h"

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/operation_log/OperationBase.h"
#include "indexlib/util/MMapAllocator.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, OperationBlock);

OperationBlock::OperationBlock(size_t maxBlockSize) : _minTimestamp(INVALID_TIMESTAMP), _maxTimestamp(INVALID_TIMESTAMP)
{
    _allocator.reset(new util::MMapAllocator);
    const static int64_t OPERATION_POOL_CHUNK_SIZE = DEFAULT_CHUNK_SIZE * 1024 * 1024; // 10M
    _pool.reset(new autil::mem_pool::Pool(_allocator.get(), OPERATION_POOL_CHUNK_SIZE));
    if (maxBlockSize > 0) {
        _operations.reset(new OperationVec);
        _operations->Init(_allocator, maxBlockSize);
    }
}
OperationBlock::OperationBlock(const OperationBlock& other)
    : _operations(other._operations)
    , _minTimestamp(other._minTimestamp)
    , _maxTimestamp(other._maxTimestamp)
    , _pool(other._pool)
    , _allocator(other._allocator)
{
}
OperationBlock::~OperationBlock() { Reset(); }

Status OperationBlock::Dump(const file_system::FileWriterPtr& fileWriter, size_t maxOpSerializeSize)
{
    assert(fileWriter);
    std::vector<char> buffer;
    buffer.resize(maxOpSerializeSize);
    for (size_t i = 0; i < Size(); ++i) {
        assert(_operations);
        OperationBase* op = (*_operations)[i];
        size_t dumpSize = DumpSingleOperation(op, (char*)buffer.data(), buffer.size());
        auto [status, writeSize] = fileWriter->Write(buffer.data(), dumpSize).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "write dump buffer failed");
    }
    return Status::OK();
}

OperationBlock* OperationBlock::Clone() const
{
    assert(_pool);
    assert(_allocator);
    return new OperationBlock(*this);
}

void OperationBlock::Reset()
{
    // OperationBlockForRead & buildOperationBlock shared same _operations
    // should call ~OperationBase only once
    if (_operations && _operations.use_count() == 1) {
        for (size_t i = 0; i < _operations->Size(); i++) {
            (*_operations)[i]->~OperationBase();
        }
    }
    _operations.reset();
    _pool.reset();
    _allocator.reset();
}

void OperationBlock::AddOperation(OperationBase* operation)
{
    assert(operation);
    assert(_operations);
    UpdateTimestamp(operation->GetTimestamp());
    _operations->PushBack(operation);
}

size_t OperationBlock::GetTotalMemoryUse() const
{
    size_t memUse = 0;
    if (_operations) {
        memUse += _operations->GetUsedBytes();
    }
    if (_pool) {
        memUse += _pool->getUsedBytes();
    }
    return memUse;
}

size_t OperationBlock::Size() const { return _operations ? _operations->Size() : 0; }

const OperationBlock::OperationVec& OperationBlock::GetOperations() const
{
    if (_operations) {
        return *_operations;
    }

    static OperationVec emptyOpVec;
    return emptyOpVec;
}

void OperationBlock::UpdateTimestamp(int64_t ts)
{
    assert(_operations);
    assert(_minTimestamp <= _maxTimestamp);
    if (unlikely(_operations->Empty())) {
        _maxTimestamp = ts;
        _minTimestamp = ts;
        return;
    }

    if (ts > _maxTimestamp) {
        _maxTimestamp = ts;
        return;
    }

    if (ts < _minTimestamp) {
        _minTimestamp = ts;
    }
}

std::pair<Status, std::shared_ptr<OperationBlock>>
OperationBlock::CreateOperationBlockForRead(const OperationFactory& mOpFactory)
{
    return {Status::OK(), std::shared_ptr<OperationBlock>(new OperationBlock(*this))};
}

autil::mem_pool::Pool* OperationBlock::GetPool() const { return _pool.get(); }
int64_t OperationBlock::GetMinTimestamp() const { return _minTimestamp; }
int64_t OperationBlock::GetMaxTimestamp() const { return _maxTimestamp; }

size_t OperationBlock::DumpSingleOperation(OperationBase* op, char* buffer, size_t bufLen)
{
    char* begin = buffer;
    char* cur = begin;
    uint8_t opType = (uint8_t)op->GetSerializedType();
    *(uint8_t*)cur = opType;
    cur += sizeof(uint8_t);
    int64_t timestamp = op->GetTimestamp();
    *(int64_t*)cur = timestamp;
    cur += sizeof(timestamp);
    uint16_t hashId = op->GetHashId();
    *(uint16_t*)cur = hashId;
    cur += sizeof(hashId);
    size_t dataLen = op->Serialize(cur, bufLen - (cur - begin));
    return (cur - begin) + dataLen;
}

} // namespace indexlib::index
