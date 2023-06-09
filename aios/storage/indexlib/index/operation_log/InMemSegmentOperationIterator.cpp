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
#include "indexlib/index/operation_log/InMemSegmentOperationIterator.h"

namespace indexlib::index {
namespace {
}

AUTIL_LOG_SETUP(indexlib.index, InMemSegmentOperationIterator);

InMemSegmentOperationIterator::InMemSegmentOperationIterator(
    const std::shared_ptr<OperationLogConfig>& opConfig, const std::shared_ptr<OperationFieldInfo>& operationFieldInfo,
    const OperationMeta& operationMeta, segmentid_t segmentId, size_t offset,
    const indexlibv2::framework::Locator& locator)
    : SegmentOperationIterator(opConfig, operationMeta, segmentId, offset, locator)
    , _opBlockIdx(-1)
    , _inBlockOffset(-1)
    , _operationFieldInfo(operationFieldInfo)
    , _curReadBlockIdx(-1)
    , _reservedOperation(NULL)
{
}

InMemSegmentOperationIterator::~InMemSegmentOperationIterator()
{
    for (size_t i = 0; i < _reservedOpVec.size(); ++i) {
        _reservedOpVec[i]->~OperationBase();
    }
}

Status InMemSegmentOperationIterator::Init(const OperationBlockVec& opBlocks)
{
    _opBlocks = opBlocks;

    if (_opMeta.GetOperationCount() == 0 || _beginPos > _opMeta.GetOperationCount() - 1) {
        AUTIL_LOG(INFO, "beginPos [%lu] is not valid, operation count is [%lu]", _beginPos,
                  _opMeta.GetOperationCount());
        _lastCursor.pos = (int32_t)_opMeta.GetOperationCount() - 1;
        return Status::InvalidArgs("begin pos is not valid");
    }

    // seek to operation (to be read) by _beginPos
    int32_t blockIdx = 0;
    size_t count = 0;
    while (count < _beginPos && blockIdx < (int32_t)_opBlocks.size()) {
        size_t blockOpCount = _opBlocks[blockIdx]->Size();
        if (_beginPos - count < blockOpCount) {
            break;
        }
        count += blockOpCount;
        ++blockIdx;
    }
    assert(count < _opMeta.GetOperationCount());
    _opBlockIdx = blockIdx;
    _inBlockOffset = _beginPos - count;
    _lastCursor.pos = _beginPos - 1;
    _reservedOperation = NULL;
    // seek next valid operation by timestamp
    SeekNextValidOpBlock();
    RETURN_IF_STATUS_ERROR(SeekNextValidOperation(), "seek valid operation failed");
    return Status::OK();
}

std::pair<Status, OperationBase*> InMemSegmentOperationIterator::Next()
{
    assert(HasNext());
    RETURN2_IF_STATUS_ERROR(SwitchToReadBlock(_opBlockIdx), nullptr, "switch block failed");
    _reservedOperation = _curBlockForRead->GetOperations()[_inBlockOffset];
    ToNextReadPosition();
    RETURN2_IF_STATUS_ERROR(SeekNextValidOperation(), nullptr, "seek next valid operation failed");
    OperationBase* op = _reservedOperation;
    if (op) {
        op->SetOperationFieldInfo(_operationFieldInfo);
    }
    _reservedOperation = NULL;
    return {Status::OK(), op};
}

inline void InMemSegmentOperationIterator::ToNextReadPosition()
{
    assert(_opBlockIdx < (int32_t)_opBlocks.size());

    ++_lastCursor.pos;
    ++_inBlockOffset;
    if (_inBlockOffset == (int32_t)_opBlocks[_opBlockIdx]->Size()) {
        ++_opBlockIdx;
        _inBlockOffset = 0;
        SeekNextValidOpBlock();
    }
}

void InMemSegmentOperationIterator::SeekNextValidOpBlock()
{
    while (_opBlockIdx < (int32_t)_opBlocks.size() &&
           _opBlocks[_opBlockIdx]->GetMaxTimestamp() < _locator.GetOffset()) {
        if (_opBlockIdx == _opBlocks.size() - 1) {
            // last block
            _lastCursor.pos = int32_t(_opMeta.GetOperationCount()) - 1;
            break;
        }
        _lastCursor.pos += _opBlocks[_opBlockIdx]->Size() - _inBlockOffset;
        ++_opBlockIdx;
        _inBlockOffset = 0;
    }
}

Status InMemSegmentOperationIterator::SwitchToReadBlock(int32_t blockIdx)
{
    if (blockIdx == _curReadBlockIdx) {
        return Status::OK();
    }
    if (_reservedOperation) {
        static const size_t RESET_POOL_THRESHOLD = 10 * 1024 * 1024; // 10 Mbytes
        if (_pool.getUsedBytes() >= RESET_POOL_THRESHOLD) {
            for (size_t i = 0; i < _reservedOpVec.size(); ++i) {
                _reservedOpVec[i]->~OperationBase();
            }
            _reservedOpVec.clear();
            _pool.reset();
        }
        _reservedOperation = _reservedOperation->Clone(&_pool);
        _reservedOpVec.push_back(_reservedOperation);
    }
    auto [status, block] = _opBlocks[blockIdx]->CreateOperationBlockForRead(_opFactory);
    RETURN_IF_STATUS_ERROR(status, "create operation block failed");
    _curBlockForRead = block;
    _curReadBlockIdx = blockIdx;
    return Status::OK();
}

Status InMemSegmentOperationIterator::SeekNextValidOperation()
{
    while (HasNext()) {
        RETURN_IF_STATUS_ERROR(SwitchToReadBlock(_opBlockIdx), "switch operation block failed");
        OperationBase* operation = _curBlockForRead->GetOperations()[_inBlockOffset];
        uint16_t hashId = operation->GetHashId();
        int64_t timestamp = operation->GetTimestamp();
        auto result = _locator.IsFasterThan(hashId, timestamp);
        if (result == indexlibv2::framework::Locator::LocatorCompareResult::LCR_INVALID) {
            AUTIL_LOG(ERROR, "compare locator [%s] with timestamp [%ld] hash id [%u] failed",
                      _locator.DebugString().c_str(), timestamp, hashId);
            return Status::Corruption("compare locator failed");
        }
        if (result == indexlibv2::framework::Locator::LocatorCompareResult::LCR_SLOWER) {
            break;
        }
        ToNextReadPosition();
    }
    return Status::OK();
}

} // namespace indexlib::index
