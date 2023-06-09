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
#include "indexlib/index/operation_log/OperationIterator.h"

#include "indexlib/index/operation_log/OperationLogIndexer.h"
#include "indexlib/index/operation_log/SegmentOperationIterator.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, OperationIterator);

OperationIterator::OperationIterator(const std::vector<std::shared_ptr<OperationLogIndexer>>& indexers,
                                     const std::shared_ptr<OperationLogConfig>& indexConfig)
    : _currentSegIdx(-1)
    , _indexers(indexers)
    , _indexConfig(indexConfig)
{
}

OperationIterator::~OperationIterator() {}

Status OperationIterator::Init(const OperationCursor& skipCursor, const indexlibv2::framework::Locator& locator)
{
    _locator = locator;
    _lastOpCursor = skipCursor;
    int32_t pos = skipCursor.pos + 1;
    assert(pos >= 0);
    std::vector<std::shared_ptr<OperationLogIndexer>> newIndexers;
    for (auto indexer : _indexers) {
        segmentid_t segmentid = indexer->GetSegmentId();
        OperationMeta opMeta;
        if (segmentid < skipCursor.segId || !indexer->GetOperationMeta(opMeta)) {
            continue;
        }
        if (indexer->IsSealed() && opMeta.GetMaxTimestamp() < _locator.GetOffset()) {
            continue;
        }
        if (segmentid == skipCursor.segId && skipCursor.pos + 1 >= (int32_t)opMeta.GetOperationCount()) {
            pos = 0;
            continue;
        }
        newIndexers.push_back(indexer);
        _segMetaVec.push_back(opMeta);
    }
    _indexers.swap(newIndexers);
    if (!_indexers.empty()) {
        auto [status, iter] = LoadSegIterator(0, pos);
        RETURN_IF_STATUS_ERROR(status, "load segment iterator failed");
        _currentSegIter = iter;
    }
    return Status::OK();
}

std::pair<Status, std::shared_ptr<SegmentOperationIterator>> OperationIterator::LoadSegIterator(int32_t segmentIdx,
                                                                                                int32_t offset)
{
    if (segmentIdx >= (int)(_indexers.size())) {
        _currentSegIdx = segmentIdx;
        return {Status::OK(), nullptr};
    }
    auto [status, iter] = _indexers[segmentIdx]->CreateSegmentOperationIterator(offset, _locator);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "create operation iterator failed");
    _currentSegIdx = segmentIdx;
    UpdateLastCursor(iter);
    if (!iter || !iter->HasNext()) {
        iter.reset();
    }
    return {Status::OK(), iter};
}

void OperationIterator::UpdateLastCursor(const std::shared_ptr<SegmentOperationIterator>& iter)
{
    if (likely(iter != NULL)) {
        OperationCursor cursor = iter->GetLastCursor();
        if (likely(cursor.pos >= 0)) // when not read any operation, cursor.pos = -1
        {
            _lastOpCursor = cursor;
        }
    }
}

std::pair<Status, bool> OperationIterator::HasNext()
{
    if (likely(_currentSegIter && _currentSegIter->HasNext())) {
        return {Status::OK(), true};
    }

    // !mCurrentSegIter || !mCurrentSegIter->HasNext()
    UpdateLastCursor(_currentSegIter);
    auto [status, iter] = LoadSegIterator(_currentSegIdx + 1, 0);
    RETURN2_IF_STATUS_ERROR(status, false, "load segment iterator failed");
    _currentSegIter = iter;
    while (!_currentSegIter && _currentSegIdx < (int)(_indexers.size())) {
        auto [nextStatus, nextIter] = LoadSegIterator(_currentSegIdx + 1, 0);
        RETURN2_IF_STATUS_ERROR(status, false, "load segment iterator failed");
        _currentSegIter = nextIter;
    }
    return {Status::OK(), _currentSegIter != nullptr};
}

std::pair<Status, OperationBase*> OperationIterator::Next()
{
    assert(_currentSegIter);
    assert(_currentSegIter->HasNext());
    auto [status, op] = _currentSegIter->Next();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "next failed");
    _lastOpCursor = _currentSegIter->GetLastCursor();
    return {Status::OK(), op};
}

segmentid_t OperationIterator::GetCurrentSegmentId() const
{
    if (!_currentSegIter) {
        return INVALID_SEGMENTID;
    }
    return _currentSegIter->GetSegmentId();
}

OperationCursor OperationIterator::GetLastCursor() const { return _lastOpCursor; }

} // namespace indexlib::index
