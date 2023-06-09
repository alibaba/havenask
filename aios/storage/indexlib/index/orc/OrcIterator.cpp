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
#include "indexlib/index/orc/OrcIterator.h"

#include <cassert>

#include "indexlib/index/orc/RowGroup.h"
#include "orc/Type.hh"
#include "orc/Vector.hh"

namespace indexlibv2::index {

OrcIterator::OrcIterator(const std::vector<std::shared_ptr<IOrcReader>>& readers, const ReaderOptions& readerOpts,
                         std::unique_ptr<orc::Type> type)
    : _segmentReaders(readers)
    , _type(std::move(type))
    , _currentRowId(0)
    , _totalRowCount(0)
    , _currentIdx(-1)
    , _readerOpts(readerOpts)
{
    for (const auto& segmentReader : _segmentReaders) {
        _totalRowCount += segmentReader->GetTotalRowCount();
    }
}

OrcIterator::~OrcIterator()
{
    _currentIter.reset();
    _segmentReaders.clear();
}

Status OrcIterator::Seek(uint64_t rowId)
{
    if (rowId >= _totalRowCount) {
        MarkEof();
        return Status::OutOfRange("Seek rowId: ", rowId, ", TotalRowCount: ", _totalRowCount);
    }

    size_t segmentIdx;
    uint64_t localRowId;
    DecodeRowId(rowId, segmentIdx, localRowId);

    std::unique_ptr<IOrcIterator> iter;
    auto s = MoveToNextNonEmptySegment(segmentIdx, iter);
    if (s.IsOK()) {
        s = iter->Seek(localRowId);
    }
    if (s.IsOK()) {
        _currentRowId = rowId;
        _currentIdx = segmentIdx;
        _currentIter = std::move(iter);
    }
    return s;
}

bool OrcIterator::HasNext() const { return _currentRowId < _totalRowCount; }

OrcIterator::Result OrcIterator::Next()
{
    assert(HasNext());
    if (_currentIter.get() == nullptr || !_currentIter->HasNext()) {
        size_t segmentIdx = _currentIdx + 1;
        std::unique_ptr<IOrcIterator> iter;
        auto s = MoveToNextNonEmptySegment(segmentIdx, iter);
        if (!s.IsOK()) {
            return {s, nullptr};
        } else {
            _currentIdx = segmentIdx;
            _currentIter = std::move(iter);
        }
    }

    assert(_currentIter.get() != nullptr);
    auto res = _currentIter->Next();
    if (res.first.IsOK()) {
        _currentRowId += res.second->GetRowCount();
    }
    return res;
}

void OrcIterator::DecodeRowId(uint64_t globalRowId, size_t& segmentIdx, uint64_t& localRowId) const
{
    uint64_t accumulatedRowCount = 0;
    for (size_t i = 0; i < _segmentReaders.size(); ++i) {
        uint64_t rowCount = _segmentReaders[i]->GetTotalRowCount();
        if (globalRowId >= accumulatedRowCount && globalRowId < rowCount + accumulatedRowCount) {
            segmentIdx = i;
            localRowId = globalRowId - accumulatedRowCount;
            break;
        } else {
            accumulatedRowCount += rowCount;
        }
    }
}

Status OrcIterator::MoveToNextNonEmptySegment(size_t& segmentIdx, std::unique_ptr<IOrcIterator>& iter) const
{
    while (segmentIdx < _segmentReaders.size()) {
        auto s = _segmentReaders[segmentIdx]->CreateIterator(_readerOpts, iter);
        if (!s.IsOK()) {
            return s;
        }
        if (!iter->HasNext()) {
            segmentIdx++;
        } else {
            return s;
        }
    }
    return Status::InternalError("unexpected eof, current segment index: ", _currentIdx);
}

void OrcIterator::MarkEof() { _currentRowId = _totalRowCount; }

} // namespace indexlibv2::index
