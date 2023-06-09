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
#include "indexlib/index/orc/OrcLeafIterator.h"

#include "indexlib/index/orc/RowGroup.h"
#include "orc/Reader.hh"

namespace indexlibv2::index {

OrcLeafIterator::OrcLeafIterator(std::unique_ptr<orc::Reader> reader, std::unique_ptr<orc::RowReader> rowReader,
                                 uint32_t batchSize)
    : _reader(std::move(reader))
    , _rowReader(std::move(rowReader))
    , _rowType(_rowReader->getSelectedType().clone())
    , _batchSize(batchSize)
    , _currentRowId(0)
{
}

OrcLeafIterator::~OrcLeafIterator() {}

Status OrcLeafIterator::Seek(uint64_t rowId)
{
    if (rowId >= _reader->getNumberOfRows()) {
        _currentRowId = _reader->getNumberOfRows(); // mark eof
        return Status::OutOfRange("rowId: ", rowId, ", totalRowCount: ", _reader->getNumberOfRows());
    }
    try {
        _rowReader->seekToRow(rowId);
        _currentRowId = rowId;
    } catch (std::exception& e) {
        return Status::InternalError("Seek to row[", rowId, "] failed, exception: ", e.what());
    }
    return Status::OK();
}

bool OrcLeafIterator::HasNext() const { return _currentRowId < _reader->getNumberOfRows(); }

OrcLeafIterator::Result OrcLeafIterator::Next(size_t batchSize)
{
    // ListVectorBatch will resize in constructor by the batchSize
    uint64_t leftCount = _reader->getNumberOfRows() - _currentRowId;
    auto colVecBatch = _rowReader->createRowBatch(std::min(static_cast<uint64_t>(batchSize), leftCount));
    OrcLeafIterator::Result ret;
    try {
        if (_rowReader->next(*colVecBatch)) {
            _currentRowId += colVecBatch->numElements;
            ret.second = std::make_unique<RowGroup>(_rowType, std::move(colVecBatch));
        } else {
            ret.first = Status::OutOfRange();
        }
    } catch (std::exception& e) {
        ret.first = Status::InternalError(e.what());
    }
    return ret;
}

OrcLeafIterator::Result OrcLeafIterator::Next() { return Next(_batchSize); }

} // namespace indexlibv2::index
