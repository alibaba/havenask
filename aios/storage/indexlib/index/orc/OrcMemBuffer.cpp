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
#include "indexlib/index/orc/OrcMemBuffer.h"

#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/orc/ColumnBuffer.h"
#include "indexlib/index/orc/OrcConfig.h"
#include "orc/Writer.hh"

using namespace std;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, OrcMemBuffer);

OrcMemBuffer::OrcMemBuffer(uint64_t numRowsPerBatch) : _numRowsPerBatch(numRowsPerBatch), _rowIdx(0) {}

OrcMemBuffer::~OrcMemBuffer()
{
    for (size_t i = 0; i < _columns.size(); i++) {
        _columns[i].reset();
    }
    _batchBase.reset();
}

Status OrcMemBuffer::Init(const indexlibv2::config::OrcConfig& config, orc::Writer* writer)
{
    _batchBase = writer->createRowBatch(_numRowsPerBatch);
    _structBatch = dynamic_cast<orc::StructVectorBatch*>(_batchBase.get());
    // start to create column buffers
    const auto& fieldConfigs = config.GetFieldConfigs();
    for (size_t i = 0; i < fieldConfigs.size(); i++) {
        const auto& fieldConfig = fieldConfigs[i];
        orc::ColumnVectorBatch* columnBatch = _structBatch->fields[i];
        auto column = ColumnBuffer::Create(fieldConfig.get(), _numRowsPerBatch, columnBatch);
        if (column == nullptr) {
            return Status::InvalidArgs("fail to create column");
        }
        _columns.push_back(std::move(column));
    }
    return Status::OK();
}

void OrcMemBuffer::Append(indexlib::document::AttributeDocument* doc)
{
    assert(_rowIdx != _numRowsPerBatch);
    for (auto& column : _columns) {
        const auto& field = doc->GetField(column->GetFieldId());
        column->Append(field, _rowIdx);
    }
    _rowIdx++;
}

bool OrcMemBuffer::IsFull() { return _rowIdx == _numRowsPerBatch; }

void OrcMemBuffer::Seal()
{
    for (auto& column : _columns) {
        column->Seal(_rowIdx);
    }
    _structBatch->numElements = _rowIdx;
}

void OrcMemBuffer::Reset()
{
    for (auto& column : _columns) {
        column->Reset();
    }
    _structBatch->reset();
    _rowIdx = 0;
}

} // namespace indexlibv2::index
