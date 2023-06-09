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
#include "indexlib/index/orc/RowGroup.h"

#include <cassert>

#include "orc/Type.hh"
#include "orc/Vector.hh"

namespace indexlibv2::index {

RowGroup::RowGroup(const std::shared_ptr<orc::Type>& type, std::unique_ptr<orc::ColumnVectorBatch> rowBatch)
    : _type(type)
    , _rowBatch(std::move(rowBatch))
{
    assert(_type != nullptr);
    assert(orc::STRUCT == _type->getKind());
}

RowGroup::~RowGroup() {}

size_t RowGroup::GetRowCount() const
{
    if (_rowBatch == nullptr) {
        // empty row group
        return 0;
    }
    return _rowBatch->numElements;
}

size_t RowGroup::GetColumnCount() const { return _type->getSubtypeCount(); }

const std::string& RowGroup::GetColumnName(size_t idx) const
{
    assert(idx < _type->getSubtypeCount());
    return _type->getFieldName(idx);
}

const orc::Type* RowGroup::GetColumnType(size_t idx) const
{
    assert(idx < _type->getSubtypeCount());
    return _type->getSubtype(idx);
}

static inline orc::StructVectorBatch* AsStructBatch(orc::ColumnVectorBatch* rowBatch)
{
    return dynamic_cast<orc::StructVectorBatch*>(rowBatch);
}

orc::ColumnVectorBatch* RowGroup::GetColumn(size_t idx) const
{
    if (_rowBatch == nullptr) {
        return nullptr;
    }
    auto batch = AsStructBatch(_rowBatch.get());
    assert(batch != nullptr);
    assert(idx < batch->fields.size());
    return batch->fields[idx];
}

size_t RowGroup::GetColumnIdx(const std::string& name) const
{
    for (size_t i = 0; i < _type->getSubtypeCount(); i++) {
        if (_type->getFieldName(i) == name) {
            return i;
        }
    }
    return -1;
}

std::string RowGroup::DebugString() const { return _type->toDebugString(); }

} // namespace indexlibv2::index
