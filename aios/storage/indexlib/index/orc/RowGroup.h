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
#pragma once

#include <memory>
#include <string>

namespace orc {
class Type;
struct ColumnVectorBatch;
} // namespace orc

namespace indexlibv2::index {

class RowGroup
{
public:
    RowGroup(const std::shared_ptr<orc::Type>& type, std::unique_ptr<orc::ColumnVectorBatch> rowBatch);
    ~RowGroup();

public:
    size_t GetRowCount() const;
    size_t GetColumnCount() const;

    const orc::Type* GetColumnType(size_t idx) const;
    orc::ColumnVectorBatch* GetColumn(size_t idx) const;
    size_t GetColumnIdx(const std::string& name) const;

    std::string DebugString() const;

    orc::ColumnVectorBatch* GetRowBatch() { return _rowBatch.get(); }

public:
    // only for ut
    const std::string& GetColumnName(size_t idx) const;

private:
    std::shared_ptr<orc::Type> _type;
    std::unique_ptr<orc::ColumnVectorBatch> _rowBatch;
};

} // namespace indexlibv2::index
