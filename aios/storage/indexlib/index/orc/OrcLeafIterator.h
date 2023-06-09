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

#include "indexlib/index/orc/IOrcIterator.h"

namespace orc {
class Reader;
class RowReader;
}; // namespace orc

namespace indexlibv2::index {

class OrcLeafIterator final : public IOrcIterator
{
public:
    using Result = IOrcIterator::Result;

    OrcLeafIterator(std::unique_ptr<orc::Reader> reader, std::unique_ptr<orc::RowReader> rowReader, uint32_t batchSize);
    ~OrcLeafIterator();

    Status Seek(uint64_t rowId) override;
    bool HasNext() const override;
    Result Next() override;
    Result Next(size_t batchSize);
    const std::shared_ptr<orc::Type>& GetOrcType() const override { return _rowType; }
    uint64_t GetCurrentRowId() const override { return _currentRowId; }

private:
    std::unique_ptr<orc::Reader> _reader;
    std::unique_ptr<orc::RowReader> _rowReader;
    std::shared_ptr<orc::Type> _rowType;
    const uint32_t _batchSize;
    uint64_t _currentRowId;
};

} // namespace indexlibv2::index
