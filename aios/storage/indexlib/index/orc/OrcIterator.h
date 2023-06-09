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
#include <vector>

#include "indexlib/index/orc/IOrcReader.h"

namespace indexlibv2 { namespace config {
class OrcConfig;
}} // namespace indexlibv2::config

namespace orc {
class Type;
} // namespace orc

namespace indexlibv2::index {

class OrcIterator final : public IOrcIterator
{
public:
    using Result = IOrcIterator::Result;

public:
    explicit OrcIterator(const std::vector<std::shared_ptr<IOrcReader>>& readers, const ReaderOptions& readerOpts,
                         std::unique_ptr<orc::Type> type);
    ~OrcIterator();

public:
    Status Seek(uint64_t rowId) override;

    bool HasNext() const override;

    Result Next() override;

    const std::shared_ptr<orc::Type>& GetOrcType() const override { return _type; }

    uint64_t GetCurrentRowId() const override { return _currentRowId; }

private:
    void DecodeRowId(uint64_t globalRowId, size_t& segmentIdx, uint64_t& localRowId) const;
    Status MoveToNextNonEmptySegment(size_t& segmentIdx, std::unique_ptr<IOrcIterator>& iter) const;
    void MarkEof();

private:
    std::vector<std::shared_ptr<IOrcReader>> _segmentReaders; // TODO: maybe use reference
    std::shared_ptr<orc::Type> _type;
    uint64_t _currentRowId;
    uint64_t _totalRowCount;
    int32_t _currentIdx;
    std::unique_ptr<IOrcIterator> _currentIter;
    ReaderOptions _readerOpts;
};

} // namespace indexlibv2::index
