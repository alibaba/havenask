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

#include "table/BaseColumnData.h"

namespace table {

// for non-built-in-types from MatchDocAllocator(e.g. MatchData), just hold the data
class UserTypeColumnData final : public BaseColumnData {
public:
    ~UserTypeColumnData();

public:
    void resize(uint32_t size) override;
    bool merge(BaseColumnData &other) override;
    bool copy(size_t startIndex, const BaseColumnData &other, size_t srcStartIndex, size_t count) override;
    std::unique_ptr<BaseColumnData> share(const std::vector<Row> *rows) override;
    std::string toString(size_t rowIndex) const override;
    std::string toOriginString(size_t rowIndex) const override;
    std::unique_ptr<matchdoc::ReferenceBase> toReference() const override;
    void serialize(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) const override;
    void deserialize(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) override;
    uint32_t capacity() const override;
    uint32_t usedBytes(uint32_t rowCount) const override;
    uint32_t allocatedBytes() const override;

public:
    static std::unique_ptr<UserTypeColumnData> fromReference(matchdoc::ReferenceBase *ref);

private:
    void reset();

private:
    std::shared_ptr<matchdoc::VectorStorage> _data;
    std::unique_ptr<matchdoc::ReferenceBase> _ref;
};

} // namespace table
