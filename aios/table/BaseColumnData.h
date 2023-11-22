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

#include <assert.h>
#include <memory>
#include <string>
#include <vector>

#include "matchdoc/Reference.h"
#include "table/Row.h"

namespace autil {
class DataBuffer;
}

namespace table {

class BaseColumnData {
public:
    virtual ~BaseColumnData();

public:
    virtual void resize(uint32_t size) = 0;
    virtual bool merge(BaseColumnData &other) = 0;
    virtual bool copy(size_t startIndex, const BaseColumnData &other, size_t srcStartIndex, size_t count) = 0;
    virtual std::unique_ptr<BaseColumnData> share(const std::vector<Row> *rows) = 0;
    virtual std::string toString(size_t rowIndex) const = 0;
    virtual std::string toOriginString(size_t rowIndex) const = 0;
    virtual std::unique_ptr<matchdoc::ReferenceBase> toReference() const = 0;
    virtual void serialize(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) const = 0;
    virtual void deserialize(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) = 0;

    virtual uint32_t capacity() const = 0;
    virtual uint32_t usedBytes(uint32_t rowCount) const = 0;
    virtual uint32_t allocatedBytes() const = 0;

public:
    void attachRows(const std::vector<Row> *rows) { _rows = rows; }
    size_t getRowCount() const { return _rows->size(); }

protected:
    Row getRow(size_t rowIndex) const {
        assert(rowIndex < _rows->size());
        return _rows->at(rowIndex);
    }

protected:
    const std::vector<Row> *_rows = nullptr;
};

} // namespace table
