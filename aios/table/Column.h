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
#include <stddef.h>
#include <string>

#include "autil/DataBuffer.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"

namespace table {

class Column {
public:
    Column(std::string name, ValueType type, std::unique_ptr<BaseColumnData> columnData);
    Column(std::shared_ptr<ColumnSchema> schema, std::unique_ptr<BaseColumnData> columnData);
    ~Column();

private:
    Column(const Column &);
    Column &operator=(const Column &);

public:
    void resize(uint32_t size) { _data->resize(size); }

    bool merge(Column &other);

    bool copy(size_t startIndex, const Column &other, size_t srcStartIndex, size_t count);

    uint32_t usedBytes(uint32_t rowCount) const { return _data->usedBytes(rowCount); }

    template <typename T>
    ColumnData<T> *getColumnData() {
        if (isUserType()) {
            return nullptr;
        }
        return dynamic_cast<ColumnData<T> *>(_data.get());
    }

    const std::string &getName() const { return _schema->getName(); }

    ValueType getType() const { return _schema->getType(); }

    bool isUserType() const { return _schema->isUserType(); }

    const ColumnSchema *getColumnSchema() const { return _schema.get(); }
    const std::shared_ptr<ColumnSchema> getSchemaPtr() const { return _schema; }
    BaseColumnData *getBaseColumnData() const { return _data.get(); }

    std::string toString(size_t row) const { return _data->toString(row); }

    std::string toOriginString(size_t row) const { return _data->toOriginString(row); }

    inline size_t getRowCount() const { return _data->getRowCount(); }

    void serializeData(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) const;
    void deserializeData(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows);

    void attachRows(const std::vector<Row> *rows) { _data->attachRows(rows); }

    std::unique_ptr<matchdoc::ReferenceBase> toReference() const;

public:
    template <typename T>
    static std::unique_ptr<Column> fromReference(matchdoc::Reference<T> *ref) {
        auto data = ColumnData<T>::fromReference(ref);
        return std::make_unique<Column>(ref->getName(), ref->getValueType(), std::move(data));
    }

    static std::unique_ptr<Column> makeUserTypeColumnFromReference(matchdoc::ReferenceBase *ref);

    template <typename T>
    static std::unique_ptr<Column> make(const std::string &name, const std::shared_ptr<autil::mem_pool::Pool> &pool) {
        auto data = std::make_unique<ColumnData<T>>(pool);
        return std::make_unique<Column>(name, matchdoc::ValueTypeHelper<T>::getValueType(), std::move(data));
    }

private:
    std::shared_ptr<ColumnSchema> _schema;
    std::unique_ptr<BaseColumnData> _data;
};

using ColumnPtr = std::shared_ptr<Column>;

} // namespace table
