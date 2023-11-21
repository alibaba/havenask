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
#include "table/Column.h"

#include "table/UserTypeColumnData.h"
#include "table/ValueTypeSwitch.h"

namespace table {

Column::Column(std::string name, ValueType type, std::unique_ptr<BaseColumnData> columnData)
    : Column(std::make_shared<ColumnSchema>(std::move(name), type), std::move(columnData)) {}

Column::Column(std::shared_ptr<ColumnSchema> columnSchema, std::unique_ptr<BaseColumnData> columnData)
    : _schema(std::move(columnSchema)), _data(std::move(columnData)) {}

Column::~Column() = default;

bool Column::merge(Column &other) {
    if (!_schema || !other._schema) {
        return false;
    }
    if (*_schema != *other._schema) {
        return false;
    }
    _data->merge(*other._data);
    return true;
}

bool Column::copy(size_t startIndex, const Column &other, size_t srcStartIndex, size_t count) {
    if (isUserType() || other.isUserType()) {
        return false;
    }
    return _data->copy(startIndex, *other._data, srcStartIndex, count);
}

void Column::serializeData(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) const {
    _data->serialize(dataBuffer, rows);
}

void Column::deserializeData(autil::DataBuffer &dataBuffer, const std::vector<Row> &rows) {
    _data->deserialize(dataBuffer, rows);
}

std::unique_ptr<matchdoc::ReferenceBase> Column::toReference() const {
    auto ref = _data->toReference();
    ref->setName(getName());
    ref->setGroupName(getName());
    ref->setSerializeLevel(_schema->isUserType() ? SL_NONE : SL_ATTRIBUTE);
    return ref;
}

std::unique_ptr<Column> Column::makeUserTypeColumnFromReference(matchdoc::ReferenceBase *ref) {
    auto data = UserTypeColumnData::fromReference(ref);
    if (!data) {
        return nullptr;
    }
    auto schema = std::make_unique<ColumnSchema>(ref->getName(), ref->getValueType(), ref->getVariableType());
    return std::make_unique<Column>(std::move(schema), std::move(data));
}

} // namespace table
