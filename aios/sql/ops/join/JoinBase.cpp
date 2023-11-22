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
#include "sql/ops/join/JoinBase.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "sql/common/Log.h"
#include "sql/ops/condition/ExprUtil.h"
#include "sql/ops/util/KernelUtil.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"
#include "table/ValueTypeSwitch.h"

using namespace std;
using namespace table;
using namespace autil;

namespace sql {

JoinBase::JoinBase(const JoinBaseParamR &joinBaseParam)
    : _joinBaseParam(joinBaseParam) {}

bool JoinBase::initJoinedTable(const table::TablePtr &leftTable,
                               const table::TablePtr &rightTable,
                               table::TablePtr &outputTable) {
    size_t outputSize = _joinBaseParam._outputFields.size();
    size_t leftSize = min(_joinBaseParam._leftInputFields.size(), outputSize);
    for (size_t i = 0; i < leftSize; i++) {
        const string &outputField = _joinBaseParam._outputFields[i];
        const string &inputField = _joinBaseParam._leftInputFields[i];
        if (!declareTableColumn(leftTable, outputTable, inputField, outputField)) {
            return false;
        }
    }
    for (size_t i = leftSize; i < outputSize; i++) {
        const string &outputField = _joinBaseParam._outputFields[i];
        const string &inputField = _joinBaseParam._rightInputFields[i - leftSize];
        if (!declareTableColumn(rightTable, outputTable, inputField, outputField)) {
            return false;
        }
    }

    return true;
}

bool JoinBase::declareTableColumn(const table::TablePtr &inputTable,
                                  table::TablePtr &outputTable,
                                  const std::string &inputField,
                                  const std::string &outputField) const {
    table::Column *tableColumn = nullptr;
    ValueType vt;
    if (!getColumnInfo(inputTable, inputField, tableColumn, vt)) {
        SQL_LOG(ERROR, "get column info failed");
        return false;
    }
    Column *column = outputTable->declareColumn(outputField, vt);
    if (column == NULL) {
        SQL_LOG(ERROR, "declare column [%s] failed", outputField.c_str());
        return false;
    }
    return true;
}

bool JoinBase::getColumnInfo(const table::TablePtr &table,
                             const string &field,
                             table::Column *&tableColumn,
                             ValueType &vt) const {
    tableColumn = table->getColumn(field);
    if (tableColumn == nullptr) {
        SQL_LOG(ERROR, "column [%s] not found in table", field.c_str());
        return false;
    }
    auto schema = tableColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(ERROR, "get column [%s] schema failed", field.c_str());
        return false;
    }
    vt = schema->getType();
    return true;
}

bool JoinBase::appendColumns(const table::TablePtr &inputTable,
                             table::TablePtr &outputTable) const {
    size_t outputSize
        = min(_joinBaseParam._outputFields.size(), _joinBaseParam._leftInputFields.size());
    for (size_t i = 0; i < outputSize; i++) {
        const string &outputField = _joinBaseParam._outputFields[i];
        const string &inputField = _joinBaseParam._leftInputFields[i];
        if (!declareTableColumn(inputTable, outputTable, inputField, outputField)) {
            return false;
        }
    }
    return true;
}

bool JoinBase::evaluateJoinedTable(const std::vector<size_t> &leftTableIndexes,
                                   const std::vector<size_t> &rightTableIndexes,
                                   const table::TablePtr &leftTable,
                                   const table::TablePtr &rightTable,
                                   table::TablePtr &outputTable) {
    if (leftTableIndexes.size() != rightTableIndexes.size()) {
        SQL_LOG(ERROR,
                "tableA size[%zu] not match tableB size[%zu]",
                leftTableIndexes.size(),
                rightTableIndexes.size());
        return false;
    }

    _joinBaseParam._joinInfoR->incJoinCount(leftTableIndexes.size());
    size_t joinIndexStart = outputTable->getRowCount();
    outputTable->batchAllocateRow(leftTableIndexes.size());

    if (!evaluateLeftTableColumns(leftTableIndexes, leftTable, joinIndexStart, outputTable)) {
        SQL_LOG(ERROR, "evaluate columns for left table failed");
        return false;
    }
    if (!evaluateRightTableColumns(rightTableIndexes, rightTable, joinIndexStart, outputTable)) {
        SQL_LOG(ERROR, "evaluate columns for right table failed");
        return false;
    }

    return true;
}

bool JoinBase::evaluateLeftTableColumns(const vector<size_t> &rowIndexes,
                                        const table::TablePtr &inputTable,
                                        size_t joinIndexStart,
                                        table::TablePtr &outputTable) {
    size_t leftSize
        = min(_joinBaseParam._leftInputFields.size(), _joinBaseParam._outputFields.size());
    for (size_t i = 0; i < leftSize; i++) {
        const string &inputField = _joinBaseParam._leftInputFields[i];
        const string &outputField = _joinBaseParam._outputFields[i];
        if (!evaluateTableColumn(
                inputTable, outputTable, inputField, outputField, joinIndexStart, rowIndexes)) {
            return false;
        }
    }
    return true;
}

bool JoinBase::evaluateRightTableColumns(const vector<size_t> &rowIndexes,
                                         const table::TablePtr &inputTable,
                                         size_t joinIndexStart,
                                         table::TablePtr &outputTable) {
    size_t outputSize = _joinBaseParam._outputFields.size();
    size_t leftSize = min(_joinBaseParam._leftInputFields.size(), outputSize);
    for (size_t i = leftSize; i < outputSize; i++) {
        const string &outputField = _joinBaseParam._outputFields[i];
        const string &inputField = _joinBaseParam._rightInputFields[i - leftSize];
        if (!evaluateTableColumn(
                inputTable, outputTable, inputField, outputField, joinIndexStart, rowIndexes)) {
            return false;
        }
    }
    return true;
}

bool JoinBase::evaluateTableColumn(const table::TablePtr &inputTable,
                                   table::TablePtr &outputTable,
                                   const std::string &inputField,
                                   const std::string &outputField,
                                   size_t joinIndex,
                                   const std::vector<size_t> &rowIndexes) {
    table::Column *tableColumn = nullptr;
    ValueType vt;
    if (!getColumnInfo(inputTable, inputField, tableColumn, vt)) {
        SQL_LOG(ERROR, "get column [%s] info failed", inputField.c_str());
        return false;
    }
    auto newColumn = outputTable->getColumn(outputField);
    if (newColumn == nullptr) {
        SQL_LOG(ERROR, "get output table column [%s] failed", outputField.c_str());
        return false;
    }
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        auto columnData = tableColumn->getColumnData<T>();
        auto newColumnData = newColumn->getColumnData<T>();
        for (auto rowIndex : rowIndexes) {
            newColumnData->set(joinIndex, columnData->get(rowIndex));
            joinIndex++;
        }
        return true;
    };
    if (!ValueTypeSwitch::switchType(vt, func, func)) {
        SQL_LOG(ERROR, "evaluate columns failed");
        return false;
    }
    return true;
}

bool JoinBase::evaluateEmptyColumns(size_t fieldIndex,
                                    size_t joinIndex,
                                    table::TablePtr &outputTable) {
    Column *outputColumn = nullptr;
    ValueType vt;
    size_t index = fieldIndex;
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        if constexpr (std::is_same_v<T, autil::HllCtx>) {
            return false;
        } else {
            const std::string &type = ExprUtil::transVariableTypeToSqlType(vt.getBuiltinType());
            auto iter = _joinBaseParam._defaultValue.find(type);
            T val;
            InitializeIfNeeded<T>()(val);
            if (!type.empty() && iter != _joinBaseParam._defaultValue.end()) {
                if (!StringUtil::fromString<T>(iter->second, val)) {
                    SQL_LOG(ERROR,
                            "field [%s] type [%s] cast default value [%s] failed",
                            _joinBaseParam._outputFields[index].c_str(),
                            type.c_str(),
                            iter->second.c_str());
                    return false;
                }
            }
            auto outputColumnData = outputColumn->getColumnData<T>();
            for (size_t i = joinIndex; i < outputTable->getRowCount(); ++i) {
                outputColumnData->set(i, val);
            }
            return true;
        }
    };
    auto strFunc = [&]() {
        const std::string &type = ExprUtil::transVariableTypeToSqlType(vt.getBuiltinType());
        auto iter = _joinBaseParam._defaultValue.find(type);
        if (!type.empty() && iter != _joinBaseParam._defaultValue.end()) {
            auto outputColumnData = outputColumn->getColumnData<MultiChar>();
            for (size_t i = joinIndex; i < outputTable->getRowCount(); ++i) {
                outputColumnData->set(i, iter->second.data(), iter->second.size());
            }
        }
        return true;
    };
    auto multiFunc = [&](auto a) { return true; };
    auto multiStrFunc = [&]() { return true; };

    size_t outputSize = _joinBaseParam._outputFields.size();
    for (; index < outputSize; index++) {
        const string &outputField = _joinBaseParam._outputFields[index];
        outputColumn = nullptr;
        if (!getColumnInfo(outputTable, outputField, outputColumn, vt)) {
            SQL_LOG(ERROR, "get column [%s] info failed", outputField.c_str());
            return false;
        }
        if (!ValueTypeSwitch::switchType(vt, func, multiFunc, strFunc, multiStrFunc)) {
            SQL_LOG(ERROR, "evaluate empty columns failed");
            return false;
        }
    }
    return true;
}

} // namespace sql
