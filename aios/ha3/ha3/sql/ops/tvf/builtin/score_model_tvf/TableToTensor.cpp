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

#include "ha3/sql/ops/tvf/builtin/TableToTensor.h"

#include "ha3/sql/common/Log.h"
#include "table/TableUtil.h"
#include "tensorflow/core/framework/types.h"
#include "turing_ops_util/util/OpUtil.h"

using namespace tensorflow;
using namespace matchdoc;
using namespace table;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, TableToTensor);

bool TableToTensor::convertColumnToTensor(const TablePtr &table,
                                          const std::string &columnName,
                                          const DataType &type,
                                          Tensor &tensor) {
    size_t rowCount = table->getRowCount();
    auto column = table->getColumn(columnName);
    if (!column) {
        SQL_LOG(ERROR, "not found column [%s] in table", columnName.c_str());
        return false;
    }
    auto vt = column->getColumnSchema()->getType();
    bool isMulti = vt.isMultiValue();
    if (isMulti) {
        SQL_LOG(ERROR,
                "multivalue is not supported when convert table column[%s] to tensor",
                columnName.c_str());
        return false;
    }
#define CASE_MACRO(bt)                                                                             \
    case bt: {                                                                                     \
        typedef matchdoc::MatchDocBuiltinType2CppType<bt, false>::CppType T;                       \
        typedef typename suez::turing::Type2TFType<T>::TFType TFType;                              \
        auto dtype = DataTypeToEnum<TFType>::value;                                                \
        if (dtype != type) {                                                                       \
            SQL_LOG(ERROR,                                                                         \
                    "column type [%s] is not equal to tensor type [%s]",                           \
                    DataTypeString(dtype).c_str(),                                                 \
                    DataTypeString(type).c_str());                                                 \
            return false;                                                                          \
        }                                                                                          \
        ColumnData<T> *columnData = column->getColumnData<T>();                                    \
        if (!columnData) {                                                                         \
            SQL_LOG(ERROR, "unexpected get column[%s] failed", columnName.c_str());                \
            return false;                                                                          \
        }                                                                                          \
        tensor = Tensor(type, TensorShape({int64_t(rowCount)}));                                   \
        auto tensorFlat = tensor.flat<TFType>();                                                   \
        for (size_t i = 0; i < rowCount; i++) {                                                    \
            tensorFlat(i) = columnData->get(i);                                                    \
        }                                                                                          \
        break;                                                                                     \
    }
    switch (vt.getBuiltinType()) {
    case bt_string: {
        if (DT_STRING != type) {
            SQL_LOG(ERROR,
                    "column type [%s] is not equal to tensor type [%s]",
                    DataTypeString(DT_STRING).c_str(),
                    DataTypeString(type).c_str());
            return false;
        }
        ColumnData<autil::MultiChar> *columnData = column->getColumnData<autil::MultiChar>();
        if (!columnData) {
            SQL_LOG(ERROR, "unexpected get column[%s] failed", columnName.c_str());
            return false;
        }
        tensor = Tensor(DT_STRING, TensorShape({int64_t(rowCount)}));
        auto tensorFlat = tensor.flat<string>();
        for (size_t i = 0; i < rowCount; i++) {
            const autil::MultiChar &val = columnData->get(i);
            tensorFlat(i) = string(val.data(), val.size());
        }
        break;
    }
        CASE_MACRO(bt_int32);
        CASE_MACRO(bt_int64);
        CASE_MACRO(bt_double);
        CASE_MACRO(bt_float);
    default: {
        SQL_LOG(ERROR,
                "not supported type [%s] when convert from table",
                TableUtil::valueTypeToString(vt).c_str());
        return false;
    }
    }
    return true;
}

} // namespace sql
} // namespace isearch
