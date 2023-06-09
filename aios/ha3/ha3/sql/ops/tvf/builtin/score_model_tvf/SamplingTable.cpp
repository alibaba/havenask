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
#include "ha3/sql/ops/tvf/builtin/SamplingTable.h"

#include "ha3/sql/common/Log.h"
#include "table/TableUtil.h"
#include "turing_ops_util/util/OpUtil.h"
#include "autil/StringUtil.h"

using namespace matchdoc;
using namespace table;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, SamplingTable);

bool SamplingTable::samplingTableToString(const table::TablePtr &table,
        const isearch::turing::SamplingInfo &samplingInfo,
        std::string &content) {

    size_t rowCount = table->getRowCount();
    size_t step = std::max((size_t)1, rowCount / samplingInfo.count);
    for (const auto &columnName: samplingInfo.fields) {
        auto column = table->getColumn(columnName);
        if (!column) {
            SQL_LOG(TRACE1, "not found column [%s] in table", columnName.c_str());
            return false;
        }
        content += columnName + ":";
        auto vt = column->getColumnSchema()->getType();
        bool isMulti = vt.isMultiValue();
        if (isMulti) {
            SQL_LOG(TRACE1,
                    "multivalue is not supported when convert table column[%s] to tensor",
                    columnName.c_str());
            return false;
        }
        switch (vt.getBuiltinType()) {
#define CASE_MACRO(bt)                                                  \
            case bt: {                                                  \
                typedef matchdoc::MatchDocBuiltinType2CppType<bt, false>::CppType T; \
                ColumnData<T> *columnData = column->getColumnData<T>(); \
                if (!columnData) {                                      \
                    SQL_LOG(TRACE1, "unexpected get column[%s] failed", columnName.c_str()); \
                    return false;                                       \
                }                                                       \
                for (size_t i = 0, j = 0;i < rowCount && j < samplingInfo.count; i += step, j++) { \
                    if (i != 0) {                                       \
                        content += ",";                                 \
                    }                                                   \
                    content += autil::StringUtil::toString(columnData->get(i)); \
                }                                                       \
                content += ";";                                         \
                break;                                                  \
            }
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO)
#undef CASE_MACRO
        case bt_string: {
            ColumnData<autil::MultiChar> *columnData = column->getColumnData<autil::MultiChar>();
            if (!columnData) {
                SQL_LOG(ERROR, "unexpected get column[%s] failed", columnName.c_str());
                return false;
            }
            for (size_t i = 0, j = 0; i < rowCount && j < samplingInfo.count;
                 i += step, j++) {
                if (i != 0) {
                    content += ",";
                }
                const autil::MultiChar &val = columnData->get(i);
                content += string(val.data(), val.size()) ;
            }
            content += ";";
            break;
        }
        default: {
            SQL_LOG(TRACE1,
                    "not supported type [%s] when convert from table",
                    TableUtil::valueTypeToString(vt).c_str());
            return false;
        }
        }
    }
    return true;
}

} // namespace sql
} // namespace isearch
