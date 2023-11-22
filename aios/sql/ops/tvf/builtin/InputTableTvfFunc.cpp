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
#include "sql/ops/tvf/builtin/InputTableTvfFunc.h"

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <utility>

#include "autil/MultiValueType.h"
#include "autil/Span.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/Row.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;

namespace sql {

const SqlTvfProfileInfo inputTableTvfInfo("inputTableTvf", "inputTableTvf");
const string inputTableTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "inputTableTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"string"
             }
         ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "tables": [
            {
              "auto_infer": true
            }
          ]
        }
      }
    ]
  }
}
)tvf_json";

const SqlTvfProfileInfo inputTableWithSepTvfInfo("inputTableWithSepTvf", "inputTableWithSepTvf");
const string inputTableWithSepTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "inputTableWithSepTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             }
         ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "tables": [
            {
              "auto_infer": true
            }
          ]
        }
      }
    ]
  }
}
)tvf_json";

InputTableTvfFunc::InputTableTvfFunc() {}

InputTableTvfFunc::~InputTableTvfFunc() {}

bool InputTableTvfFunc::init(TvfFuncInitContext &context) {
    _queryPool = context.queryPool;
    string inputTableInfoStr;
    string rowSep = ";";
    string colSep = ",";
    string multiValueSep = ":";
    auto paramSize = context.params.size();
    if (paramSize == 1u) {
        inputTableInfoStr = context.params[0];
    } else if (paramSize == 4u) {
        inputTableInfoStr = context.params[0];
        rowSep = context.params[1];
        colSep = context.params[2];
        multiValueSep = context.params[3];
    } else {
        SQL_LOG(ERROR, "invalid param size [%lu]", paramSize);
        return false;
    }
    if (rowSep.size() != 1u || colSep.size() != 1u || multiValueSep.size() != 1u) {
        SQL_LOG(ERROR,
                "invalid sep row[%s],col[%s],multi_value[%s]",
                rowSep.data(),
                colSep.data(),
                multiValueSep.data());
        return false;
    }
    _rowSep = rowSep[0];
    _colSep = colSep[0];
    _multiValueSep = multiValueSep[0];
    if (!prepareInputTableInfo(inputTableInfoStr, _inputTableInfo)) {
        SQL_LOG(ERROR, "prepare input table info failed [%s]", inputTableInfoStr.c_str());
        return false;
    }
    return true;
}

bool InputTableTvfFunc::doCompute(const TablePtr &input, TablePtr &output) {
    output = input;

    bool ret = fillInputTable(_inputTableInfo, output);
    if (!ret) {
        SQL_LOG(ERROR, "fill input table failed");
        return false;
    }
    return true;
}

bool InputTableTvfFunc::prepareInputTableInfo(const string &input,
                                              vector<vector<vector<string>>> &output) {
    if (input.empty()) {
        output = {{{string()}}};
        return true;
    }
    auto data = input.data();
    auto size = input.size();
    if (*input.rbegin() == _rowSep) {
        --size;
    }
    size_t columnCount = 0;
    StringView constInput(data, size);
    vector<StringView> rows
        = StringTokenizer::constTokenize(constInput, _rowSep, StringTokenizer::TOKEN_LEAVE_AS);
    output.reserve(rows.size());
    for (const auto &row : rows) {
        vector<StringView> cols
            = StringTokenizer::constTokenize(row, _colSep, StringTokenizer::TOKEN_LEAVE_AS);
        if (columnCount == 0) {
            columnCount = cols.size();
        } else if (columnCount != cols.size()) {
            SQL_LOG(WARN,
                    "Parse input table error: Column number not equal [%lu][%lu]",
                    columnCount,
                    row.size());
            return false;
        }
        vector<vector<string>> outputRow;
        outputRow.reserve(cols.size());
        for (const auto &col : cols) {
            vector<StringView> values = StringTokenizer::constTokenize(
                col, _multiValueSep, StringTokenizer::TOKEN_LEAVE_AS);
            vector<string> outputCol;
            outputCol.reserve(values.size());
            for (const auto &value : values) {
                outputCol.emplace_back(value.data(), value.size());
            }
            outputRow.push_back(move(outputCol));
        }
        output.push_back(move(outputRow));
    }
    return true;
}

#define NUMERIC_SWITCH_CASE(fbType, fieldType)                                                     \
    case fieldType: {                                                                              \
        typedef MatchDocBuiltinType2CppType<fieldType, false>::CppType cppType;                    \
        ColumnData<cppType> *columnData = outputColumn->getColumnData<cppType>();                  \
        if (!columnData) {                                                                         \
            break;                                                                                 \
        }                                                                                          \
        for (size_t rowIndex = 0; rowIndex < input.size(); rowIndex++) {                           \
            cppType colValue {0};                                                                  \
            vector<string> &value = input[rowIndex][colIndex];                                     \
            StringUtil::strTo##fbType(value[0].c_str(), colValue);                                 \
            columnData->set(rowIndex, colValue);                                                   \
        }                                                                                          \
        break;                                                                                     \
    }

#define MULTINUMERIC_SWITCH_CASE(fbType, fieldType)                                                \
    case fieldType: {                                                                              \
        typedef MatchDocBuiltinType2CppType<fieldType, false>::CppType cppType;                    \
        auto *columnData = outputColumn->getColumnData<MultiValueType<cppType>>();                 \
        if (!columnData) {                                                                         \
            break;                                                                                 \
        }                                                                                          \
        for (size_t rowIndex = 0; rowIndex < input.size(); rowIndex++) {                           \
            vector<string> &value = input[rowIndex][colIndex];                                     \
            vector<cppType> colValues;                                                             \
            for (auto strValue : value) {                                                          \
                cppType tempValue {0};                                                             \
                StringUtil::strTo##fbType(strValue.c_str(), tempValue);                            \
                colValues.push_back(tempValue);                                                    \
            }                                                                                      \
            columnData->set(rowIndex, colValues.data(), colValues.size());                         \
        }                                                                                          \
        break;                                                                                     \
    }

#define NUMERIC_SWITCH_CASE_HELPER(MY_MACRO)                                                       \
    MY_MACRO(UInt8, bt_uint8);                                                                     \
    MY_MACRO(Int8, bt_int8);                                                                       \
    MY_MACRO(UInt16, bt_uint16);                                                                   \
    MY_MACRO(Int16, bt_int16);                                                                     \
    MY_MACRO(UInt32, bt_uint32);                                                                   \
    MY_MACRO(Int32, bt_int32);                                                                     \
    MY_MACRO(UInt64, bt_uint64);                                                                   \
    MY_MACRO(Int64, bt_int64);                                                                     \
    MY_MACRO(Float, bt_float);                                                                     \
    MY_MACRO(Double, bt_double);

bool InputTableTvfFunc::fillInputTable(vector<vector<vector<string>>> &input, TablePtr &output) {
    size_t columnCount = output->getColumnCount();
    if (columnCount != input[0].size()) {
        SQL_LOG(ERROR,
                "fillInputTable error: Column number not equal [%lu][%lu]",
                columnCount,
                input[0].size());
        return false;
    }

    output->batchAllocateRow(input.size());

    for (size_t colIndex = 0; colIndex < input[0].size(); colIndex++) {
        auto outputColumn = output->getColumn(colIndex);
        ValueType columnValueType = outputColumn->getType();
        BuiltinType columnType = columnValueType.getBuiltinType();
        bool isMultiValue = columnValueType.isMultiValue();
        if (!isMultiValue) {
            switch (columnType) {
                NUMERIC_SWITCH_CASE_HELPER(NUMERIC_SWITCH_CASE);
            default:
                ColumnData<MultiChar> *columnData = outputColumn->getColumnData<MultiChar>();
                if (!columnData) {
                    break;
                }
                for (size_t rowIndex = 0; rowIndex < input.size(); rowIndex++) {
                    const auto &value = input[rowIndex][colIndex];
                    columnData->set(rowIndex, value[0].data(), value[0].size());
                }
            }
        } else {
            switch (columnType) {
                NUMERIC_SWITCH_CASE_HELPER(MULTINUMERIC_SWITCH_CASE);
            default:
                ColumnData<MultiString> *columnData = outputColumn->getColumnData<MultiString>();
                if (!columnData) {
                    break;
                }
                for (size_t rowIndex = 0; rowIndex < input.size(); rowIndex++) {
                    const auto &value = input[rowIndex][colIndex];
                    columnData->set(rowIndex, value.data(), value.size());
                }
            }
        }
    }
    return true;
}

#undef NUMERIC_SWITCH_CASE_HELPER
#undef NUMERIC_SWITCH_CASE
#undef MULTINUMERIC_SWITCH_CASE

InputTableTvfFuncCreator::InputTableTvfFuncCreator()
    : TvfFuncCreatorR(inputTableTvfDef, inputTableTvfInfo) {}

InputTableTvfFuncCreator::~InputTableTvfFuncCreator() {}

TvfFunc *InputTableTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new InputTableTvfFunc();
}

REGISTER_RESOURCE(InputTableTvfFuncCreator);

inputTableWithSepTvfFuncCreator::inputTableWithSepTvfFuncCreator()
    : TvfFuncCreatorR(inputTableWithSepTvfDef, inputTableWithSepTvfInfo) {}

inputTableWithSepTvfFuncCreator::~inputTableWithSepTvfFuncCreator() {}

TvfFunc *inputTableWithSepTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new InputTableTvfFunc();
}

REGISTER_RESOURCE(inputTableWithSepTvfFuncCreator);

} // namespace sql
