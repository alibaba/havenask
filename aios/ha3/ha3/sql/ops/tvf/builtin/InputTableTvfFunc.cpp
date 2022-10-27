#include "ha3/sql/ops/tvf/builtin/InputTableTvfFunc.h"
#include <autil/legacy/RapidJsonCommon.h>
#include <ha3/sql/data/TableUtil.h>
#include <autil/MultiValueCreator.h>
using namespace std;
using namespace autil;
using namespace matchdoc;

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(sql);

const char InputTableTvfFunc::ROW_SEPERATOR = ';';
const char InputTableTvfFunc::COL_SEPERATOR = ',';
const char InputTableTvfFunc::MULTIVALUE_SEPERATOR = ':';

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

InputTableTvfFunc::InputTableTvfFunc() {
}

InputTableTvfFunc::~InputTableTvfFunc() {
}

bool InputTableTvfFunc::init(TvfFuncInitContext &context) {
    _queryPool = context.queryPool;
    if (context.params.size() != 1) {
        SQL_LOG(ERROR, "param size [%lu] not equal 1", context.params.size());
        return false;
    }

    std::string inputTableInfoStr = context.params[0];
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
        vector<vector<vector<string>>>& output)
{
    if (input.empty()) {
        output = {{{string()}}};
        return true;
    }
    auto data = input.data();
    auto size = input.size();
    if (*input.rbegin() == ROW_SEPERATOR) {
        --size;
    }
    size_t columnCount = 0;
    ConstString constInput(data, size);
    vector<ConstString> rows = StringTokenizer::constTokenize(
        constInput, ROW_SEPERATOR, StringTokenizer::TOKEN_LEAVE_AS);
    output.reserve(rows.size());
    for (const auto& row : rows) {
        vector<ConstString> cols = StringTokenizer::constTokenize(
            row, COL_SEPERATOR, StringTokenizer::TOKEN_LEAVE_AS);
        if (columnCount == 0) {
            columnCount = cols.size();
        }
        else if (columnCount != cols.size()) {
            SQL_LOG(WARN, "Parse input table error: Column number not equal [%lu][%lu]",
                columnCount, row.size());
            return false;
        }
        vector<vector<string>> outputRow;
        outputRow.reserve(cols.size());
        for (const auto& col : cols) {
            vector<ConstString> values = StringTokenizer::constTokenize(
                col, MULTIVALUE_SEPERATOR, StringTokenizer::TOKEN_LEAVE_AS);
            vector<string> outputCol;
            outputCol.reserve(values.size());
            for (const auto& value : values) {
                outputCol.emplace_back(value.data(), value.size());
            }
            outputRow.push_back(move(outputCol));
        }
        output.push_back(move(outputRow));
    }
    return true;
}

#define NUMERIC_SWITCH_CASE(fbType, fieldType)                          \
    case fieldType:                                                     \
    {                                                                   \
        typedef MatchDocBuiltinType2CppType<fieldType, false>::CppType cppType; \
        ColumnData<cppType> *columnData = output->getColumn(colIndex)-> \
                                          getColumnData<cppType>();     \
        if (!columnData) {                                              \
            break;                                                      \
        }                                                               \
        for (size_t rowIndex = 0; rowIndex < input.size(); rowIndex++) { \
            cppType colValue{0};                                        \
            vector<string> &value = input[rowIndex][colIndex];          \
            StringUtil::strTo##fbType(value[0].c_str(), colValue);      \
            columnData->set(rowIndex, colValue);                        \
        }                                                               \
        break;                                                          \
    }

#define MULTINUMERIC_SWITCH_CASE(fbType, fieldType)                     \
    case fieldType:                                                     \
    {                                                                   \
        typedef MatchDocBuiltinType2CppType<fieldType, false>::CppType cppType; \
        auto *columnData = output->getColumn(colIndex)->                \
                           getColumnData<MultiValueType<cppType>>();    \
        if (!columnData) {                                              \
            break;                                                      \
        }                                                               \
        for (size_t rowIndex = 0; rowIndex < input.size(); rowIndex++) { \
            vector<string> &value = input[rowIndex][colIndex];          \
            vector<cppType> colValues;                                  \
            for (auto strValue : value) {                               \
                cppType tempValue{0};                                   \
                StringUtil::strTo##fbType(strValue.c_str(), tempValue); \
                colValues.push_back(tempValue);                         \
            }                                                           \
            char *multiValueBuffer = MultiValueCreator::createMultiValueBuffer( \
                    colValues, _queryPool);                             \
            MultiValueType<cppType> colValue(multiValueBuffer);         \
            columnData->set(rowIndex, colValue);                        \
        }                                                               \
        break;                                                          \
    }

#define NUMERIC_SWITCH_CASE_HELPER(MY_MACRO)    \
    MY_MACRO(UInt8, bt_uint8);                  \
    MY_MACRO(Int8, bt_int8);                    \
    MY_MACRO(UInt16, bt_uint16);                \
    MY_MACRO(Int16, bt_int16);                  \
    MY_MACRO(UInt32, bt_uint32);                \
    MY_MACRO(Int32, bt_int32);                  \
    MY_MACRO(UInt64, bt_uint64);                \
    MY_MACRO(Int64, bt_int64);                  \
    MY_MACRO(Float, bt_float);                  \
    MY_MACRO(Double, bt_double);

bool InputTableTvfFunc::fillInputTable(vector<vector<vector<string>>>& input,
                                       TablePtr &output)
{
    size_t columnCount = output->getColumnCount();
    if (columnCount != input[0].size()) {
        SQL_LOG(ERROR, "fillInputTable error: Column number not equal [%lu][%lu]",
                columnCount, input[0].size());
        return false;
    }

    output->batchAllocateRow(input.size());

    for (size_t colIndex = 0; colIndex < input[0].size(); colIndex++) {
        ValueType columnValueType = output->getColumnType(colIndex);
        BuiltinType columnType = columnValueType.getBuiltinType();
        bool isMultiValue = columnValueType.isMultiValue();
        if (!isMultiValue) {
            switch(columnType) {
                NUMERIC_SWITCH_CASE_HELPER(NUMERIC_SWITCH_CASE);
            default:
                ColumnData<MultiChar> *columnData =
                    output->getColumn(colIndex)->getColumnData<MultiChar>();
                if (!columnData) {
                    break;
                }
                for (size_t rowIndex = 0; rowIndex < input.size(); rowIndex++) {
                    const auto &value = input[rowIndex][colIndex];
                    char *buf = MultiValueCreator::createMultiValueBuffer(value[0].data(), value[0].size(), _queryPool);
                    MultiChar multiValue(buf);
                    columnData->set(rowIndex, multiValue);
                }
            }
        } else {
            switch(columnType) {
                NUMERIC_SWITCH_CASE_HELPER(MULTINUMERIC_SWITCH_CASE);
            default:
                ColumnData<MultiString> *columnData =
                    output->getColumn(colIndex)->getColumnData<MultiString>();
                if (!columnData) {
                    break;
                }
                for (size_t rowIndex = 0; rowIndex < input.size(); rowIndex++) {
                    const auto &value = input[rowIndex][colIndex];
                    char *multiValueBuffer = MultiValueCreator::createMultiStringBuffer(value, _queryPool);
                    MultiValueType<MultiChar> colValue(multiValueBuffer);
                    columnData->set(rowIndex, colValue);
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
    : TvfFuncCreator(inputTableTvfDef, inputTableTvfInfo)
{}

InputTableTvfFuncCreator::~InputTableTvfFuncCreator() {
}

TvfFunc *InputTableTvfFuncCreator::createFunction(
        const HA3_NS(config)::SqlTvfProfileInfo &info)
{
    return new InputTableTvfFunc();
}

END_HA3_NAMESPACE(sql);
