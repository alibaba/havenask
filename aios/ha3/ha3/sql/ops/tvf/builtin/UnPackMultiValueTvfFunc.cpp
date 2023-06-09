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
#include "ha3/sql/ops/tvf/builtin/UnPackMultiValueTvfFunc.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>

#include "alog/Logger.h"
#include "autil/ConstString.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/DataCommon.h"
#include "table/Table.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, UnPackMultiValueTvfFunc);
AUTIL_LOG_SETUP(sql, UnPackMultiValueTvfFuncCreator);

const SqlTvfProfileInfo unpackMultiValueTvfInfo("unpackMultiValue", "unpackMultiValue");
const string unpackMultiValueTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "unpackMultiValue",
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
              "type": "string"
            }
          ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "new_fields": [
          ],
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

UnPackMultiValueTvfFunc::UnPackMultiValueTvfFunc() {}

UnPackMultiValueTvfFunc::~UnPackMultiValueTvfFunc() {}

bool UnPackMultiValueTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 1) {
        SQL_LOG(WARN, "unpack multi value tvf only support one param");
        return false;
    }
    _queryPool = context.queryPool;
    if (_queryPool == nullptr) {
        SQL_LOG(WARN, "query pool is null");
        return false;
    }
    _unpackFields = StringUtil::split(context.params[0], ",");
    return true;
}

bool UnPackMultiValueTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    if (input == nullptr) {
        return true;
    }
    size_t inputRowCount = input->getRowCount();
    if (_unpackFields.size() == 0 || inputRowCount == 0) {
        output = input;
        return true;
    }
    set<string> needUnpackSet(_unpackFields.begin(), _unpackFields.end());
    size_t columnCount = input->getColumnCount();
    size_t rawRowCount = input->getRowCount();
    size_t poolUsedSize = _queryPool->getTotalBytes();
    for (size_t i = 0; i < columnCount; i++) {
        auto column = input->getColumn(i);
        if (column == nullptr) {
            return false;
        }
        auto schema = column->getColumnSchema();
        if (schema == nullptr) {
            return false;
        }
        const string &colName = schema->getName();
        if (needUnpackSet.count(colName) != 0) {
            if (!schema->getType().isMultiValue()) {
                continue;
            }
            if (!unpackMultiValue(column, input)) {
                return false;
            }
        }
        size_t curSize = _queryPool->getTotalBytes();
        if (curSize >= MAX_SQL_POOL_SIZE) {
            SQL_LOG(WARN, "query use size [%ld] larger than [%ld]", curSize, MAX_SQL_POOL_SIZE);
            return false;
        }
    }
    output = input;
    if (output) {
        SQL_LOG(DEBUG,
                "before unpack row count [%ld], after unpack row count [%ld], pool used [%ld]",
                rawRowCount,
                output->getRowCount(),
                _queryPool->getTotalBytes() - poolUsedSize);
    }
    return true;
}

bool UnPackMultiValueTvfFunc::unpackMultiValue(Column *column, TablePtr table) {
    vector<pair<int32_t, int32_t>> unpackOffsetVec;
    if (!genColumnUnpackOffsetVec(column, unpackOffsetVec)) {
        return false;
    }
    if (unpackOffsetVec.size() < table->getRowCount()) {
        return false;
    }
    if (unpackOffsetVec.size() == table->getRowCount()) {
        return true;
    }
    if (!unpackTable(unpackOffsetVec, column->getColumnSchema()->getName(), table)) {
        return false;
    }
    return true;
}

bool UnPackMultiValueTvfFunc::unpackTable(const vector<pair<int32_t, int32_t>> &unpackVec,
                                          const string &unpackColName,
                                          TablePtr &unpackTable) {
    size_t columnCount = unpackTable->getColumnCount();
    size_t rawRowCount = unpackTable->getRowCount();
    if (unpackVec.size() < rawRowCount) {
        return false;
    }
    unpackTable->batchAllocateRow(unpackVec.size() - rawRowCount);
    vector<Row> allRow = unpackTable->getRows();
    for (size_t i = 0; i < columnCount; i++) {
        auto column = unpackTable->getColumn(i);
        if (column == nullptr) {
            return false;
        }
        auto schema = column->getColumnSchema();
        if (schema == nullptr) {
            return false;
        }
        string colName = schema->getName();
        if (colName != unpackColName) {
            if (!copyNormalCol(rawRowCount, allRow, unpackVec, column)) {
                SQL_LOG(WARN, "copy column [%s] fail.", colName.c_str());
                return false;
            }
        } else {
            if (!copyUnpackCol(rawRowCount, allRow, unpackVec, column)) {
                SQL_LOG(WARN, "copy column [%s] fail.", colName.c_str());
                return false;
            }
        }
    }
    return true;
}

bool UnPackMultiValueTvfFunc::copyNormalCol(size_t rawRowCount,
                                            const vector<Row> &allRow,
                                            const vector<pair<int32_t, int32_t>> &unpackVec,
                                            Column *column) {
    assert(allRow.size() == unpackVec.size());
    auto schema = column->getColumnSchema();
    auto vt = schema->getType();
    bool isMulti = vt.isMultiValue();
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        if (isMulti) {                                                                             \
            typedef matchdoc::MatchDocBuiltinType2CppType<ft, true>::CppType T;                    \
            auto columnData = column->getColumnData<T>();                                          \
            for (size_t i = rawRowCount; i < allRow.size(); i++) {                                 \
                columnData->set(allRow[i], columnData->get(allRow[unpackVec[i].first]));           \
            }                                                                                      \
        } else {                                                                                   \
            typedef matchdoc::MatchDocBuiltinType2CppType<ft, false>::CppType T;                   \
            auto columnData = column->getColumnData<T>();                                          \
            for (size_t i = rawRowCount; i < allRow.size(); i++) {                                 \
                columnData->set(allRow[i], columnData->get(allRow[unpackVec[i].first]));           \
            }                                                                                      \
        }                                                                                          \
        break;                                                                                     \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        SQL_LOG(ERROR, "impossible reach this branch");
        return false;
    }
    } // switch
    return true;
}

bool UnPackMultiValueTvfFunc::copyUnpackCol(size_t rawRowCount,
                                            const vector<Row> &allRow,
                                            const vector<pair<int32_t, int32_t>> &unpackVec,
                                            Column *column) {
    assert(allRow.size() == unpackVec.size());
    auto schema = column->getColumnSchema();
    string colName = schema->getName();
    auto vt = schema->getType();
    if (!vt.isMultiValue()) {
        return false;
    }
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        typedef matchdoc::MatchDocBuiltinType2CppType<ft, true>::CppType T;                        \
        auto columnData = column->getColumnData<T>();                                              \
        if (columnData == nullptr) {                                                               \
            SQL_LOG(ERROR, "get column data [%s] failed", colName.c_str());                        \
            return false;                                                                          \
        }                                                                                          \
        typedef BuiltinType2CppType<ft>::CppType VT;                                               \
        vector<VT> valVec;                                                                         \
        unordered_map<VT, char *> mvValCache;                                                      \
        for (int32_t i = allRow.size() - 1; i >= 0; i--) {                                         \
            const auto &mv = columnData->get(allRow[unpackVec[i].first]);                          \
            int32_t mvSize = mv.size();                                                            \
            if (mvSize == 0) {                                                                     \
                columnData->set(allRow[i], mv);                                                    \
            } else {                                                                               \
                if (unpackVec[i].second >= mvSize) {                                               \
                    SQL_LOG(ERROR,                                                                 \
                            "multi value offset [%d] larger than expect [%d]",                     \
                            unpackVec[i].second,                                                   \
                            mvSize);                                                               \
                    return false;                                                                  \
                }                                                                                  \
                VT val = mv[unpackVec[i].second];                                                  \
                auto iter = mvValCache.find(val);                                                  \
                if (iter != mvValCache.end()) {                                                    \
                    columnData->getPointer(allRow[i])->init(iter->second);                         \
                } else {                                                                           \
                    valVec.clear();                                                                \
                    valVec.push_back(val);                                                         \
                    auto p = autil::MultiValueCreator::createMultiValueBuffer(valVec, _queryPool); \
                    columnData->getPointer(allRow[i])->init(p);                                    \
                    mvValCache[val] = p;                                                           \
                }                                                                                  \
            }                                                                                      \
        }                                                                                          \
        SQL_LOG(                                                                                   \
            INFO, "column [%s] unpack uniq val count [%ld].", colName.c_str(), mvValCache.size()); \
        break;                                                                                     \
    }
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    case bt_string: {
        auto columnData = column->getColumnData<autil::MultiValueType<autil::MultiChar>>();
        if (columnData == nullptr) {
            SQL_LOG(ERROR, "get column data [%s] failed", colName.c_str());
            return false;
        }
        vector<StringView> valVec;
        map<StringView, char *> mvValCache;
        for (int32_t i = allRow.size() - 1; i >= 0; i--) {
            const auto &mv = columnData->get(allRow[unpackVec[i].first]);
            int32_t mvSize = mv.size();
            if (mvSize == 0) {
                columnData->set(allRow[i], mv);
            } else {
                if (unpackVec[i].second >= mvSize) {
                    SQL_LOG(ERROR,
                            "multi value offset [%d] larger than expect [%d]",
                            unpackVec[i].second,
                            mvSize);
                    return false;
                }
                const auto &val = mv[unpackVec[i].second];
                StringView constVal(val.data(), val.size());
                auto iter = mvValCache.find(constVal);
                if (iter != mvValCache.end()) {
                    columnData->getPointer(allRow[i])->init(iter->second);
                } else {
                    valVec.clear();
                    valVec.emplace_back(constVal);
                    auto p = autil::MultiValueCreator::createMultiValueBuffer(valVec, _queryPool);
                    mvValCache[constVal] = p;
                    columnData->getPointer(allRow[i])->init(p);
                }
            }
        }
        SQL_LOG(
            INFO, "column [%s] unpack uniq val count [%ld].", colName.c_str(), mvValCache.size());
        break;
    }
    default: {
        SQL_LOG(ERROR, "impossible reach this branch");
        return false;
    }
    } // switch
    return true;
}

bool UnPackMultiValueTvfFunc::genColumnUnpackOffsetVec(Column *column,
                                                       vector<pair<int32_t, int32_t>> &unpackVec) {
    auto schema = column->getColumnSchema();
    auto vt = schema->getType();
    if (!vt.isMultiValue()) {
        return false;
    }
    size_t rowCount = column->getRowCount();
    unpackVec.reserve(rowCount * 4);
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        typedef matchdoc::MatchDocBuiltinType2CppType<ft, true>::CppType T;                        \
        auto columnData = column->getColumnData<T>();                                              \
        size_t dataSize = 0;                                                                       \
        for (size_t i = 0; i < rowCount; i++) {                                                    \
            const auto &datas = columnData->get(i);                                                \
            dataSize = datas.size() != 0 ? datas.size() : 1;                                       \
            for (size_t k = 0; k < dataSize; k++) {                                                \
                unpackVec.emplace_back(i, k);                                                      \
            }                                                                                      \
        }                                                                                          \
        break;                                                                                     \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        SQL_LOG(ERROR, "impossible reach this branch");
        return false;
    }
    } // switch
    size_t i = 0;
    for (size_t j = 0; j < unpackVec.size(); j++) {
        if (unpackVec[j].second == 0) {
            swap(unpackVec[i], unpackVec[j]);
            i++;
        }
    }
    return true;
}

UnPackMultiValueTvfFuncCreator::UnPackMultiValueTvfFuncCreator()
    : TvfFuncCreatorR(unpackMultiValueTvfDef, unpackMultiValueTvfInfo) {}

UnPackMultiValueTvfFuncCreator::~UnPackMultiValueTvfFuncCreator() {}

TvfFunc *UnPackMultiValueTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new UnPackMultiValueTvfFunc();
}

REGISTER_RESOURCE(UnPackMultiValueTvfFuncCreator);

} // namespace sql
} // namespace isearch
