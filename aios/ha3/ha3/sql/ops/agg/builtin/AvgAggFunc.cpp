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
#include "ha3/sql/ops/agg/builtin/AvgAggFunc.h"

#include <algorithm>
#include <iosfwd>

#include "alog/Logger.h"
#include "ha3/sql/common/Log.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/flatbuffer/MatchDoc_generated.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "table/TableUtil.h"

using namespace std;
using namespace table;
using namespace matchdoc;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, AvgAggFuncCreator);
AggFunc *AvgAggFuncCreator::createFirstStageFunction(const vector<ValueType> &inputTypes,
                                                     const vector<string> &inputFields,
                                                     const vector<string> &outputFields,
                                                     AggFuncMode mode) {
    if (inputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (inputTypes[0].isMultiValue()) {
        SQL_LOG(ERROR, "avg func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_AVG_AGG_FUN_HELPER(ft, V)                                                           \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                 \
        return new AvgAggFunc<T, V>(inputFields, outputFields, mode);                              \
    }
        CREATE_AVG_AGG_FUN_HELPER(bt_int8, int64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_int16, int64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_int32, int64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_int64, int64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_uint8, uint64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_uint16, uint64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_uint32, uint64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_uint64, uint64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_float, float);
        CREATE_AVG_AGG_FUN_HELPER(bt_double, double);
#undef CREATE_AVG_AGG_FUN_HELPER
    default: {
        SQL_LOG(ERROR,
                "avg func input type only support number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AggFunc *AvgAggFuncCreator::createGlobalFunction(const vector<ValueType> &inputTypes,
                                                 const vector<string> &inputFields,
                                                 const vector<string> &outputFields) {
    if (inputFields.size() != 2) {
        SQL_LOG(ERROR, "impossible input length, expect 2, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }
    if (inputTypes[0].getTypeIgnoreConstruct()
        != ValueTypeHelper<uint64_t>::getValueType().getTypeIgnoreConstruct()) {
        SQL_LOG(ERROR,
                "impossible count input type, expect uint64, actual %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    if (inputTypes[1].isMultiValue()) {
        SQL_LOG(ERROR, "avg func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[1].getBuiltinType()) {
#define CREATE_AVG_AGG_FUN_HELPER(ft)                                                              \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                 \
        return new AvgAggFunc<int, T>(                                                             \
            inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_GLOBAL);                         \
    }
        CREATE_AVG_AGG_FUN_HELPER(bt_int64);
        CREATE_AVG_AGG_FUN_HELPER(bt_uint64);
        CREATE_AVG_AGG_FUN_HELPER(bt_float);
        CREATE_AVG_AGG_FUN_HELPER(bt_double);
#undef CREATE_AVG_AGG_FUN_HELPER
    default: {
        SQL_LOG(ERROR,
                "avg func input type only support number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[1]).c_str());
        return nullptr;
    }
    }
}

REGISTER_RESOURCE(AvgAggFuncCreator);

} // namespace sql
} // namespace isearch
