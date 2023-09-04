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
#include "sql/ops/agg/builtin/LogicAggFunc.h"

#include <iosfwd>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

#include "autil/legacy/json.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace table;
using namespace matchdoc;

namespace sql {

navi::ErrorCode LogicAndAggFuncCreator::init(navi::ResourceInitContext &ctx) {
#define SUPPORTED_INPUT_TYPES "[int8]|[int16]|[int32]|[int64]"
    autil::legacy::json::JsonMap properties;
    REGISTER_NAVI_AGG_FUNC_INFO(
        SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, properties);
    return navi::EC_NONE;
#undef SUPPORTED_INPUT_TYPES
}

AggFunc *LogicAndAggFuncCreator::createLocalFunction(const vector<ValueType> &inputTypes,
                                                     const vector<string> &inputFields,
                                                     const vector<string> &outputFields) {
    if (inputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }
    if (inputTypes[0].isMultiValue()) {
        SQL_LOG(ERROR, "logic-and func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_LOGIC_AGG_FUN_HELPER(ft)                                                            \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                 \
        auto callback = [](T &a, T b) { a &= b; };                                                 \
        return new LogicAggFunc<T>(inputFields,                                                    \
                                   outputFields,                                                   \
                                   AggFuncMode::AGG_FUNC_MODE_LOCAL,                               \
                                   move(callback),                                                 \
                                   numeric_limits<T>::max());                                      \
    }
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint8);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint16);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint32);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint64);

#undef CREATE_LOGIC_AGG_FUN_HELPER
    default: {
        SQL_LOG(ERROR,
                "logic-and func input type only support unsigned number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AggFunc *LogicOrAggFuncCreator::createLocalFunction(const vector<ValueType> &inputTypes,
                                                    const vector<string> &inputFields,
                                                    const vector<string> &outputFields) {
    if (inputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }
    if (inputTypes[0].isMultiValue()) {
        SQL_LOG(ERROR, "logic-or func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_LOGIC_AGG_FUN_HELPER(ft)                                                            \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                 \
        auto callback = [](T &a, T b) { a |= b; };                                                 \
        return new LogicAggFunc<T>(                                                                \
            inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_LOCAL, move(callback));          \
    }
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint8);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint16);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint32);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint64);

#undef CREATE_LOGIC_AGG_FUN_HELPER
    default: {
        SQL_LOG(ERROR,
                "logic-or func input type only support unsigned number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

REGISTER_RESOURCE(LogicAndAggFuncCreator);
REGISTER_RESOURCE(LogicOrAggFuncCreator);

} // namespace sql
