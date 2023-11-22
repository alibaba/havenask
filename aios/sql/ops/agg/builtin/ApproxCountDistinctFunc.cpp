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
#include "sql/ops/agg/builtin/ApproxCountDistinctFunc.h"

#include "sql/common/Log.h"

using namespace std;
using namespace table;
using namespace matchdoc;

namespace sql {

navi::ErrorCode ApproxCountDistinctFuncCreator::init(navi::ResourceInitContext &ctx) {
#define SUPPORTED_INPUT_TYPES "[int8]|[int16]|[int32]|[int64]|[float]|[double]|[string]"

#define SUPPORTED_OUTPUT_TYPES "[int64]|[int64]|[int64]|[int64]|[int64]|[int64]|[int64]"
    autil::legacy::json::JsonMap properties;
    REGISTER_NAVI_AGG_FUNC_INFO(
        SUPPORTED_OUTPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_OUTPUT_TYPES, properties);
    return navi::EC_NONE;
#undef SUPPORTED_INPUT_TYPES
#undef SUPPORTED_OUTPUT_TYPES
}

AggFunc *
ApproxCountDistinctFuncCreator::createFirstStageFunction(const vector<ValueType> &inputTypes,
                                                         const vector<string> &inputFields,
                                                         const vector<string> &outputFields,
                                                         AggFuncMode mode) {
    if (inputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (inputTypes[0].isMultiValue()) {
        SQL_LOG(ERROR, "approx count distinct func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_FUN_HELPER(ft)                                                                      \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                 \
        return new ApproxCountDistinctFunc<T>(inputFields, outputFields, mode);                    \
    }
        BUILTIN_TYPE_MACRO_HELPER(CREATE_FUN_HELPER);
        CREATE_FUN_HELPER(bt_hllctx);
#undef CREATE_FUN_HELPER
    default: {
        SQL_LOG(ERROR, "not support type %s", TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AggFunc *
ApproxCountDistinctFuncCreator::createGlobalFunction(const std::vector<ValueType> &inputTypes,
                                                     const std::vector<std::string> &inputFields,
                                                     const std::vector<std::string> &outputFields) {
    if (inputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }

    if (inputTypes[0].getBuiltinType() != bt_hllctx) {
        SQL_LOG(ERROR, "global func only support hllctx type");
        return nullptr;
    }
    return new ApproxCountDistinctFunc<autil::HllCtx>(
        inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_GLOBAL);
}

REGISTER_RESOURCE(ApproxCountDistinctFuncCreator);

} // namespace sql
