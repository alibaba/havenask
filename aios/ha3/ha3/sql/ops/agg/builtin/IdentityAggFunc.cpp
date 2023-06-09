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
#include "ha3/sql/ops/agg/builtin/IdentityAggFunc.h"

#include <algorithm>
#include <iosfwd>
#include <memory>

#include "alog/Logger.h"
#include "autil/legacy/json.h"
#include "ha3/sql/common/Log.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/flatbuffer/MatchDoc_generated.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "table/TableUtil.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace table;
using namespace matchdoc;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, IdentityAggFuncCreator);
AggFunc *IdentityAggFuncCreator::createLocalFunction(const vector<ValueType> &inputTypes,
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
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_IDENTITY_AGG_FUN_HELPER(ft)                                                         \
    case ft: {                                                                                     \
        if (inputTypes[0].isMultiValue()) {                                                        \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                              \
            return new IdentityAggFunc<T>(                                                         \
                inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_LOCAL);                      \
        } else {                                                                                   \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                             \
            return new IdentityAggFunc<T>(                                                         \
                inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_LOCAL);                      \
        }                                                                                          \
    }
        BUILTIN_TYPE_MACRO_HELPER(CREATE_IDENTITY_AGG_FUN_HELPER);
#undef CREATE_IDENTITY_AGG_FUN_HELPER
    default: {
        SQL_LOG(ERROR,
                "identity func input type only support builtin type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AUTIL_LOG_SETUP(sql, ArbitraryAggFuncCreator);
navi::ErrorCode ArbitraryAggFuncCreator::init(navi::ResourceInitContext &ctx) {
#define SUPPORTED_TYPES                                                                            \
    "[int8]|[int16]|[int32]|[int64]|[float]|[double]|[string]|"                                    \
    "[Mint8]|[Mint16]|[Mint32]|[Mint64]|[Mfloat]|[Mdouble]|[Mstring]"
    autil::legacy::json::JsonMap properties;
    properties["return_const"] =
        autil::legacy::json::ParseJson(R"json({"input_idx": 0})json");
    REGISTER_NAVI_AGG_FUNC_INFO(SUPPORTED_TYPES, SUPPORTED_TYPES, SUPPORTED_TYPES, properties);
    return navi::EC_NONE;
#undef SUPPORTED_TYPES
}

REGISTER_RESOURCE(IdentityAggFuncCreator);
REGISTER_RESOURCE(ArbitraryAggFuncCreator);

} // namespace sql
} // namespace isearch
