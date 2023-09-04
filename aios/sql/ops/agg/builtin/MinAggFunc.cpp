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
#include "sql/ops/agg/builtin/MinAggFunc.h"

#include <algorithm>
#include <iosfwd>
#include <memory>

#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
#include "table/TableUtil.h"

using namespace std;
using namespace table;
using namespace matchdoc;

namespace sql {

AggFunc *MinAggFuncCreator::createLocalFunction(const vector<ValueType> &inputTypes,
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
#define CREATE_MIN_AGG_FUN_HELPER(ft)                                                              \
    case ft: {                                                                                     \
        if (inputTypes[0].isMultiValue()) {                                                        \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                              \
            return new MinAggFunc<T>(inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_LOCAL); \
        } else {                                                                                   \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                             \
            return new MinAggFunc<T>(inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_LOCAL); \
        }                                                                                          \
    }
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CREATE_MIN_AGG_FUN_HELPER);
#undef CREATE_MIN_AGG_FUN_HELPER
    default: {
        SQL_LOG(ERROR,
                "min func input type only support number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

REGISTER_RESOURCE(MinAggFuncCreator);

} // namespace sql
