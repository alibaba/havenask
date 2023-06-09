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
#include "ha3/sql/ops/agg/builtin/SumAggFunc.h"

#include <algorithm>
#include <iosfwd>
#include <memory>
#include <stdint.h>

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
AUTIL_LOG_SETUP(sql, SumAggFuncCreator);
AggFunc *SumAggFuncCreator::createLocalFunction(const vector<ValueType> &inputTypes,
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
        SQL_LOG(ERROR, "sum func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_SUM_AGG_FUN_HELPER(ft, V)                                                           \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                 \
        return new SumAggFunc<T, V>(inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_LOCAL);  \
    }
        CREATE_SUM_AGG_FUN_HELPER(bt_int8, int64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_int16, int64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_int32, int64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_int64, int64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_uint8, uint64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_uint16, uint64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_uint32, uint64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_uint64, uint64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_float, float);
        CREATE_SUM_AGG_FUN_HELPER(bt_double, double);
#undef CREATE_SUM_AGG_FUN_HELPER
    default: {
        SQL_LOG(ERROR,
                "sum func input type only support number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

REGISTER_RESOURCE(SumAggFuncCreator);

} // namespace sql
} // namespace isearch
