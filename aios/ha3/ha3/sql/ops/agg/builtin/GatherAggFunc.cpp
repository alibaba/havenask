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
#include "ha3/sql/ops/agg/builtin/GatherAggFunc.h"

#include <algorithm>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/legacy/json.h"
#include "autil/ConstString.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/sql/common/Log.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/flatbuffer/MatchDoc_generated.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "table/ColumnData.h"
#include "table/TableUtil.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, GatherAggFuncCreator);
template <>
bool GatherAggFunc<autil::MultiChar>::collect(Row inputRow, Accumulator *acc) {
    GatherAccumulator<autil::MultiChar> *gatherAcc
        = static_cast<GatherAccumulator<autil::MultiChar> *>(acc);
    const autil::MultiChar &value = _inputColumn->get(inputRow);
    gatherAcc->poolVector.push_back(StringView(value.data(), value.size()));
    return true;
}

template <>
bool GatherAggFunc<autil::MultiChar>::merge(Row inputRow, Accumulator *acc) {
    GatherAccumulator<autil::MultiChar> *gatherAcc
        = static_cast<GatherAccumulator<autil::MultiChar> *>(acc);
    const autil::MultiValueType<autil::MultiChar> &values = _gatheredColumn->get(inputRow);
    for (size_t i = 0; i < values.size(); i++) {
        const autil::MultiChar &value = values[i];
        gatherAcc->poolVector.push_back(StringView(value.data(), value.size()));
    }
    return true;
}

navi::ErrorCode GatherAggFuncCreator::init(navi::ResourceInitContext &ctx) {
#define SUPPORTED_INPUT_TYPES "[int8]|[int16]|[int32]|[int64]|[float]|[double]|[string]"

#define SUPPORTED_OUTPUT_TYPES "[Mint8]|[Mint16]|[Mint32]|[Mint64]|[Mfloat]|[Mdouble]|[Mstring]"
    autil::legacy::json::JsonMap properties;
    REGISTER_NAVI_AGG_FUNC_INFO(
            SUPPORTED_OUTPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_OUTPUT_TYPES, properties);
    return navi::EC_NONE;
#undef SUPPORTED_INPUT_TYPES
#undef SUPPORTED_OUTPUT_TYPES
}

AggFunc *GatherAggFuncCreator::createFirstStageFunction(const vector<ValueType> &inputTypes,
                                                        const vector<string> &inputFields,
                                                        const vector<string> &outputFields,
                                                        AggFuncMode mode) {
    if (inputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(ERROR, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }

    if (inputTypes[0].isMultiValue()) {
        SQL_LOG(ERROR, "local gather func input type not support multi value type");
        return nullptr;
    }

    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_GATHER_AGG_FUN_HELPER(ft)                                                           \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                 \
        return new GatherAggFunc<T>(inputFields, outputFields, mode);                              \
    }
        CREATE_GATHER_AGG_FUN_HELPER(bt_int8);
        CREATE_GATHER_AGG_FUN_HELPER(bt_int16);
        CREATE_GATHER_AGG_FUN_HELPER(bt_int32);
        CREATE_GATHER_AGG_FUN_HELPER(bt_int64);
        CREATE_GATHER_AGG_FUN_HELPER(bt_uint8);
        CREATE_GATHER_AGG_FUN_HELPER(bt_uint16);
        CREATE_GATHER_AGG_FUN_HELPER(bt_uint32);
        CREATE_GATHER_AGG_FUN_HELPER(bt_uint64);
        CREATE_GATHER_AGG_FUN_HELPER(bt_float);
        CREATE_GATHER_AGG_FUN_HELPER(bt_double);
        CREATE_GATHER_AGG_FUN_HELPER(bt_string);
#undef CREATE_GATHER_AGG_FUN_HELPER
    default: {
        SQL_LOG(ERROR,
                "avg func input type only support number and string type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AggFunc *GatherAggFuncCreator::createGlobalFunction(const vector<ValueType> &inputTypes,
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

    if (!inputTypes[0].isMultiValue()) {
        SQL_LOG(ERROR, "global gather func input type not support single value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_GATHER_AGG_FUN_HELPER(ft)                                                           \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                 \
        return new GatherAggFunc<T>(inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_GLOBAL); \
    }
        CREATE_GATHER_AGG_FUN_HELPER(bt_int8);
        CREATE_GATHER_AGG_FUN_HELPER(bt_int16);
        CREATE_GATHER_AGG_FUN_HELPER(bt_int32);
        CREATE_GATHER_AGG_FUN_HELPER(bt_int64);
        CREATE_GATHER_AGG_FUN_HELPER(bt_uint8);
        CREATE_GATHER_AGG_FUN_HELPER(bt_uint16);
        CREATE_GATHER_AGG_FUN_HELPER(bt_uint32);
        CREATE_GATHER_AGG_FUN_HELPER(bt_uint64);
        CREATE_GATHER_AGG_FUN_HELPER(bt_float);
        CREATE_GATHER_AGG_FUN_HELPER(bt_double);
        CREATE_GATHER_AGG_FUN_HELPER(bt_string);
#undef CREATE_GATHER_AGG_FUN_HELPER
    default: {
        SQL_LOG(ERROR,
                "gather func input type only support number and string type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

REGISTER_RESOURCE(GatherAggFuncCreator);

} // namespace sql
} // namespace isearch
