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
#include "sql/ops/agg/builtin/MaxLabelAggFunc.h"

#include <algorithm>
#include <iosfwd>
#include <map>

#include "autil/legacy/json.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "table/TableUtil.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace table;
using namespace matchdoc;

namespace sql {

navi::ErrorCode MaxLabelAggFuncCreator::init(navi::ResourceInitContext &ctx) {
    constexpr char accTypes[] = "[Mint8,Mint8]|[Mint8,Mint16]|[Mint8,Mint32]|[Mint8,Mint64]|[Mint8,"
                                "Mfloat]|[Mint8,Mdouble]|[Mint8,int8]|["
                                "Mint8,int16]|[Mint8,int32]|[Mint8,int64]|[Mint8,float]|[Mint8,"
                                "double]|[Mint16,Mint8]|[Mint16,Mint16]|["
                                "Mint16,Mint32]|[Mint16,Mint64]|[Mint16,Mfloat]|[Mint16,Mdouble]|["
                                "Mint16,int8]|[Mint16,int16]|[Mint16,int32]|"
                                "[Mint16,int64]|[Mint16,float]|[Mint16,double]|[Mint32,Mint8]|["
                                "Mint32,Mint16]|[Mint32,Mint32]|[Mint32,Mint64]"
                                "|[Mint32,Mfloat]|[Mint32,Mdouble]|[Mint32,int8]|[Mint32,int16]|["
                                "Mint32,int32]|[Mint32,int64]|[Mint32,float]|"
                                "[Mint32,double]|[Mint64,Mint8]|[Mint64,Mint16]|[Mint64,Mint32]|["
                                "Mint64,Mint64]|[Mint64,Mfloat]|[Mint64,"
                                "Mdouble]|[Mint64,int8]|[Mint64,int16]|[Mint64,int32]|[Mint64,"
                                "int64]|[Mint64,float]|[Mint64,double]|[Mfloat,"
                                "Mint8]|[Mfloat,Mint16]|[Mfloat,Mint32]|[Mfloat,Mint64]|[Mfloat,"
                                "Mfloat]|[Mfloat,Mdouble]|[Mfloat,int8]|["
                                "Mfloat,int16]|[Mfloat,int32]|[Mfloat,int64]|[Mfloat,float]|["
                                "Mfloat,double]|[Mdouble,Mint8]|[Mdouble,Mint16]|"
                                "[Mdouble,Mint32]|[Mdouble,Mint64]|[Mdouble,Mfloat]|[Mdouble,"
                                "Mdouble]|[Mdouble,int8]|[Mdouble,int16]|["
                                "Mdouble,int32]|[Mdouble,int64]|[Mdouble,float]|[Mdouble,double]|["
                                "int8,Mint8]|[int8,Mint16]|[int8,Mint32]|["
                                "int8,Mint64]|[int8,Mfloat]|[int8,Mdouble]|[int8,int8]|[int8,int16]"
                                "|[int8,int32]|[int8,int64]|[int8,float]|["
                                "int8,double]|[int16,Mint8]|[int16,Mint16]|[int16,Mint32]|[int16,"
                                "Mint64]|[int16,Mfloat]|[int16,Mdouble]|["
                                "int16,int8]|[int16,int16]|[int16,int32]|[int16,int64]|[int16,"
                                "float]|[int16,double]|[int32,Mint8]|[int32,"
                                "Mint16]|[int32,Mint32]|[int32,Mint64]|[int32,Mfloat]|[int32,"
                                "Mdouble]|[int32,int8]|[int32,int16]|[int32,"
                                "int32]|[int32,int64]|[int32,float]|[int32,double]|[int64,Mint8]|["
                                "int64,Mint16]|[int64,Mint32]|[int64,Mint64]"
                                "|[int64,Mfloat]|[int64,Mdouble]|[int64,int8]|[int64,int16]|[int64,"
                                "int32]|[int64,int64]|[int64,float]|[int64,"
                                "double]|[float,Mint8]|[float,Mint16]|[float,Mint32]|[float,Mint64]"
                                "|[float,Mfloat]|[float,Mdouble]|[float,"
                                "int8]|[float,int16]|[float,int32]|[float,int64]|[float,float]|["
                                "float,double]|[double,Mint8]|[double,Mint16]|"
                                "[double,Mint32]|[double,Mint64]|[double,Mfloat]|[double,Mdouble]|["
                                "double,int8]|[double,int16]|[double,int32]"
                                "|[double,int64]|[double,float]|[double,double]|[string,Mint8]|["
                                "string,Mint16]|[string,Mint32]|[string,"
                                "Mint64]|[string,Mfloat]|[string,Mdouble]|[string,int8]|[string,"
                                "int16]|[string,int32]|[string,int64]|[string,"
                                "float]|[string,double]|[Mstring,Mint8]|[Mstring,Mint16]|[Mstring,"
                                "Mint32]|[Mstring,Mint64]|[Mstring,Mfloat]|["
                                "Mstring,Mdouble]|[Mstring,int8]|[Mstring,int16]|[Mstring,int32]|["
                                "Mstring,int64]|[Mstring,float]|[Mstring,"
                                "double]";
    constexpr char returnTypes[]
        = "[Mint8]|[Mint8]|[Mint8]|[Mint8]|[Mint8]|[Mint8]|[Mint8]|[Mint8]|[Mint8]|[Mint8]|[Mint8]|"
          "[Mint8]|[Mint16]|["
          "Mint16]|[Mint16]|[Mint16]|[Mint16]|[Mint16]|[Mint16]|[Mint16]|[Mint16]|[Mint16]|[Mint16]"
          "|[Mint16]|[Mint32]|["
          "Mint32]|[Mint32]|[Mint32]|[Mint32]|[Mint32]|[Mint32]|[Mint32]|[Mint32]|[Mint32]|[Mint32]"
          "|[Mint32]|[Mint64]|["
          "Mint64]|[Mint64]|[Mint64]|[Mint64]|[Mint64]|[Mint64]|[Mint64]|[Mint64]|[Mint64]|[Mint64]"
          "|[Mint64]|[Mfloat]|["
          "Mfloat]|[Mfloat]|[Mfloat]|[Mfloat]|[Mfloat]|[Mfloat]|[Mfloat]|[Mfloat]|[Mfloat]|[Mfloat]"
          "|[Mfloat]|[Mdouble]|"
          "[Mdouble]|[Mdouble]|[Mdouble]|[Mdouble]|[Mdouble]|[Mdouble]|[Mdouble]|[Mdouble]|["
          "Mdouble]|[Mdouble]|["
          "Mdouble]|[int8]|[int8]|[int8]|[int8]|[int8]|[int8]|[int8]|[int8]|[int8]|[int8]|[int8]|["
          "int8]|[int16]|[int16]"
          "|[int16]|[int16]|[int16]|[int16]|[int16]|[int16]|[int16]|[int16]|[int16]|[int16]|[int32]"
          "|[int32]|[int32]|["
          "int32]|[int32]|[int32]|[int32]|[int32]|[int32]|[int32]|[int32]|[int32]|[int64]|[int64]|["
          "int64]|[int64]|["
          "int64]|[int64]|[int64]|[int64]|[int64]|[int64]|[int64]|[int64]|[float]|[float]|[float]|["
          "float]|[float]|["
          "float]|[float]|[float]|[float]|[float]|[float]|[float]|[double]|[double]|[double]|["
          "double]|[double]|[double]"
          "|[double]|[double]|[double]|[double]|[double]|[double]|[string]|[string]|[string]|["
          "string]|[string]|[string]"
          "|[string]|[string]|[string]|[string]|[string]|[string]|[Mstring]|[Mstring]|[Mstring]|["
          "Mstring]|[Mstring]|["
          "Mstring]|[Mstring]|[Mstring]|[Mstring]|[Mstring]|[Mstring]|[Mstring]";
    autil::legacy::json::JsonMap properties;
    properties["return_const"] = autil::legacy::json::ParseJson(R"json({"input_idx": 0})json");
    REGISTER_NAVI_AGG_FUNC_INFO(accTypes, accTypes, returnTypes, properties);
    return navi::EC_NONE;
}

AggFunc *MaxLabelAggFuncCreator::createFirstStageFunction(const vector<ValueType> &inputTypes,
                                                          const vector<string> &inputFields,
                                                          const vector<string> &outputFields,
                                                          AggFuncMode mode) {
    if (inputFields.size() != 2) {
        SQL_LOG(ERROR, "impossible input length, expect 2, actual %lu", inputFields.size());
        return nullptr;
    }
    return listLabelType(inputTypes, inputFields, outputFields, mode);
}

AggFunc *MaxLabelAggFuncCreator::createGlobalFunction(const vector<ValueType> &inputTypes,
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
    return listLabelType(inputTypes, inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_GLOBAL);
}

AggFunc *MaxLabelAggFuncCreator::listLabelType(const vector<ValueType> &inputTypes,
                                               const vector<string> &inputFields,
                                               const vector<string> &outputFields,
                                               AggFuncMode mode) {
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_MAXLABEL_AGG_FUN_HELPER(ft)                                                         \
    case ft: {                                                                                     \
        if (!inputTypes[0].isMultiValue()) {                                                       \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                             \
            return listValueType<T>(inputTypes, inputFields, outputFields, mode);                  \
        } else {                                                                                   \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                              \
            return listValueType<T>(inputTypes, inputFields, outputFields, mode);                  \
        }                                                                                          \
    }

        BUILTIN_TYPE_MACRO_HELPER(CREATE_MAXLABEL_AGG_FUN_HELPER);
    default:
        SQL_LOG(ERROR,
                "avg func input type not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
#undef CREATE_MAXLABEL_AGG_FUN_HELPER
    }
}

template <typename T>
AggFunc *MaxLabelAggFuncCreator::listValueType(const vector<ValueType> &inputTypes,
                                               const vector<string> &inputFields,
                                               const vector<string> &outputFields,
                                               AggFuncMode mode) {
    switch (inputTypes[1].getBuiltinType()) {
#define CREATE_MAXLABEL_AGG_FUN_HELPER(ft)                                                         \
    case ft: {                                                                                     \
        if (!inputTypes[1].isMultiValue()) {                                                       \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType V;                             \
            return new MaxLabelAggFunc<T, V>(inputFields, outputFields, mode);                     \
        } else {                                                                                   \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType V;                              \
            return new MaxLabelAggFunc<T, V>(inputFields, outputFields, mode);                     \
        }                                                                                          \
    }
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CREATE_MAXLABEL_AGG_FUN_HELPER);
    default:
        SQL_LOG(ERROR,
                "avg func input type not support %s",
                TableUtil::valueTypeToString(inputTypes[1]).c_str());
        return nullptr;
#undef CREATE_MAXLABEL_AGG_FUN_HELPER
    }
    return nullptr;
}

REGISTER_RESOURCE(MaxLabelAggFuncCreator);

} // namespace sql
