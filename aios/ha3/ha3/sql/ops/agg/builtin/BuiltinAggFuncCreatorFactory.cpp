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
#include "ha3/sql/ops/agg/builtin/BuiltinAggFuncCreatorFactory.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "ha3/sql/ops/agg/AggFuncCreatorFactory.h"
#include "ha3/sql/ops/agg/builtin/AvgAggFunc.h"
#include "ha3/sql/ops/agg/builtin/CountAggFunc.h"
#include "ha3/sql/ops/agg/builtin/GatherAggFunc.h"
#include "ha3/sql/ops/agg/builtin/IdentityAggFunc.h"
#include "ha3/sql/ops/agg/builtin/LogicAggFunc.h"
#include "ha3/sql/ops/agg/builtin/MaxAggFunc.h"
#include "ha3/sql/ops/agg/builtin/MaxLabelAggFunc.h"
#include "ha3/sql/ops/agg/builtin/MinAggFunc.h"
#include "ha3/sql/ops/agg/builtin/MultiGatherAggFunc.h"
#include "ha3/sql/ops/agg/builtin/SumAggFunc.h"

using namespace std;
namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, BuiltinAggFuncCreatorFactory);

BuiltinAggFuncCreatorFactory::BuiltinAggFuncCreatorFactory() {
}

BuiltinAggFuncCreatorFactory::~BuiltinAggFuncCreatorFactory() {}

bool BuiltinAggFuncCreatorFactory::registerAggFunctions(autil::InterfaceManager *manager) {
    REGISTER_INTERFACE("AVG", AvgAggFuncCreator, manager);
    REGISTER_INTERFACE("COUNT", CountAggFuncCreator, manager);
    REGISTER_INTERFACE("IDENTITY", IdentityAggFuncCreator, manager);
    REGISTER_INTERFACE("MAX", MaxAggFuncCreator, manager);
    REGISTER_INTERFACE("MIN", MinAggFuncCreator, manager);
    REGISTER_INTERFACE("SUM", SumAggFuncCreator, manager);
    REGISTER_INTERFACE("ARBITRARY", IdentityAggFuncCreator, manager);
    REGISTER_INTERFACE("MAXLABEL", MaxLabelAggFuncCreator, manager);
    REGISTER_INTERFACE("GATHER", GatherAggFuncCreator, manager);
    REGISTER_INTERFACE("MULTIGATHER", MultiGatherAggFuncCreator, manager);
    REGISTER_INTERFACE("LOGIC_AND", LogicAndAggFuncCreator, manager);
    REGISTER_INTERFACE("LOGIC_OR", LogicOrAggFuncCreator, manager);
    return true;
}

bool BuiltinAggFuncCreatorFactory::registerAggFunctionInfos() {
    if (!registerAccSize("AVG", 2)) {
        return false;
    }
    if (!registerAccSize("COUNT", 1)) {
        return false;
    }
    if (!registerAccSize("IDENTITY", 1)) {
        return false;
    }
    if (!registerAccSize("MAX", 1)) {
        return false;
    }
    if (!registerAccSize("MIN", 1)) {
        return false;
    }
    if (!registerAccSize("SUM", 1)) {
        return false;
    }
    if (!registerArbitraryFunctionInfos()) {
        return false;
    }
    if (!registerMaxLabelFunctionInfos()) {
        return false;
    }
    if (!registerGatherFunctionInfos()) {
        return false;
    }

    if (!registerMultiGatherFunctionInfos()) {
        return false;
    }

    if (!registerLogicAndFunctionInfos()) {
        return false;
    }

    if (!registerLogicOrFunctionInfos()) {
        return false;
    }

    return true;
}

bool BuiltinAggFuncCreatorFactory::registerArbitraryFunctionInfos() {
#define SUPPORTED_TYPES                                                                            \
    "[int8]|[int16]|[int32]|[int64]|[float]|[double]|[string]|"                                    \
    "[Mint8]|[Mint16]|[Mint32]|[Mint64]|[Mfloat]|[Mdouble]|[Mstring]"
    autil::legacy::json::JsonMap properties;
    properties["return_const"] =
        autil::legacy::json::ParseJson(R"json({"input_idx": 0})json");
    REGISTER_AGG_FUNC_INFO("ARBITRARY", SUPPORTED_TYPES, SUPPORTED_TYPES, SUPPORTED_TYPES, properties);
    return true;
#undef SUPPORTED_TYPES
}

bool BuiltinAggFuncCreatorFactory::registerMaxLabelFunctionInfos() {
    /*
    vector<string> basicNumberType = {"int8", "int16", "int32", "int64", "float", "double"};
    vector<string> multiNumberType;
    for (auto type : basicNumberType) {
        multiNumberType.emplace_back("M" + type);
    }

    vector<string> numberType;
    numberType.insert(numberType.begin(), basicNumberType.begin(), basicNumberType.end());
    numberType.insert(numberType.begin(), multiNumberType.begin(), multiNumberType.end());

    vector<string> allType;
    allType.insert(allType.begin(), numberType.begin(), numberType.end());
    allType.emplace_back("string");
    allType.emplace_back("Mstring");

    string accTypes;
    string paramTypes;
    string returnTypes;
    for (auto labelType : allType) {
        for (auto valueType : numberType) {
            accTypes += "[" + labelType + "," + valueType + "]|";
            returnTypes += "[" + labelType + "]|";
        }
    }
    accTypes.erase(accTypes.end() - 1);
    returnTypes.erase(returnTypes.end() - 1);
    paramTypes = accTypes;

    cout << "accTypes:" << accTypes << endl;
    cout << "returnTypes:" << returnTypes << endl;
    cout << "paramTypes:" << paramTypes << endl;
    */
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
    properties["return_const"] =
        autil::legacy::json::ParseJson(R"json({"input_idx": 0})json");
    REGISTER_AGG_FUNC_INFO("MAXLABEL", accTypes, accTypes, returnTypes, properties);
    return true;
}

bool BuiltinAggFuncCreatorFactory::registerGatherFunctionInfos() {
#define SUPPORTED_INPUT_TYPES "[int8]|[int16]|[int32]|[int64]|[float]|[double]|[string]"

#define SUPPORTED_OUTPUT_TYPES "[Mint8]|[Mint16]|[Mint32]|[Mint64]|[Mfloat]|[Mdouble]|[Mstring]"
    autil::legacy::json::JsonMap properties;
    REGISTER_AGG_FUNC_INFO(
            "GATHER", SUPPORTED_OUTPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_OUTPUT_TYPES, properties);
    return true;
#undef SUPPORTED_INPUT_TYPES
#undef SUPPORTED_OUTPUT_TYPES
}

bool BuiltinAggFuncCreatorFactory::registerMultiGatherFunctionInfos() {
#define SUPPORTED_INPUT_TYPES "[Mint8]|[Mint16]|[Mint32]|[Mint64]|[Mfloat]|[Mdouble]|[Mstring]"
    autil::legacy::json::JsonMap properties;
    REGISTER_AGG_FUNC_INFO(
            "MULTIGATHER", SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, properties);
    return true;
#undef SUPPORTED_INPUT_TYPES
}

bool BuiltinAggFuncCreatorFactory::registerLogicAndFunctionInfos() {
#define SUPPORTED_INPUT_TYPES "[int8]|[int16]|[int32]|[int64]"
    autil::legacy::json::JsonMap properties;
    REGISTER_AGG_FUNC_INFO(
            "LOGIC_AND", SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, properties);
    return true;
#undef SUPPORTED_INPUT_TYPES
}

bool BuiltinAggFuncCreatorFactory::registerLogicOrFunctionInfos() {
#define SUPPORTED_INPUT_TYPES "[int8]|[int16]|[int32]|[int64]"
    autil::legacy::json::JsonMap properties;
    REGISTER_AGG_FUNC_INFO(
            "LOGIC_OR", SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, properties);
    return true;
#undef SUPPORTED_INPUT_TYPES
}

} // namespace sql
} // namespace isearch
