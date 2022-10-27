#include <ha3/sql/ops/agg/builtin/BuiltinAggFuncCreatorFactory.h>
#include <ha3/sql/ops/agg/builtin/AvgAggFunc.h>
#include <ha3/sql/ops/agg/builtin/CountAggFunc.h>
#include <ha3/sql/ops/agg/builtin/IdentityAggFunc.h>
#include <ha3/sql/ops/agg/builtin/MaxAggFunc.h>
#include <ha3/sql/ops/agg/builtin/MinAggFunc.h>
#include <ha3/sql/ops/agg/builtin/SumAggFunc.h>
#include <ha3/sql/ops/agg/builtin/MaxLabelAggFunc.h>
#include <ha3/sql/ops/agg/builtin/GatherAggFunc.h>
#include <ha3/sql/ops/agg/builtin/MultiGatherAggFunc.h>
#include <ha3/sql/ops/agg/builtin/LogicAggFunc.h>

using namespace std;
BEGIN_HA3_NAMESPACE(sql);

AUTIL_LOG_SETUP(sql, BuiltinAggFuncCreatorFactory);

BuiltinAggFuncCreatorFactory::BuiltinAggFuncCreatorFactory() {
}

BuiltinAggFuncCreatorFactory::~BuiltinAggFuncCreatorFactory() {
}

bool BuiltinAggFuncCreatorFactory::registerAggFunctions() {
    REGISTER_AGG_FUNC_CREATOR("AVG", AvgAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("COUNT", CountAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("IDENTITY", IdentityAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("MAX", MaxAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("MIN", MinAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("SUM", SumAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("ARBITRARY", IdentityAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("MAXLABEL", MaxLabelAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("GATHER", GatherAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("MULTIGATHER", MultiGatherAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("LOGIC_AND", LogicAndAggFuncCreator);
    REGISTER_AGG_FUNC_CREATOR("LOGIC_OR", LogicOrAggFuncCreator);
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
#define SUPPORTED_TYPES                                                 \
    "[int8]|[int16]|[int32]|[int64]|[float]|[double]|[string]|"         \
    "[Mint8]|[Mint16]|[Mint32]|[Mint64]|[Mfloat]|[Mdouble]|[Mstring]"
    REGISTER_AGG_FUNC_INFO("ARBITRARY", SUPPORTED_TYPES, SUPPORTED_TYPES, SUPPORTED_TYPES);
    return true;
#undef SUPPORTED_TYPES
}

bool BuiltinAggFuncCreatorFactory::registerMaxLabelFunctionInfos() {
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

    REGISTER_AGG_FUNC_INFO("MAXLABEL", accTypes.c_str(), paramTypes.c_str(), returnTypes.c_str());
    return true;
}

bool BuiltinAggFuncCreatorFactory::registerGatherFunctionInfos() {
#define SUPPORTED_INPUT_TYPES                                           \
    "[int8]|[int16]|[int32]|[int64]|[float]|[double]|[string]"

#define SUPPORTED_OUTPUT_TYPES                                           \
    "[Mint8]|[Mint16]|[Mint32]|[Mint64]|[Mfloat]|[Mdouble]|[Mstring]"
    REGISTER_AGG_FUNC_INFO("GATHER", SUPPORTED_OUTPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_OUTPUT_TYPES);
    return true;
#undef SUPPORTED_INPUT_TYPES
#undef SUPPORTED_OUTPUT_TYPES
}

bool BuiltinAggFuncCreatorFactory::registerMultiGatherFunctionInfos() {
#define SUPPORTED_INPUT_TYPES                                           \
    "[Mint8]|[Mint16]|[Mint32]|[Mint64]|[Mfloat]|[Mdouble]|[Mstring]"
    REGISTER_AGG_FUNC_INFO("MULTIGATHER", SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES);
    return true;
#undef SUPPORTED_INPUT_TYPES
}

bool BuiltinAggFuncCreatorFactory::registerLogicAndFunctionInfos() {
#define SUPPORTED_INPUT_TYPES                                           \
    "[int8]|[int16]|[int32]|[int64]"
    REGISTER_AGG_FUNC_INFO("LOGIC_AND", SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES);
    return true;
#undef SUPPORTED_INPUT_TYPES
}

bool BuiltinAggFuncCreatorFactory::registerLogicOrFunctionInfos() {
#define SUPPORTED_INPUT_TYPES                                           \
    "[int8]|[int16]|[int32]|[int64]"
    REGISTER_AGG_FUNC_INFO("LOGIC_OR", SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES, SUPPORTED_INPUT_TYPES);
    return true;
#undef SUPPORTED_INPUT_TYPES
}

END_HA3_NAMESPACE(sql);
