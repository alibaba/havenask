#include <ha3/sql/ops/agg/builtin/MaxLabelAggFunc.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, MaxLabelAggFuncCreator);
AggFunc *MaxLabelAggFuncCreator::createLocalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields)
{
    if (inputFields.size() != 2) {
        SQL_LOG(WARN, "impossible input length, expect 2, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 2) {
        SQL_LOG(WARN, "impossible output length, expect 2, actual %lu", outputFields.size());
        return nullptr;
    }
    return listLabelType(inputTypes, inputFields, outputFields, true);
}

AggFunc *MaxLabelAggFuncCreator::createGlobalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields)
{
    if (inputFields.size() != 2) {
        SQL_LOG(WARN, "impossible input length, expect 2, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(WARN, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }
    return listLabelType(inputTypes, inputFields, outputFields, false);
}

AggFunc *MaxLabelAggFuncCreator::listLabelType(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields,
        bool isLocal) {
    switch (inputTypes[0].getBuiltinType()) {
        #define CREATE_MAXLABEL_AGG_FUN_HELPER(ft) \
        case ft: {  \
            if (!inputTypes[0].isMultiValue()) { \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;  \
                return listValueType<T>(inputTypes, inputFields, outputFields, isLocal); \
            } else {    \
                typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;  \
                return listValueType<T>(inputTypes, inputFields, outputFields, isLocal); \
            }   \
        }
        
        BUILTIN_TYPE_MACRO_HELPER(CREATE_MAXLABEL_AGG_FUN_HELPER);        
        default:
            SQL_LOG(WARN, "avg func input type not support %s",   \
                    TableUtil::valueTypeToString(inputTypes[0]).c_str());   \
            return nullptr;
        #undef CREATE_MAXLABEL_AGG_FUN_HELPER
    }
}

template <typename T>
AggFunc *MaxLabelAggFuncCreator::listValueType(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields,
        bool isLocal) {    
    switch (inputTypes[1].getBuiltinType()) {
        #define CREATE_MAXLABEL_AGG_FUN_HELPER(ft) \
        case ft: {  \
            if (!inputTypes[1].isMultiValue()) { \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType V;  \
                return new MaxLabelAggFunc<T, V>(inputFields, outputFields, isLocal); \
            } else {    \
                typedef MatchDocBuiltinType2CppType<ft, true>::CppType V;  \
                return new MaxLabelAggFunc<T, V>(inputFields, outputFields, isLocal); \
            }   \
        }
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CREATE_MAXLABEL_AGG_FUN_HELPER);        
        default:
            SQL_LOG(WARN, "avg func input type not support %s",   \
                    TableUtil::valueTypeToString(inputTypes[1]).c_str());   \
            return nullptr;
        #undef CREATE_MAXLABEL_AGG_FUN_HELPER
    }
    return nullptr;
}

END_HA3_NAMESPACE(sql);
