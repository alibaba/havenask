#include <ha3/sql/ops/agg/builtin/MaxAggFunc.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, MaxAggFuncCreator);
AggFunc *MaxAggFuncCreator::createLocalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields)
{
    if (inputFields.size() != 1) {
        SQL_LOG(WARN, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(WARN, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_MAX_AGG_FUN_HELPER(ft)                                   \
        case ft: {                                                      \
            if (inputTypes[0].isMultiValue()) {                         \
                typedef MatchDocBuiltinType2CppType<ft, true>::CppType T; \
                return new MaxAggFunc<T>(inputFields, outputFields, true); \
            } else {                                                    \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                return new MaxAggFunc<T>(inputFields, outputFields, true); \
            }                                                           \
        }
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CREATE_MAX_AGG_FUN_HELPER);
#undef CREATE_MAX_AGG_FUN_HELPER
    default: {
        SQL_LOG(WARN, "max func input type only support number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AggFunc *MaxAggFuncCreator::createGlobalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields)
{
    return createLocalFunction(inputTypes, inputFields, outputFields);
}

END_HA3_NAMESPACE(sql);
