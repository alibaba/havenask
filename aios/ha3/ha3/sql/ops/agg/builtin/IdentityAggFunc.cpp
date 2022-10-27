#include <ha3/sql/ops/agg/builtin/IdentityAggFunc.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, IdentityAggFuncCreator);
AggFunc *IdentityAggFuncCreator::createLocalFunction(
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
#define CREATE_IDENTITY_AGG_FUN_HELPER(ft)                                  \
        case ft: {                                                      \
            if (inputTypes[0].isMultiValue()) {                         \
                typedef MatchDocBuiltinType2CppType<ft, true>::CppType T; \
                return new IdentityAggFunc<T>(inputFields, outputFields, true); \
            } else {                                                    \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                return new IdentityAggFunc<T>(inputFields, outputFields, true); \
            }                                                           \
        }
        BUILTIN_TYPE_MACRO_HELPER(CREATE_IDENTITY_AGG_FUN_HELPER);
#undef CREATE_IDENTITY_AGG_FUN_HELPER
    default: {
        SQL_LOG(WARN, "identity func input type only support builtin type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AggFunc *IdentityAggFuncCreator::createGlobalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields)
{
    return createLocalFunction(inputTypes, inputFields, outputFields);
}

END_HA3_NAMESPACE(sql);
