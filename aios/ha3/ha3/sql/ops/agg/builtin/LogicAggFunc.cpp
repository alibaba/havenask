#include <ha3/sql/ops/agg/builtin/LogicAggFunc.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, LogicAndAggFuncCreator);
AggFunc *LogicAndAggFuncCreator::createLocalFunction(
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
    if (inputTypes[0].isMultiValue()) {
        SQL_LOG(WARN, "logic-and func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_LOGIC_AGG_FUN_HELPER(ft)                                 \
        case ft: {                                                      \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;  \
            auto callback = [](T &a, T b) {a &= b;};                    \
            return new LogicAggFunc<T>(inputFields, outputFields,       \
                    true, move(callback), numeric_limits<T>::max());    \
        }
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint8);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint16);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint32);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint64);

#undef CREATE_LOGIC_AGG_FUN_HELPER
    default: {
        SQL_LOG(WARN, "logic-and func input type only support unsigned number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AggFunc *LogicAndAggFuncCreator::createGlobalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields)
{
    return createLocalFunction(inputTypes, inputFields, outputFields);
}

HA3_LOG_SETUP(sql, LogicOrAggFuncCreator);
AggFunc *LogicOrAggFuncCreator::createLocalFunction(
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
    if (inputTypes[0].isMultiValue()) {
        SQL_LOG(WARN, "logic-or func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_LOGIC_AGG_FUN_HELPER(ft)                                 \
        case ft: {                                                      \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;  \
            auto callback = [](T &a, T b) { a |= b;};                   \
            return new LogicAggFunc<T>(inputFields, outputFields,       \
                    true, move(callback));                              \
        }
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint8);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint16);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint32);
        CREATE_LOGIC_AGG_FUN_HELPER(bt_uint64);

#undef CREATE_LOGIC_AGG_FUN_HELPER
    default: {
        SQL_LOG(WARN, "logic-or func input type only support unsigned number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AggFunc *LogicOrAggFuncCreator::createGlobalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields)
{
    return createLocalFunction(inputTypes, inputFields, outputFields);
}

END_HA3_NAMESPACE(sql);
