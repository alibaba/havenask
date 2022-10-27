#include <ha3/sql/ops/agg/builtin/AvgAggFunc.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, AvgAggFuncCreator);
AggFunc *AvgAggFuncCreator::createLocalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields)
{
    if (inputFields.size() != 1) {
        SQL_LOG(WARN, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 2) {
        SQL_LOG(WARN, "impossible output length, expect 2, actual %lu", outputFields.size());
        return nullptr;
    }
    if (inputTypes[0].isMultiValue()) {
        SQL_LOG(WARN, "avg func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_AVG_AGG_FUN_HELPER(ft, V)                                \
        case ft: {                                                      \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;  \
            return new AvgAggFunc<T, V>(inputFields, outputFields, true); \
        }
        CREATE_AVG_AGG_FUN_HELPER(bt_int8, int64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_int16, int64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_int32, int64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_int64, int64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_uint8, uint64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_uint16, uint64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_uint32, uint64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_uint64, uint64_t);
        CREATE_AVG_AGG_FUN_HELPER(bt_float, float);
        CREATE_AVG_AGG_FUN_HELPER(bt_double, double);
#undef CREATE_AVG_AGG_FUN_HELPER
    default: {
        SQL_LOG(WARN, "avg func input type only support number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AggFunc *AvgAggFuncCreator::createGlobalFunction(
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
    if (inputTypes[0].getTypeIgnoreConstruct()
        != ValueTypeHelper<uint64_t>::getValueType().getTypeIgnoreConstruct())
    {
        SQL_LOG(WARN, "impossible count input type, expect uint64, actual %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    if (inputTypes[1].isMultiValue()) {
        SQL_LOG(WARN, "avg func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[1].getBuiltinType()) {
#define CREATE_AVG_AGG_FUN_HELPER(ft)                                   \
        case ft: {                                                      \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;  \
            return new AvgAggFunc<int, T>(inputFields, outputFields, false);  \
        }
        CREATE_AVG_AGG_FUN_HELPER(bt_int64);
        CREATE_AVG_AGG_FUN_HELPER(bt_uint64);
        CREATE_AVG_AGG_FUN_HELPER(bt_float);
        CREATE_AVG_AGG_FUN_HELPER(bt_double);
#undef CREATE_AVG_AGG_FUN_HELPER
    default: {
        SQL_LOG(WARN, "avg func input type only support number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[1]).c_str());
        return nullptr;
    }
    }
}

END_HA3_NAMESPACE(sql);
