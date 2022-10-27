#include <ha3/sql/ops/agg/builtin/GatherAggFunc.h>

using namespace std;
using namespace autil;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, GatherAggFuncCreator);
template<>
bool GatherAggFunc<autil::MultiChar>::collect(Row inputRow, Accumulator *acc) {
    GatherAccumulator<autil::MultiChar> *gatherAcc = static_cast<GatherAccumulator<autil::MultiChar> *>(acc);
    autil::MultiChar value = _inputColumn->get(inputRow);
    gatherAcc->poolVector.push_back(std::string(value.data(), value.size()));
    return true;
}

template<>
bool GatherAggFunc<autil::MultiChar>::merge(Row inputRow, Accumulator *acc) {
    GatherAccumulator<autil::MultiChar> *gatherAcc = static_cast<GatherAccumulator<autil::MultiChar> *>(acc);
    autil::MultiValueType<autil::MultiChar> values =  _gatheredColumn->get(inputRow);
    for (size_t i = 0; i < values.size(); i ++) {
        autil::MultiChar value = values[i];
        gatherAcc->poolVector.push_back(std::string(value.data(), value.size()));
    }
    return true;
}


AggFunc *GatherAggFuncCreator::createLocalFunction(
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
        SQL_LOG(WARN, "local gather func input type not support multi value type");
        return nullptr;
    }

    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_GATHER_AGG_FUN_HELPER(ft)                                   \
        case ft: {                                                      \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;  \
            return new GatherAggFunc<T>(inputFields, outputFields, true); \
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
        SQL_LOG(WARN, "avg func input type only support number and string type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

AggFunc *GatherAggFuncCreator::createGlobalFunction(
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

    if (!inputTypes[0].isMultiValue()) {
        SQL_LOG(WARN, "global gather func input type not support single value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_GATHER_AGG_FUN_HELPER(ft)                                \
        case ft: {                                                      \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;  \
            return new GatherAggFunc<T>(inputFields, outputFields, false); \
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
        SQL_LOG(WARN, "gather func input type only support number and string type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}



END_HA3_NAMESPACE(sql);
