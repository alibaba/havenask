#include "SampleAggFunc.h"

#include <iosfwd>
#include <memory>
#include <stdint.h>

#include "autil/legacy/json.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
#include "table/TableUtil.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace table;
using namespace matchdoc;

namespace sql {

navi::ErrorCode SampleAggFuncCreator::init(navi::ResourceInitContext &ctx) {
    autil::legacy::json::JsonMap properties;
    REGISTER_NAVI_AGG_FUNC_INFO("[int64]", "[int32]", "[int64]", properties);
    return navi::EC_NONE;
}

AggFunc *SampleAggFuncCreator::createLocalFunction(const vector<ValueType> &inputTypes,
                                                   const vector<string> &inputFields,
                                                   const vector<string> &outputFields) {
    if (inputFields.size() != 1) {
        SQL_LOG(WARN, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(WARN, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }
    if (inputTypes[0].isMultiValue()) {
        SQL_LOG(WARN, "sum func input type not support multi value type");
        return nullptr;
    }
    switch (inputTypes[0].getBuiltinType()) {
#define CREATE_SUM_AGG_FUN_HELPER(ft, V)                                                           \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                 \
        return new SampleAggFunc<T, V>(                                                            \
            inputFields, outputFields, AggFuncMode::AGG_FUNC_MODE_LOCAL);                          \
    }
        CREATE_SUM_AGG_FUN_HELPER(bt_int8, int64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_int16, int64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_int32, int64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_int64, int64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_uint8, uint64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_uint16, uint64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_uint32, uint64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_uint64, uint64_t);
        CREATE_SUM_AGG_FUN_HELPER(bt_float, float);
        CREATE_SUM_AGG_FUN_HELPER(bt_double, double);
#undef CREATE_SUM_AGG_FUN_HELPER
    default: {
        SQL_LOG(WARN,
                "sum func input type only support number type, not support %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    }
}

REGISTER_RESOURCE(SampleAggFuncCreator);

} // namespace sql
