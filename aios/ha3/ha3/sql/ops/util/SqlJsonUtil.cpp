#include <ha3/sql/ops/util/SqlJsonUtil.h>

using namespace std;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);
bool SqlJsonUtil::isColumn(const SimpleValue& param) {
    return param.IsString() && param.GetStringLength() > 1 && param.GetString()[0] == SQL_COLUMN_PREFIX;
}

string SqlJsonUtil::getColumnName(const SimpleValue& param) {
    assert(isColumn(param));
    return string(param.GetString()).substr(1);
}

END_HA3_NAMESPACE(sql);
