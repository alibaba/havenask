#include <ha3/sql/ops/khronosScan/KhronosUtil.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <autil/StringUtil.h>
#include <regex>

using namespace std;
using namespace autil;
using namespace autil_rapidjson;
using namespace khronos::search;

BEGIN_HA3_NAMESPACE(sql);

bool KhronosUtil::updateTimeRange(const std::string &op, int64_t value,
                                  khronos::search::TimeRange &tsRange)
{
    if (value <= 0) {
        // for `now - 5h` `now - 5s`
        value += TimeUtility::currentTimeInSeconds();
    }
    if (op == SQL_GE_OP) {
        tsRange = tsRange.GetOverLappedRange(
                TimeRange(value, TimeRange::maxTime));
    } else if (op == SQL_GT_OP) {
        tsRange = tsRange.GetOverLappedRange(
                TimeRange(value + 1, TimeRange::maxTime));
    } else if (op == SQL_LE_OP) {
        tsRange = tsRange.GetOverLappedRange(
                TimeRange(TimeRange::minTime, value + 1));
    } else if (op == SQL_LT_OP) {
        tsRange = tsRange.GetOverLappedRange(
                TimeRange(TimeRange::minTime, value));
    } else {
        return false;
    }
    return true;
}

std::string KhronosUtil::timestampUdf2Op(const std::string &funcName) {
    if (funcName == KHRONOS_UDF_TIMESTAMP_LARGER_THAN) {
        return SQL_GT_OP;
    } else if (funcName == KHRONOS_UDF_TIMESTAMP_LARGER_EQUAL_THAN) {
        return SQL_GE_OP;
    } else if (funcName == KHRONOS_UDF_TIMESTAMP_LESS_THAN) {
        return SQL_LT_OP;
    } else if (funcName == KHRONOS_UDF_TIMESTAMP_LESS_EQUAL_THAN) {
        return SQL_LE_OP;
    }
    return SQL_UNKNOWN_OP;
}

string KhronosUtil::makeWildcardToRegex(const string &wildcard)
{
    //static regex specialChars { R"([-[\]{}()+?.,\^$|#\s])" };
    static regex specialChars { R"([\-\[\]\{\}\(\)\+\?\.\,\^\$\|\#\\])" };
    string sanitized = std::regex_replace(wildcard, specialChars, R"(\$&)" );
    StringUtil::replaceAll(sanitized, "*", ".*");
    return string("^") + sanitized + "$";
}

bool KhronosUtil::typeMatch(const IndexInfoMapType &indexInfos,
                            const std::string &columnName,
                            const std::string &indexType)
{
    auto iter = indexInfos.find(columnName);
    if (iter == indexInfos.end()) {
        return false;
    }
    return iter->second.type == indexType;
}

END_HA3_NAMESPACE(sql);
