#pragma once

#include <ha3/common.h>
#include <ha3/sql/attr/IndexInfo.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <khronos_table_interface/TimeRange.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosUtil {
public:
    static bool updateTimeRange(const std::string &op, int64_t value,
                                khronos::search::TimeRange &tsRange);
    static std::string timestampUdf2Op(const std::string &funcName);
    static std::string makeWildcardToRegex(const std::string &wildcard);
    static bool typeMatch(const IndexInfoMapType &indexInfos,
                          const std::string &columnName,
                          const std::string &indexType);
};

END_HA3_NAMESPACE(sql);
