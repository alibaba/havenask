#ifndef ISEARCH_SQLJSONUTIL_H
#define ISEARCH_SQLJSONUTIL_H

#include <ha3/sql/common/common.h>
#include <autil/legacy/RapidJsonCommon.h>
BEGIN_HA3_NAMESPACE(sql);

class SqlJsonUtil {
public:
    static bool isColumn(const autil_rapidjson::SimpleValue& param);
    static std::string getColumnName(const autil_rapidjson::SimpleValue& param);
};

END_HA3_NAMESPACE(sql);

#endif
