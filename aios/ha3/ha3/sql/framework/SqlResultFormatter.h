#ifndef ISEARCH_SQLRESULT_FORMATTER_H
#define ISEARCH_SQLRESULT_FORMATTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/QrsSessionSqlResult.h>
#include <ha3/common/SqlAccessLog.h>
#include <ha3/common/ErrorResult.h>
#include <ha3/sql/framework/SqlQueryResponse.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(sql);

class SqlResultFormatter
{
public:
    SqlResultFormatter() {}
    ~SqlResultFormatter() {}
public:
    static void format(HA3_NS(service)::QrsSessionSqlResult &sqlResult,
                       HA3_NS(common)::SqlAccessLog *accessLog,
                       autil::mem_pool::Pool *pool = nullptr);
    static void formatJson(HA3_NS(service)::QrsSessionSqlResult &sqlResult,
                           HA3_NS(common)::SqlAccessLog *accessLog);
    static void formatFullJson(HA3_NS(service)::QrsSessionSqlResult &sqlResult,
                               HA3_NS(common)::SqlAccessLog *accessLog);
    static void formatString(HA3_NS(service)::QrsSessionSqlResult &sqlResult,
                             HA3_NS(common)::SqlAccessLog *accessLog);
    static void formatTsdb(HA3_NS(service)::QrsSessionSqlResult &sqlResult,
                           HA3_NS(common)::SqlAccessLog *accessLog);
    static void formatFlatbuffers(HA3_NS(service)::QrsSessionSqlResult &sqlResult,
                                  HA3_NS(common)::SqlAccessLog *accessLog,
                                  autil::mem_pool::Pool *pool);
    static void formatTsdbPrometheus(HA3_NS(service)::QrsSessionSqlResult &sqlResult,
            HA3_NS(common)::SqlAccessLog *accessLog);
    static double calcCoveredPercent(const SqlSearchInfo &searchInfo);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(sql);


#endif // ISEARCH_SQLRESULT_FORMATTER_H
