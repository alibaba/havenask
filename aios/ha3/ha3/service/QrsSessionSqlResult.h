#ifndef ISEARCH_QRSSESSIONSQLRESULT_H
#define ISEARCH_QRSSESSIONSQLRESULT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <multi_call/common/ErrorCode.h>
#include <ha3/monitor/Ha3BizMetrics.h>
#include <ha3/sql/data/Table.h>
#include <navi/engine/NaviResult.h>
#include <iquan_common/service/ResponseWrapper.h>

BEGIN_HA3_NAMESPACE(service);

class QrsSessionSqlResult {
public:
    enum SqlResultFormatType {
        SQL_RF_JSON = 0,
        SQL_RF_STRING = 1,
        SQL_RF_FULL_JSON = 2,
        SQL_RF_TSDB = 3,
        SQL_RF_FLATBUFFERS = 4,
        SQL_RF_TSDB_PROMETHEUS = 5,
    };
public:

    QrsSessionSqlResult(SqlResultFormatType formatType_ = SQL_RF_JSON,
                        monitor::Ha3BizMetricsPtr metrics_ = monitor::Ha3BizMetricsPtr())
        : multicallEc(multi_call::MULTI_CALL_ERROR_RPC_FAILED)
        , formatType(formatType_)
        , metrics(metrics_)
        , getSearchInfo(false)
        , readable(false)
    {}
    static SqlResultFormatType strToType(const std::string &format) {
        if (format == "json") {
            return QrsSessionSqlResult::SQL_RF_JSON;
        } else if (format == "full_json"){
            return QrsSessionSqlResult::SQL_RF_FULL_JSON;
        } else if (format == "khronos"|| format == "tsdb"){
            return QrsSessionSqlResult::SQL_RF_TSDB;
        } else if (format == "flatbuffers") {
            return QrsSessionSqlResult::SQL_RF_FLATBUFFERS;
        } else if (format == "prometheus") {
            return QrsSessionSqlResult::SQL_RF_TSDB_PROMETHEUS;
        } else {
            return QrsSessionSqlResult::SQL_RF_STRING;
        }
    }

    static std::string typeToStr(SqlResultFormatType type) {
        switch (type) {
            case SQL_RF_JSON:
                return "json";
            case SQL_RF_FULL_JSON:
                return "full_json";
            case SQL_RF_STRING:
                return "string";
            case SQL_RF_TSDB:
                return "tsdb";
            case SQL_RF_FLATBUFFERS:
                return "flatbuffers";
            case SQL_RF_TSDB_PROMETHEUS:
                return "prometheus";
            default :
                return "string";
            }
    }
    const size_t getTableRowCount() const {
        return table ? table->getRowCount() : 0;
    }
public:
    multi_call::MultiCallErrorCode multicallEc;
    SqlResultFormatType formatType;
    common::ErrorResult errorResult;
    sql::TablePtr table;
    navi::NaviResultPtr naviResult;
    std::string resultStr;
    std::string formatDesc;
    monitor::Ha3BizMetricsPtr metrics;
    std::list<std::string> sqlTrace;
    std::string sqlQuery;
    std::string naviGraph;
    iquan::SqlQueryResponseWrapper iquanResponseWrapper;
    bool getSearchInfo;
    bool readable;
};

HA3_TYPEDEF_PTR(QrsSessionSqlResult);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSSESSIONSQLRESULT_H
