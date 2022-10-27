#include <ha3/sql/framework/SqlResultFormatter.h>
#include <ha3/sql/framework/TsdbTableFormatter.h>
#include <ha3/sql/framework/PrometheusTableFormatter.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/data/SqlResultUtil.h>
#include <ha3/proto/SqlResult_generated.h>
#include <http_arpc/ProtoJsonizer.h>
#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>

using namespace autil;
using namespace std;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(sql);

double SqlResultFormatter::calcCoveredPercent(const SqlSearchInfo &searchInfo) {
    double coveredPercent = 0.0;
    int32_t totalRpcCount = 0;
    int32_t totalReturnCount = 0;
    for (int i = 0; i < searchInfo.rpcinfos_size(); i++) {
        auto rpcInfo = searchInfo.rpcinfos(i);
        totalRpcCount += rpcInfo.totalrpccount();
        if (rpcInfo.totalrpccount() > rpcInfo.lackcount()) {
            totalReturnCount += rpcInfo.totalrpccount() - rpcInfo.lackcount();
        }
    }
    if (totalRpcCount != 0) {
        coveredPercent = 1.0 * totalReturnCount / totalRpcCount;
    }
    return coveredPercent;
}

void SqlResultFormatter::format(QrsSessionSqlResult &sqlResult,
                                SqlAccessLog *accessLog,
                                Pool *pool) {
    switch (sqlResult.formatType) {
    case QrsSessionSqlResult::SQL_RF_JSON:
        formatJson(sqlResult, accessLog);
        return;
    case QrsSessionSqlResult::SQL_RF_FULL_JSON:
        formatFullJson(sqlResult, accessLog);
        return;
    case QrsSessionSqlResult::SQL_RF_TSDB:
        formatTsdb(sqlResult, accessLog);
        return;
    case QrsSessionSqlResult::SQL_RF_FLATBUFFERS:
        formatFlatbuffers(sqlResult, accessLog, pool);
        return;
    case QrsSessionSqlResult::SQL_RF_TSDB_PROMETHEUS:
        formatTsdbPrometheus(sqlResult, accessLog);
        return;
    default:
        formatString(sqlResult, accessLog);
        return;
    }
}

void SqlResultFormatter::formatJson(QrsSessionSqlResult &sqlResult, SqlAccessLog *accessLog) {
    SqlQueryResponse response;
    if (accessLog) {
        response.processTime = accessLog->getProcessTime() / 1000.0;
        response.rowCount = accessLog->getRowCount();
        const auto &searchInfo = accessLog->getSearchInfo();
        response.coveredPercent = calcCoveredPercent(searchInfo);
        if (sqlResult.getSearchInfo) {
            http_arpc::ProtoJsonizer::toJsonMap(searchInfo, response.searchInfo);
        }
    }
    response.errorInfo = sqlResult.errorResult.toJsonString();
    if (sqlResult.table) {
        response.sqlResult = TableUtil::toJsonString(sqlResult.table);
    }
    response.formatType = sqlResult.typeToStr(sqlResult.formatType);
    response.trace = std::move(sqlResult.sqlTrace);
    if (sqlResult.naviResult) {
        response.trace.insert(response.trace.end(), sqlResult.naviResult->_trace.begin(),
                              sqlResult.naviResult->_trace.end());
    }
    response.sqlQuery = sqlResult.sqlQuery;
    response.iquanResponseWrapper = sqlResult.iquanResponseWrapper;
    response.naviGraph = sqlResult.naviGraph;
    if (sqlResult.readable) {
        sqlResult.resultStr = ToJsonString(response);
    } else {
        sqlResult.resultStr = FastToJsonString(response);
    }
}

void SqlResultFormatter::formatFullJson(QrsSessionSqlResult &sqlResult, SqlAccessLog *accessLog) {
    SqlQueryResponseFullJson response;
    if (accessLog) {
        response.processTime = accessLog->getProcessTime() / 1000.0;
        response.rowCount = accessLog->getRowCount();
        const auto &searchInfo = accessLog->getSearchInfo();
        response.coveredPercent = calcCoveredPercent(searchInfo);
        if (sqlResult.getSearchInfo) {
            http_arpc::ProtoJsonizer::toJsonMap(searchInfo, response.searchInfo);
        }
    }
    response.errorInfo.errorResult = sqlResult.errorResult;
    if (sqlResult.table) {
        TableUtil::toTableJson(sqlResult.table, response.sqlResult);
    }
    response.formatType = sqlResult.typeToStr(sqlResult.formatType);
    response.trace = std::move(sqlResult.sqlTrace);
    if (sqlResult.naviResult) {
        response.trace.insert(response.trace.end(), sqlResult.naviResult->_trace.begin(),
                              sqlResult.naviResult->_trace.end());
    }
    response.sqlQuery = sqlResult.sqlQuery;
    response.iquanResponseWrapper = sqlResult.iquanResponseWrapper;
    response.naviGraph = sqlResult.naviGraph;
    if (sqlResult.readable) {
        sqlResult.resultStr = ToJsonString(response);
    } else {
        sqlResult.resultStr = FastToJsonString(response);
    }
}

void SqlResultFormatter::formatString(QrsSessionSqlResult &sqlResult, SqlAccessLog *accessLog)
{
    if (accessLog) {
        sqlResult.resultStr += "USE_TIME: " + autil::StringUtil::toString(
                accessLog->getProcessTime() / 1000.0) + "ms, ";
        sqlResult.resultStr += "ROW_COUNT: " + autil::StringUtil::toString(accessLog->getRowCount()) + "\n";
    }
    if (sqlResult.errorResult.hasError()) {
        sqlResult.resultStr += sqlResult.errorResult.getErrorMsg() + "\n";
    }
    if (sqlResult.table) {
        sqlResult.resultStr += "\n------------------------------- TABLE INFO ---------------------------\n";
        sqlResult.resultStr += TableUtil::toString(sqlResult.table);
    }
    if (accessLog) {
        sqlResult.resultStr += "\n------------------------------- SEARCH INFO ---------------------------\n";
        sqlResult.resultStr += accessLog->getSearchInfoStr() + "\n";
    }
    if (!sqlResult.sqlQuery.empty()) {
        sqlResult.resultStr += "\n------------------------------- PLAN INFO ---------------------------\n";
        sqlResult.resultStr += "SQL QUERY:\n" + sqlResult.sqlQuery+"\n";

        sqlResult.resultStr += "IQUAN_RESULT:\n" + autil::legacy::ToJsonString(
                sqlResult.iquanResponseWrapper, true) + "\n";
    }

    sqlResult.resultStr += "\n------------------------------- TRACE INFO ---------------------------\n";

    for (auto trace : sqlResult.sqlTrace) {
        sqlResult.resultStr += trace;
    }
    if (sqlResult.naviResult) {
        for (auto trace : sqlResult.naviResult->_trace) {
            sqlResult.resultStr += trace;
        }
    }
}

void SqlResultFormatter::formatTsdb(QrsSessionSqlResult &sqlResult, SqlAccessLog *accessLog)
{
    SqlQueryResponseTsdb response;
    float intergrity = -1;
    if (accessLog) {
        response.processTime = accessLog->getProcessTime() / 1000.0;
        response.rowCount = accessLog->getRowCount();
        const auto &searchInfo = accessLog->getSearchInfo();
        response.coveredPercent = calcCoveredPercent(searchInfo);
        intergrity = response.coveredPercent;
        if (sqlResult.getSearchInfo) {
            http_arpc::ProtoJsonizer::toJsonMap(searchInfo, response.searchInfo);
        }
    }
    response.errorInfo.errorResult = sqlResult.errorResult;

    if (sqlResult.table) {
        ErrorResult errorResult =
            TsdbTableFormatter::convertToTsdb(sqlResult.table,
                    sqlResult.formatDesc, intergrity,  response.tsdbResults);
        if (errorResult.hasError()) {
            response.errorInfo.errorResult = errorResult;
        }
    }
    response.formatType = sqlResult.typeToStr(sqlResult.formatType);
    response.trace = std::move(sqlResult.sqlTrace);
    if (sqlResult.naviResult) {
        response.trace.insert(response.trace.end(), sqlResult.naviResult->_trace.begin(),
                              sqlResult.naviResult->_trace.end());
    }
    response.sqlQuery = sqlResult.sqlQuery;
    response.iquanResponseWrapper = sqlResult.iquanResponseWrapper;
    if (sqlResult.readable) {
        sqlResult.resultStr = ToJsonString(response);
    } else {
        sqlResult.resultStr = FastToJsonString(response);
    }
}

void SqlResultFormatter::formatFlatbuffers(QrsSessionSqlResult &sqlResult,
        SqlAccessLog *accessLog, Pool *pool) {
    string searchInfo;
    double processTime = 0.0;
    uint32_t rowCount = 0u;
    if (accessLog) {
        legacy::json::JsonMap searchInfoJsonMap;
        processTime = accessLog->getProcessTime() / 1000.0;
        rowCount = accessLog->getRowCount();
        if (sqlResult.getSearchInfo) {
            http_arpc::ProtoJsonizer::toJsonMap(accessLog->getSearchInfo(),
                    searchInfoJsonMap);
            searchInfo = ToJsonString(searchInfoJsonMap);
        }
    }

    SqlResultUtil::toFlatBuffersString(processTime, rowCount,
            searchInfo, sqlResult, pool);
}

void SqlResultFormatter::formatTsdbPrometheus(QrsSessionSqlResult &sqlResult, SqlAccessLog *accessLog)
{
    SqlQueryResponseTsdbPrometheus response;
    float intergrity = -1;
    if (accessLog) {
        response.processTime = accessLog->getProcessTime() / 1000.0;
        response.rowCount = accessLog->getRowCount();
        const auto &searchInfo = accessLog->getSearchInfo();
        response.coveredPercent = calcCoveredPercent(searchInfo);
        intergrity = response.coveredPercent;
        if (sqlResult.getSearchInfo) {
            http_arpc::ProtoJsonizer::toJsonMap(searchInfo, response.searchInfo);
        }
    }
    response.errorResult = sqlResult.errorResult;

    if (sqlResult.table) {
        ErrorResult errorResult =
            PrometheusTableFormatter::convertToPrometheus(sqlResult.table,
                    sqlResult.formatDesc, intergrity,  response.data.prometheusResults);
        if (errorResult.hasError()) {
            response.errorResult = errorResult;
        }
    }
    response.formatType = sqlResult.typeToStr(sqlResult.formatType);
    response.trace = std::move(sqlResult.sqlTrace);
    if (sqlResult.naviResult) {
        response.trace.insert(response.trace.end(), sqlResult.naviResult->_trace.begin(),
                              sqlResult.naviResult->_trace.end());
    }
    response.sqlQuery = sqlResult.sqlQuery;
    response.iquanResponseWrapper = sqlResult.iquanResponseWrapper;
    if (sqlResult.readable) {
        sqlResult.resultStr = ToJsonString(response);
    } else {
        sqlResult.resultStr = FastToJsonString(response);
    }
}

END_HA3_NAMESPACE(sql);
