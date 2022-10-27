#ifndef ISEARCH_SQLQUERYRESPONSE_H
#define ISEARCH_SQLQUERYRESPONSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <multi_call/common/ErrorCode.h>
#include <ha3/sql/data/TableJson.h>
#include <ha3/sql/framework/TsdbResult.h>
#include <ha3/sql/framework/PrometheusResult.h>
#include <iquan_common/service/ResponseWrapper.h>
BEGIN_HA3_NAMESPACE(sql);

class SqlQueryResponseBase : public autil::legacy::Jsonizable {
public:
    SqlQueryResponseBase()
        : processTime(0.0)
        , coveredPercent(0.0)
        , rowCount(0)
        , formatType("json") {
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        json.Jsonize("total_time", processTime, processTime);
        json.Jsonize("covered_percent", coveredPercent, coveredPercent);
        json.Jsonize("row_count", rowCount, rowCount);
        json.Jsonize("format_type", formatType, formatType);
        json.Jsonize("search_info", searchInfo, searchInfo);
        if (!sqlQuery.empty()) {
            json.Jsonize("sql_query", sqlQuery);
            json.Jsonize("iquan_plan", iquanResponseWrapper);
            json.Jsonize("navi_graph", naviGraph, naviGraph);
        }
        json.Jsonize("trace", trace);
    }
public:
    double processTime;
    double coveredPercent;
    uint32_t rowCount;
    autil::legacy::json::JsonMap searchInfo;
    std::string formatType;
    std::list<std::string> trace;
    std::string sqlQuery;
    std::string naviGraph;
    iquan::SqlQueryResponseWrapper iquanResponseWrapper;
};

class SqlQueryResponse : public SqlQueryResponseBase {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        SqlQueryResponseBase::Jsonize(json);
        json.Jsonize("sql_result", sqlResult);
        json.Jsonize("error_info", errorInfo);
    }
public:
    std::string sqlResult;
    std::string errorInfo;
};

HA3_TYPEDEF_PTR(SqlQueryResponse);

struct ErrorResultJsonWrapper : public autil::legacy::Jsonizable {
   void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
       errorCode = errorResult.getErrorCode();
       errorStr = haErrorCodeToString(errorCode);
       errorMsg = errorResult.getErrorMsg();
       json.Jsonize("ErrorCode", errorCode);
       json.Jsonize("Error", errorStr);
       json.Jsonize("Message", errorMsg);
   }
    common::ErrorResult errorResult;
    ErrorCode errorCode;
    std::string errorStr;
    std::string errorMsg;
};

class SqlQueryResponseFullJson : public SqlQueryResponseBase {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        SqlQueryResponseBase::Jsonize(json);
        json.Jsonize("sql_result", sqlResult);
        json.Jsonize("error_info", errorInfo);
    }
public:
    TableJson sqlResult;
    ErrorResultJsonWrapper errorInfo;
};

class SqlQueryResponseTsdb : public SqlQueryResponseBase {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        SqlQueryResponseBase::Jsonize(json);
        json.Jsonize("sql_result", tsdbResults);
        json.Jsonize("error_info", errorInfo);
    }
public:
    TsdbResults tsdbResults;
    ErrorResultJsonWrapper errorInfo;
private:
    HA3_LOG_DECLARE();
};

class SqlQueryResponseTsdbPrometheusData : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        std::string resultType = "matrix";
        json.Jsonize("resultType", resultType);
        json.Jsonize("result", prometheusResults);
    }
public:
    PrometheusResults prometheusResults;
private:
    HA3_LOG_DECLARE();
};

class SqlQueryResponseTsdbPrometheus : public SqlQueryResponseBase {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        SqlQueryResponseBase::Jsonize(json);
        json.Jsonize("data", data);

        auto errorCode = errorResult.getErrorCode();
        status = errorCode == ERROR_NONE ? "success" : "error";
        errorType = haErrorCodeToString(errorCode);
        error = errorResult.getErrorMsg();
        json.Jsonize("status", status);
        json.Jsonize("error", error);
        json.Jsonize("errorType", errorType);
    }
public:
    SqlQueryResponseTsdbPrometheusData data;
    common::ErrorResult errorResult;
    std::string status;
    std::string errorType;
    std::string error;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SqlQueryResponse);
END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQLQUERYRESPONSE_H
