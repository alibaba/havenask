/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/framework/ErrorResult.h"
#include "ha3/sql/framework/TsdbResult.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "navi/engine/NaviResult.h"
#include "navi/log/TraceCollector.h"
#include "table/TableJson.h"

namespace isearch {
namespace sql {

class SqlQueryResponseBase : public autil::legacy::Jsonizable {
public:
    SqlQueryResponseBase()
        : processTime(0.0)
        , hasSoftFailure(false)
        , coveredPercent(0.0)
        , rowCount(0)
        , formatType("json") {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("total_time", processTime, processTime);
        json.Jsonize("has_soft_failure", hasSoftFailure, hasSoftFailure);
        json.Jsonize("covered_percent", coveredPercent, coveredPercent);
        json.Jsonize("row_count", rowCount, rowCount);
        json.Jsonize("format_type", formatType, formatType);
        json.Jsonize("search_info", searchInfo, searchInfo);
        json.Jsonize("rpc_info", rpcInfo, rpcInfo);
        json.Jsonize("table_leader_info", leaderInfos, leaderInfos);
        json.Jsonize("table_build_watermark", watermarkInfos, watermarkInfos);
        if (!sqlQuery.empty()) {
            json.Jsonize("sql_query", sqlQuery);
            json.Jsonize("iquan_plan", iquanResponseWrapper);
            json.Jsonize("navi_graph", naviGraph, naviGraph);
        }
        json.Jsonize("trace", trace);
    }
    void fillTrace(const navi::NaviResultPtr naviResult) {
        assert(naviResult);
        naviResult->traceCollector.format(trace);
    }

public:
    double processTime;
    bool hasSoftFailure;
    double coveredPercent;
    uint32_t rowCount;
    autil::legacy::json::JsonMap searchInfo;
    std::string rpcInfo;
    std::string formatType;
    std::vector<std::string> trace;
    std::string sqlQuery;
    std::string naviGraph;
    iquan::IquanDqlResponseWrapper iquanResponseWrapper;
    std::map<std::string, bool> leaderInfos;
    std::map<std::string, int64_t> watermarkInfos;
};

class SqlQueryResponse : public SqlQueryResponseBase {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        SqlQueryResponseBase::Jsonize(json);
        json.Jsonize("sql_result", sqlResult);
        json.Jsonize("error_info", errorInfo);
    }

public:
    std::string sqlResult;
    std::string errorInfo;
};

typedef std::shared_ptr<SqlQueryResponse> SqlQueryResponsePtr;
struct ErrorResultJsonWrapper : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        errorCode = errorResult.getErrorCode();
        errorStr = haErrorCodeToString(errorCode);
        errorMsg = errorResult.getErrorMsg();
        json.Jsonize("ErrorCode", errorCode);
        json.Jsonize("Error", errorStr);
        json.Jsonize("Message", errorMsg);
    }
    ErrorResult errorResult;
    ErrorCode errorCode;
    std::string errorStr;
    std::string errorMsg;
};

class SqlQueryResponseFullJson : public SqlQueryResponseBase {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        SqlQueryResponseBase::Jsonize(json);
        json.Jsonize("sql_result", sqlResult);
        json.Jsonize("error_info", errorInfo);
    }

public:
    table::TableJson sqlResult;
    ErrorResultJsonWrapper errorInfo;
};

class SqlQueryResponseTsdb : public SqlQueryResponseBase {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        SqlQueryResponseBase::Jsonize(json);
        json.Jsonize("sql_result", tsdbResults);
        json.Jsonize("error_info", errorInfo);
    }

public:
    TsdbResults tsdbResults;
    ErrorResultJsonWrapper errorInfo;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlQueryResponse> SqlQueryResponsePtr;
} // namespace sql
} // namespace isearch
