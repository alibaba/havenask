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
#include "sql/framework/SqlResultFormatter.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <exception>
#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "ha3/common/ErrorDefine.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "multi_call/common/ErrorCode.h"
#include "multi_call/stream/GigStreamRpcInfo.h"
#include "navi/engine/NaviResult.h"
#include "navi/log/TraceCollector.h"
#include "sql/data/ErrorResult.h"
#include "sql/framework/QrsSessionSqlResult.h"
#include "sql/framework/SqlAccessLog.h"
#include "sql/framework/SqlAccessLogFormatHelper.h"
#include "sql/framework/SqlQueryResponse.h"
#include "sql/framework/SqlResultUtil.h"
#include "sql/proto/SqlSearch.pb.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

using namespace std;
using namespace table;
using namespace autil;
using namespace autil::mem_pool;
using namespace multi_call;

namespace sql {
AUTIL_LOG_SETUP(sql, SqlResultFormatter);

void SqlResultFormatter::formatJson(QrsSessionSqlResult &sqlResult,
                                    const SqlAccessLogFormatHelper *accessLogHelper) {
    SqlQueryResponse response;
    if (accessLogHelper) {
        auto *accessLog = accessLogHelper->getAccessLog();
        response.hasSoftFailure = accessLogHelper->hasSoftFailure();
        response.coveredPercent = accessLogHelper->coveredPercent();
        response.softFailureCodes = accessLogHelper->getSoftFailureCodes();

        response.processTime = accessLog->getProcessTime() / 1000.0;
        response.rowCount = accessLog->getRowCount();
        response.watermarkInfos = accessLog->getBuildWatermarks();
        const auto &searchInfo = accessLog->getSearchInfo();
        if (sqlResult.searchInfoLevel != QrsSessionSqlResult::SQL_SIL_NONE) {
            http_arpc::ProtoJsonizer::toJsonMap(searchInfo, response.searchInfo);
            if (sqlResult.searchInfoLevel != QrsSessionSqlResult::SQL_SIL_SIMPLE) {
                response.rpcInfo = StringUtil::toString(accessLog->getRpcInfoMap());
            }
        }
    }
    response.errorInfo = sqlResult.errorResult.toJsonString();
    if (sqlResult.table) {
        response.sqlResult = TableUtil::toJsonString(sqlResult.table);
    }
    response.formatType = sqlResult.typeToStr(sqlResult.formatType);
    response.trace = std::move(sqlResult.sqlTrace);
    response.sqlQuery = sqlResult.sqlQuery;
    response.naviGraph = sqlResult.naviGraph;
    try {
        response.iquanResponseWrapper = sqlResult.iquanResponseWrapper;
        sqlResult.resultStr = FastToJsonString(response);
    } catch (std::exception &e) { SQL_LOG(ERROR, "format json result error, msg: [%s]", e.what()); }
}

void SqlResultFormatter::formatFullJson(QrsSessionSqlResult &sqlResult,
                                        const SqlAccessLogFormatHelper *accessLogHelper) {
    SqlQueryResponseFullJson response;
    if (accessLogHelper) {
        auto *accessLog = accessLogHelper->getAccessLog();
        response.hasSoftFailure = accessLogHelper->hasSoftFailure();
        response.softFailureCodes = accessLogHelper->getSoftFailureCodes();
        response.coveredPercent = accessLogHelper->coveredPercent();

        response.processTime = accessLog->getProcessTime() / 1000.0;
        response.rowCount = accessLog->getRowCount();
        response.watermarkInfos = accessLog->getBuildWatermarks();
        const auto &searchInfo = accessLog->getSearchInfo();
        if (sqlResult.searchInfoLevel != QrsSessionSqlResult::SQL_SIL_NONE) {
            http_arpc::ProtoJsonizer::toJsonMap(searchInfo, response.searchInfo);
            if (sqlResult.searchInfoLevel != QrsSessionSqlResult::SQL_SIL_SIMPLE) {
                response.rpcInfo = StringUtil::toString(accessLog->getRpcInfoMap());
            }
        }
    }
    response.errorInfo.errorResult = sqlResult.errorResult;
    if (sqlResult.table) {
        TableUtil::toTableJson(sqlResult.table, response.sqlResult);
    }
    response.formatType = sqlResult.typeToStr(sqlResult.formatType);
    response.trace = std::move(sqlResult.sqlTrace);
    response.sqlQuery = sqlResult.sqlQuery;
    response.naviGraph = sqlResult.naviGraph;
    try {
        response.iquanResponseWrapper = sqlResult.iquanResponseWrapper;
        sqlResult.resultStr = FastToJsonString(response);
    } catch (std::exception &e) { SQL_LOG(ERROR, "format json result error, msg: [%s]", e.what()); }
}

void SqlResultFormatter::formatString(QrsSessionSqlResult &sqlResult,
                                      const SqlAccessLogFormatHelper *accessLogHelper) {
    const SqlAccessLog *accessLog = nullptr;
    if (accessLogHelper) {
        accessLog = accessLogHelper->getAccessLog();
        sqlResult.resultStr
            += "USE_TIME: " + autil::StringUtil::toString(accessLog->getProcessTime() / 1000.0)
               + "ms, ";
        sqlResult.resultStr
            += "ROW_COUNT: " + autil::StringUtil::toString(accessLog->getRowCount()) + ", ";
        sqlResult.resultStr
            += "HAS_SOFT_FAILURE: " + autil::StringUtil::toString(accessLogHelper->hasSoftFailure())
               + ", ";
        sqlResult.resultStr
            += "SOFT_FAILURE_CODES: ["
               + autil::StringUtil::toString(accessLogHelper->getSoftFailureCodes(), ",") + "], ";
        sqlResult.resultStr
            += "COVERAGE: " + autil::StringUtil::toString(accessLogHelper->coveredPercent()) + "\n";

        auto buildInfos = accessLog->getBuildWatermarks();
        if (!buildInfos.empty()) {
            sqlResult.resultStr
                += "BUILD_WATERMARK: " + autil::StringUtil::toString(buildInfos) + "\n";
        }
    }
    if (sqlResult.errorResult.hasError()) {
        sqlResult.resultStr += sqlResult.errorResult.getErrorMsg() + "\n";
    }
    if (sqlResult.table) {
        sqlResult.resultStr
            += "\n------------------------------- TABLE INFO ---------------------------\n";
        sqlResult.resultStr += TableUtil::toString(sqlResult.table);
    }
    if (accessLog != nullptr && sqlResult.searchInfoLevel != QrsSessionSqlResult::SQL_SIL_NONE) {
        sqlResult.resultStr
            += "\n------------------------------- SEARCH INFO ---------------------------\n";
        auto &searchInfo = accessLog->getSearchInfo();
        sqlResult.resultStr += searchInfo.ShortDebugString() + "\n";
        if (sqlResult.searchInfoLevel != QrsSessionSqlResult::SQL_SIL_SIMPLE) {
            sqlResult.resultStr
                += "\n------------------------------- RPC INFO ---------------------------\n";
            sqlResult.resultStr += StringUtil::toString(accessLog->getRpcInfoMap()) + "\n";
        }
    }

    if (!sqlResult.sqlQuery.empty()) {
        sqlResult.resultStr
            += "\n------------------------------- PLAN INFO ---------------------------\n";
        sqlResult.resultStr += "SQL QUERY:\n" + sqlResult.sqlQuery + "\n";

        sqlResult.resultStr
            += "IQUAN_RESULT:\n"
               + autil::legacy::FastToJsonString(sqlResult.iquanResponseWrapper, true) + "\n";
    }

    sqlResult.resultStr
        += "\n------------------------------- TRACE INFO ---------------------------\n";

    for (const auto &trace : sqlResult.sqlTrace) {
        sqlResult.resultStr += trace;
    }
}

void SqlResultFormatter::formatFlatbuffers(QrsSessionSqlResult &sqlResult,
                                           const SqlAccessLogFormatHelper *accessLogHelper,
                                           Pool *pool,
                                           bool timeline,
                                           bool useByteString) {
    string searchInfoStr;
    double processTime = 0.0;
    uint32_t rowCount = 0u;
    double coveredPercent = 1.0;
    map<string, bool> leaderInfo;
    map<string, int64_t> watermarkInfo;
    bool hasSoftFailure = false;
    vector<int64_t> softFailureCodes;
    if (accessLogHelper) {
        auto *accessLog = accessLogHelper->getAccessLog();
        coveredPercent = accessLogHelper->coveredPercent();
        hasSoftFailure = accessLogHelper->hasSoftFailure();
        softFailureCodes = accessLogHelper->getSoftFailureCodes();

        processTime = accessLog->getProcessTime() / 1000.0;
        rowCount = accessLog->getRowCount();
        watermarkInfo = accessLog->getBuildWatermarks();
        const auto &searchInfo = accessLog->getSearchInfo();
        if (!timeline) {
            if (sqlResult.searchInfoLevel != QrsSessionSqlResult::SQL_SIL_NONE) {
                if (useByteString) {
                    searchInfo.SerializeToString(&searchInfoStr);
                } else {
                    legacy::json::JsonMap searchInfoJsonMap;
                    http_arpc::ProtoJsonizer::toJsonMap(searchInfo, searchInfoJsonMap);
                    searchInfoStr = FastToJsonString(searchInfoJsonMap);
                }
            }
        } else {
            searchInfo.SerializeToString(&searchInfoStr);
        }
    }
    SqlResultUtil(useByteString)
        .toFlatBuffersString(processTime,
                             rowCount,
                             coveredPercent,
                             searchInfoStr,
                             sqlResult,
                             leaderInfo,
                             watermarkInfo,
                             hasSoftFailure,
                             softFailureCodes,
                             pool);
}

void SqlResultFormatter::formatTable(QrsSessionSqlResult &sqlResult,
                                     const SqlAccessLogFormatHelper *accessLogHelper,
                                     Pool *pool) {
    SqlSearcherResponse response;
    const SqlAccessLog *accessLog = accessLogHelper ? accessLogHelper->getAccessLog() : nullptr;
    if (accessLog != nullptr && sqlResult.searchInfoLevel != QrsSessionSqlResult::SQL_SIL_NONE) {
        *response.mutable_searchinfo() = accessLog->getSearchInfo();
    }
    SqlErrorInfo *errorInfo = response.mutable_errorinfo();
    errorInfo->set_multicall_ec(sqlResult.multicallEc);
    errorInfo->set_errormsg(sqlResult.errorResult.getErrorDescription());
    if (sqlResult.errorResult.getErrorCode() != isearch::ERROR_NONE) {
        errorInfo->set_errorcode(SQL_ERROR_RUN_FAILED);
    }

    if (sqlResult.table.get() != nullptr) {
        auto &resultStr = *(response.mutable_tabledata());
        sqlResult.table->serializeToString(resultStr, pool);
    }

    if (!response.SerializeToString(&sqlResult.resultStr)) {
        SQL_LOG(ERROR, "serialize to string fail");
    }
}

} // namespace sql
