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
#include "sql/rpc/SqlFormatKernel.h"

#include <engine/NaviConfigContext.h>
#include <exception>
#include <map>
#include <memory>
#include <utility>

#include "aios/network/opentelemetry/core/Span.h"
#include "aios/network/opentelemetry/core/SpanMeta.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/proto/ProtoClassUtil.h"
#include "ha3/proto/QrsService.pb.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "multi_call/interface/QuerySession.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/TimeoutChecker.h"
#include "navi/log/LoggingEvent.h"
#include "navi/log/NaviLogger.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "navi/resource/NaviResultR.h"
#include "navi/resource/QuerySessionR.h"
#include "navi/rpc_server/NaviArpcRequestData.h"
#include "navi/rpc_server/NaviArpcResponseData.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/data/ErrorResult.h"
#include "sql/data/SqlPlanData.h"
#include "sql/data/SqlQueryRequest.h"
#include "sql/data/SqlRequestData.h"
#include "sql/data/TableData.h"
#include "sql/framework/ResultFormatter.h"
#include "sql/framework/SqlAccessLog.h"
#include "sql/framework/SqlAccessLogFormatHelper.h"
#include "sql/ops/metaCollect/SqlMetaData.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "table/Table.h"

namespace sql {

const std::string SqlFormatKernel::KERNEL_ID = "sql.format.k";
const std::string SqlFormatKernel::INPUT_TABLE_GROUP = "table";
const std::string SqlFormatKernel::INPUT_ERROR_GROUP = "err";
const std::string SqlFormatKernel::OUTPUT_PORT = "out";

SqlFormatKernel::SqlFormatKernel() {}

SqlFormatKernel::~SqlFormatKernel() {}

std::string SqlFormatKernel::getName() const {
    return KERNEL_ID;
}

std::vector<std::string> SqlFormatKernel::getOutputs() const {
    return {OUTPUT_PORT};
}

bool SqlFormatKernel::config(navi::KernelConfigContext &ctx) {
    std::string formatStr;
    NAVI_JSONIZE(ctx, "format_type", formatStr, formatStr);
    _formatType = QrsSessionSqlResult::strToType(formatStr);
    NAVI_JSONIZE(ctx, "disable_soft_failure", _disableSoftFailure, _disableSoftFailure);
    return true;
}

navi::ErrorCode SqlFormatKernel::init(navi::KernelInitContext &ctx) {
    _accessLog = std::make_unique<sql::SqlAccessLog>();
    return navi::EC_NONE;
}

navi::ErrorCode SqlFormatKernel::compute(navi::KernelComputeContext &ctx) {
    autil::ScopedTime2 timeScope;
    QrsSessionSqlResult result;
    auto pool = _graphMemoryPoolR->getPool();
    if (!pool) {
        NAVI_KERNEL_LOG(ERROR, "get memory pool failed");
        return navi::EC_ABORT;
    }
    _sqlSearchInfoCollectorR->getCollector()->setSqlRunForkGraphEndTime(
        autil::TimeUtility::currentTime());
    assert(_accessLog && "kernel must be inited before compute");
    process(pool, ctx, result);
    auto responsePb = new isearch::proto::QrsResponse();
    fillResponse(result.resultStr, result.resultCompressType, result.formatType, pool, responsePb);
    size_t responseSize = responsePb->ByteSize();
    auto data = std::make_shared<navi::NaviArpcResponseData>(responsePb);
    ctx.setOutput(0, data, true);
    endGigTrace(responseSize, result);
    _accessLog->setFormatResultTime(timeScope.done_us());
    auto logger = navi::NAVI_TLS_LOGGER;
    if (logger && logger->logger) {
        _accessLog->setLoggerId(logger->logger->getName());
    }
    return navi::EC_NONE;
}

bool SqlFormatKernel::process(const autil::mem_pool::PoolPtr &pool,
                              navi::KernelComputeContext &ctx,
                              QrsSessionSqlResult &result) {
    navi::GroupDatas tableGroupDatas;
    ctx.getGroupInput(0, tableGroupDatas);
    const auto &requestData = tableGroupDatas[0].data;
    const auto &tableData = tableGroupDatas[1].data;
    const auto &planData = tableGroupDatas[2].data;
    const auto &metaData = tableGroupDatas[3].data;
    auto sqlRequestData = dynamic_cast<SqlRequestData *>(requestData.get());
    SqlQueryRequest *sqlQueryRequest = nullptr;
    if (sqlRequestData) {
        sqlQueryRequest = sqlRequestData->getSqlRequest().get();
    } else {
        NAVI_KERNEL_LOG(ERROR, "invalid sql request data");
    }
    initFormatType(sqlQueryRequest);
    fillSqlPlan(sqlQueryRequest, planData, result);
    fillSqlResult(tableData, ctx, result);
    fillMetaData(metaData, ctx.getTimeoutChecker()->beginTime(), result);
    ctx.fillTrace(result.sqlTrace);
    if (sqlQueryRequest) {
        _accessLog->setQueryString(sqlQueryRequest->getRawQueryStr());
    }
    _accessLog->setProcessTime(ctx.getTimeoutChecker()->elapsedTime());
    SqlAccessLogFormatHelper accessLogHelper(*_accessLog);
    if (disableSoftFailure(sqlQueryRequest) && accessLogHelper.hasSoftFailure()) {
        result.errorResult.resetError(isearch::ERROR_SQL_SOFT_FAILURE_NOT_ALLOWED,
                                      "soft failure is not allowed");
    }
    _accessLog->setStatusCode(result.errorResult.getErrorCode());
    _accessLog->setIp(_requestData->getClientIp());
    _accessLog->setRowCount(result.table ? result.table->getRowCount() : 0);
    formatSqlResult(sqlQueryRequest, &accessLogHelper, pool, result);
    _accessLog->setResultLen(result.resultStr.size());
    return true;
}

bool SqlFormatKernel::fillSqlPlan(sql::SqlQueryRequest *sqlQueryRequest,
                                  const navi::DataPtr &planData,
                                  QrsSessionSqlResult &result) const {
    auto sqlPlanData = dynamic_cast<SqlPlanData *>(planData.get());
    if (!sqlPlanData) {
        NAVI_KERNEL_LOG(WARN, "null sql plan data");
        return false;
    }
    result.sqlQueryRequest = *(sqlPlanData->getIquanSqlRequest());
    auto sqlPlan = sqlPlanData->getSqlPlan();
    if (isOutputSqlPlan(sqlQueryRequest) && sqlPlan) {
        iquan::IquanDqlResponse sqlQueryResponse(*sqlPlan);
        iquan::IquanDqlResponseWrapper responseWrapper;
        try {
            if (!responseWrapper.convert(sqlQueryResponse, result.sqlQueryRequest.dynamicParams)) {
                NAVI_KERNEL_LOG(ERROR, "convert sql plan failed");
                return false;
            }
            result.iquanResponseWrapper = responseWrapper;
        } catch (const std::exception &e) {
            NAVI_KERNEL_LOG(ERROR, "convert sql plan failed, exception: %s", e.what());
            return false;
        }
    }
    return true;
}

bool SqlFormatKernel::isOutputSqlPlan(sql::SqlQueryRequest *sqlQueryRequest) const {
    if (sqlQueryRequest && sqlQueryRequest->isOutputSqlPlan()) {
        return true;
    }
    return navi::logEnable(navi::NAVI_TLS_LOGGER, navi::LOG_LEVEL_DEBUG);
}

void SqlFormatKernel::initFormatType(const sql::SqlQueryRequest *sqlQueryRequest) {
    if (!sqlQueryRequest) {
        return;
    }
    std::string format;
    sqlQueryRequest->getValue(SQL_FORMAT_TYPE, format);
    if (format.empty()) {
        sqlQueryRequest->getValue(SQL_FORMAT_TYPE_NEW, format);
    }
    if (format.empty()) {
        return;
    }
    _formatType = QrsSessionSqlResult::strToType(format);
}

QrsSessionSqlResult::SearchInfoLevel
SqlFormatKernel::parseSearchInfoLevel(sql::SqlQueryRequest *sqlQueryRequest) const {
    std::string info;
    sqlQueryRequest->getValue(SQL_GET_SEARCH_INFO, info);
    if (!info.empty()) {
        if (info == "simple") {
            return QrsSessionSqlResult::SearchInfoLevel::SQL_SIL_SIMPLE;
        }
        bool ret;
        autil::StringUtil::fromString(info, ret);
        if (ret || info == "full") {
            return QrsSessionSqlResult::SearchInfoLevel::SQL_SIL_FULL;
        }
    }
    return QrsSessionSqlResult::SearchInfoLevel::SQL_SIL_NONE;
}

isearch::proto::FormatType
SqlFormatKernel::convertFormatType(QrsSessionSqlResult::SqlResultFormatType formatType) {
    switch (formatType) {
    case sql::QrsSessionSqlResult::SQL_RF_JSON:
        return isearch::proto::FT_SQL_JSON;
    case sql::QrsSessionSqlResult::SQL_RF_STRING:
        return isearch::proto::FT_SQL_STRING;
    case sql::QrsSessionSqlResult::SQL_RF_FULL_JSON:
        return isearch::proto::FT_SQL_FULL_JSON;
    case sql::QrsSessionSqlResult::SQL_RF_FLATBUFFERS:
        return isearch::proto::FT_SQL_FLATBUFFERS;
    default:
        return isearch::proto::FT_SQL_UNKNOWN;
    }
}

bool SqlFormatKernel::fillSqlResult(const navi::DataPtr &data,
                                    navi::KernelComputeContext &ctx,
                                    QrsSessionSqlResult &result) const {
    auto tableData = dynamic_cast<TableData *>(data.get());
    if (!tableData || !tableData->getTable()) {
        const auto &message = ctx.firstErrorEvent().message;
        if (isearch::ERROR_NONE == result.errorResult.getErrorCode()) {
            result.errorResult.resetError(isearch::ERROR_SQL_RUN_GRAPH, message);
        }
        NAVI_KERNEL_LOG(WARN, "empty searcher result table");
        return false;
    }
    result.table = tableData->getTable();
    return true;
}

void SqlFormatKernel::fillMetaData(const navi::DataPtr &metaData,
                                   int64_t runGraphBeginTime,
                                   QrsSessionSqlResult &result) {
    auto *sqlMetaData = dynamic_cast<SqlMetaData *>(metaData.get());
    if (!sqlMetaData) {
        SQL_LOG(WARN, "sql meta data is empty");
        return;
    }
    const auto &collector = sqlMetaData->getCollector();
    if (!collector) {
        SQL_LOG(WARN, "sql info collector is empty");
        return;
    }
    auto info = collector->stealSqlSearchInfo();
    SQL_LOG(DEBUG, "sql info received [%s]", info.DebugString().c_str());
    auto thisInfo = _sqlSearchInfoCollectorR->getCollector()->stealSqlSearchInfo();
    info.mutable_runsqltimeinfo()->Swap(thisInfo.mutable_runsqltimeinfo());
    RunSqlTimeInfo *timeInfo = info.mutable_runsqltimeinfo();
    timeInfo->set_sqlrunforkgraphtime(timeInfo->sqlrunforkgraphendtime()
                                      - timeInfo->sqlrunforkgraphbegintime());
    timeInfo->set_sqlrungraphtime(timeInfo->sqlrunforkgraphendtime() - runGraphBeginTime);
    _accessLog->setSearchInfo(std::move(info),
                              _formatType != QrsSessionSqlResult::SQL_RF_FLATBUFFERS_TIMELINE);
    _accessLog->setRpcInfoMap(_naviResultR->getRpcInfoMap());
}

void SqlFormatKernel::formatSqlResult(sql::SqlQueryRequest *sqlQueryRequest,
                                      const SqlAccessLogFormatHelper *accessLogHelper,
                                      const autil::mem_pool::PoolPtr &pool,
                                      QrsSessionSqlResult &result) const {
    if (sqlQueryRequest) {
        result.readable = sqlQueryRequest->isResultReadable();
        result.sqlQuery = sqlQueryRequest->getRawQueryStr();
        result.formatType = _formatType;
        sqlQueryRequest->getValue(SQL_FORMAT_DESC, result.formatDesc);
        result.resultCompressType = sqlQueryRequest->getResultCompressType();
        result.searchInfoLevel = parseSearchInfoLevel(sqlQueryRequest);
    }
    ResultFormatter::format(result, accessLogHelper, pool.get());
}

void SqlFormatKernel::fillResponse(const std::string &result,
                                   autil::CompressType compressType,
                                   QrsSessionSqlResult::SqlResultFormatType formatType,
                                   const autil::mem_pool::PoolPtr &pool,
                                   isearch::proto::QrsResponse *response) {
    std::string compressedResult;
    bool compressed = false;
    if (compressType != autil::CompressType::NO_COMPRESS
        && compressType != autil::CompressType::INVALID_COMPRESS_TYPE) {
        compressed
            = autil::CompressionUtil::compress(result, compressType, compressedResult, pool.get());

        if (compressed) {
            response->set_compresstype(
                isearch::proto::ProtoClassUtil::convertCompressType(compressType));
            response->set_assemblyresult(compressedResult);
        }
    }

    if (!compressed) {
        response->set_compresstype(isearch::proto::CT_NO_COMPRESS);
        response->set_assemblyresult(result);
    }
    response->set_formattype(convertFormatType(formatType));
}

bool SqlFormatKernel::disableSoftFailure(sql::SqlQueryRequest *sqlQueryRequest) const {
    std::string info;
    if (sqlQueryRequest) {
        sqlQueryRequest->getValue(SQL_RESULT_ALLOW_SOFT_FAILURE, info);
    }
    if (info.empty()) {
        return _disableSoftFailure;
    }
    bool res = false;
    autil::StringUtil::fromString(info, res);
    return !res;
}

void SqlFormatKernel::endGigTrace(size_t responseSize, QrsSessionSqlResult &result) {
    const auto &querySession = _querySessionR->getQuerySession();
    auto span = querySession->getTraceServerSpan();
    if (span) {
        _accessLog->setTraceId(opentelemetry::EagleeyeUtil::getETraceId(span));
        const auto &userDataMap = opentelemetry::EagleeyeUtil::getEUserDatas(span);
        std::string userData;
        for (auto iter = userDataMap.begin(); iter != userDataMap.end(); iter++) {
            if (iter->first == "s") {
                continue;
            }
            userData += iter->first + "=" + iter->second + ";";
        }
        _accessLog->setUserData(userData);
        span->setAttribute(opentelemetry::kSpanAttrRespContentLength, std::to_string(responseSize));
        if (result.errorResult.hasError()) {
            span->setStatus(opentelemetry::StatusCode::kError,
                            result.errorResult.getErrorDescription());
        } else {
            span->setStatus(opentelemetry::StatusCode::kOk);
        }
    }
}

REGISTER_KERNEL(SqlFormatKernel);

} // namespace sql
