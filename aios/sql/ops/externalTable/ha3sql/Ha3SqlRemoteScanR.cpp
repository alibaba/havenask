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
#include "sql/ops/externalTable/ha3sql/Ha3SqlRemoteScanR.h"

#include <assert.h>
#include <engine/NaviConfigContext.h>
#include <flatbuffers/flatbuffers.h>
#include <google/protobuf/message.h>
#include <map>
#include <ostream>
#include <rapidjson/document.h>
#include <set>
#include <stddef.h>
#include <type_traits>
#include <typeinfo>
#include <utility>

#include "autil/CompressionUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "autil/legacy/fast_jsonizable_dec.h"
#include "autil/legacy/md5.h"
#include "autil/result/Result.h"
#include "ha3/common/Query.h"
#include "ha3/proto/QrsService.pb.h"
#include "ha3/proto/SqlResult_generated.h"
#include "iquan/common/Common.h"
#include "kmonitor/client/MetricsReporter.h"
#include "multi_call/common/ErrorCode.h"
#include "multi_call/interface/Response.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/AsyncPipe.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h"
#include "sql/common/TableMeta.h"
#include "sql/common/common.h"
#include "sql/data/ErrorResult.h"
#include "sql/framework/PushDownOp.h"
#include "sql/framework/SqlResultUtil.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/condition/OutputFieldsVisitor.h"
#include "sql/ops/externalTable/GigQuerySessionCallbackCtxR.h"
#include "sql/ops/externalTable/ha3sql/Ha3SqlConditionVisitor.h"
#include "sql/ops/externalTable/ha3sql/Ha3SqlExprGeneratorVisitor.h"
#include "sql/ops/externalTable/ha3sql/Ha3SqlRequestGenerator.h"
#include "sql/ops/scan/ScanUtil.h"
#include "sql/ops/util/KernelUtil.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "sql/resource/TimeoutTerminatorR.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace table;

namespace sql {

std::string Ha3SqlRemoteScanR::QueryGenerateParam::toString() const {
    std::stringstream ss;
    ss << "serviceName:" << serviceName << ",dbName:" << dbName << ",tableName:" << tableName
       << ",enableAuth:" << enableAuth << ",authToken:" << authToken << ",authKey:" << authKey
       << ",leftTimeMs:" << leftTimeMs;
    return std::move(ss).str();
}

const std::string Ha3SqlRemoteScanR::RESOURCE_ID = "ha3_sql_remote_scan_r";

Ha3SqlRemoteScanR::Ha3SqlRemoteScanR() {}

Ha3SqlRemoteScanR::~Ha3SqlRemoteScanR() {}

void Ha3SqlRemoteScanR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool Ha3SqlRemoteScanR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "remote_scan_init_fire", _initFireQuery, _initFireQuery);
    return true;
}

navi::ErrorCode Ha3SqlRemoteScanR::init(navi::ResourceInitContext &ctx) {
    if (!ScanBase::doInit()) {
        return navi::EC_ABORT;
    }
    setAsyncPipe(_ctx->getAsyncPipe());
    if (!initWithExternalTableConfig()) {
        SQL_LOG(ERROR, "init with external table config failed");
        return navi::EC_ABORT;
    }
    if (!initOutputFieldsVisitor()) {
        SQL_LOG(ERROR, "init output fields visitor failed");
        return navi::EC_ABORT;
    }
    if (!initConditionVisitor()) {
        SQL_LOG(ERROR, "init condition visitor failed");
        return navi::EC_ABORT;
    }
    if (!_initFireQuery) {
        getAsyncPipe()->setData(nullptr); // mock activation data for first compute
        SQL_LOG(DEBUG, "skip init fire query, trigger by update scan query later");
    } else if (!fireQuery()) {
        SQL_LOG(ERROR, "fire query to external table failed");
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool Ha3SqlRemoteScanR::initWithExternalTableConfig() {
    auto &tableConfigMap = _externalTableConfigR->tableConfigMap;
    auto it = tableConfigMap.find(_scanInitParamR->tableName);
    if (it == tableConfigMap.end()) {
        SQL_LOG(ERROR,
                "table[%s] not configured in external table config",
                _scanInitParamR->tableName.c_str());
        return false;
    }
    _tableConfig = &it->second;

    if (_tableConfig->serviceName.empty() || _tableConfig->dbName.empty()
        || _tableConfig->tableName.empty()) {
        SQL_LOG(ERROR,
                "invalid table config[%s] table[%s]",
                FastToJsonString(*_tableConfig, true).c_str(),
                _scanInitParamR->tableName.c_str());
        return false;
    }

    if (_tableConfig->dbName == SQL_DEFAULT_EXTERNAL_DATABASE_NAME) {
        // TODO
        SQL_LOG(ERROR,
                "accessing external table recursively not allowed, table[%s]",
                _scanInitParamR->tableName.c_str());
        return false;
    }

    auto &serviceConfigMap = _externalTableConfigR->serviceConfigMap;
    auto serviceIter = serviceConfigMap.find(_tableConfig->serviceName);
    if (serviceIter == serviceConfigMap.end()) {
        SQL_LOG(ERROR,
                "service name[%s] not found in service configs, table[%s]",
                _tableConfig->serviceName.c_str(),
                _scanInitParamR->tableName.c_str());
        return false;
    }
    _serviceConfig = &serviceIter->second;
    if (_serviceConfig->type != EXTERNAL_TABLE_TYPE_HA3SQL) {
        SQL_LOG(ERROR,
                "invalid service type, expect[%s] actual[%s]",
                EXTERNAL_TABLE_TYPE_HA3SQL.c_str(),
                _serviceConfig->type.c_str());
        return false;
    }

    _miscConfig = dynamic_cast<const Ha3ServiceMiscConfig *>(_serviceConfig->miscConfig.get());
    if (!_miscConfig) {
        auto *miscConfig = _serviceConfig->miscConfig.get();
        SQL_LOG(ERROR,
                "cast misc config failed, typeid[%s] service[%s] table[%s]",
                miscConfig ? typeid(*miscConfig).name() : "null",
                _tableConfig->serviceName.c_str(),
                _scanInitParamR->tableName.c_str());
        return false;
    }
    return true;
}

bool Ha3SqlRemoteScanR::initOutputFieldsVisitor() {
    auto tempPool = _graphMemoryPoolR->getPool();
    autil::AutilPoolAllocator allocator(tempPool.get());
    autil::SimpleDocument outputExprsDoc(&allocator);
    const auto &outputExprsJson = _scanInitParamR->calcInitParamR->outputExprsJson;
    outputExprsDoc.Parse(outputExprsJson.c_str());
    if (outputExprsDoc.HasParseError()) {
        SQL_LOG(ERROR, "parse output exprs error, jsonStr[%s]", outputExprsJson.c_str());
        return false;
    }
    if (!outputExprsDoc.IsObject()) {
        SQL_LOG(ERROR, "output exprs format error, jsonStr[%s]", outputExprsJson.c_str());
        return false;
    }
    auto outputFieldsJson = autil::legacy::FastToJsonString(
        _scanInitParamR->calcInitParamR->outputFieldsOrigin, true);
    autil::SimpleDocument outputFieldsDoc(&allocator);
    outputFieldsDoc.Parse(outputFieldsJson.c_str());
    if (outputFieldsDoc.HasParseError()) {
        SQL_LOG(ERROR, "parse output fields error, jsonStr[%s]", outputFieldsJson.c_str());
        return false;
    }
    if (!outputFieldsDoc.IsArray()) {
        SQL_LOG(ERROR, "output fields format error, jsonStr[%s]", outputFieldsJson.c_str());
        return false;
    }
    SQL_LOG(TRACE3,
            "outputFields[%s] outputExprsJson[%s]",
            outputFieldsJson.c_str(),
            outputExprsJson.c_str());

    _outputFieldsVisitor.reset(new OutputFieldsVisitor());
    for (size_t i = 0; i < outputFieldsDoc.Size(); ++i) {
        assert(outputFieldsDoc[i].IsString());
        string fieldName = outputFieldsDoc[i].GetString();
        if (outputExprsDoc.HasMember(fieldName)) {
            _outputFieldsVisitor->visit(outputExprsDoc[fieldName]);
        } else {
            _outputFieldsVisitor->visit(outputFieldsDoc[i]);
        }
        if (_outputFieldsVisitor->isError()) {
            SQL_LOG(ERROR, "visitor visit failed: %s", _outputFieldsVisitor->errorInfo().c_str());
            return false;
        }
    }
    SQL_LOG(TRACE3,
            "visited output fields item[%s] column[%s]",
            autil::StringUtil::toString(_outputFieldsVisitor->getUsedFieldsItemSet()).c_str(),
            autil::StringUtil::toString(_outputFieldsVisitor->getUsedFieldsColumnSet()).c_str());
    return true;
}

bool Ha3SqlRemoteScanR::initConditionVisitor() {
    auto tempPool = _graphMemoryPoolR->getPool();
    ConditionParser parser(tempPool.get());
    ConditionPtr condition;
    const auto &conditionJson = _scanInitParamR->calcInitParamR->conditionJson;
    SQL_LOG(TRACE2, "start parsing condition [%s]", conditionJson.c_str());
    if (!parser.parseCondition(conditionJson, condition)) {
        SQL_LOG(ERROR, "parse condition [%s] failed", conditionJson.c_str());
        return false;
    }
    _conditionVisitor.reset(new Ha3SqlConditionVisitor(_graphMemoryPoolR->getPool()));
    // select metric from meta
    if (condition == nullptr) {
        return true;
    }

    condition->accept(_conditionVisitor.get());
    if (_conditionVisitor->isError()) {
        SQL_LOG(ERROR,
                "visit condition [%s] failed: %s",
                conditionJson.c_str(),
                _conditionVisitor->errorInfo().c_str());
        return false;
    }
    return true;
}

bool Ha3SqlRemoteScanR::fireQuery() {
    GigQuerySessionCallbackCtxR::ScanOptions options;
    options.requestGenerator = constructRequestGenerator();
    if (!_ctx->start(options)) {
        SQL_LOG(ERROR, "start scan external table failed");
        return false;
    }
    return true;
}

bool Ha3SqlRemoteScanR::doBatchScan(TablePtr &table, bool &eof) {
    do {
        _scanInitParamR->incSeekTime(_ctx->getSeekTime());

        auto result = _ctx->stealResult();
        if (result.is_err()) {
            SQL_LOG(ERROR, "get result failed, error[%s]", result.get_error().message().c_str());
            if (_tableConfig->allowDegradedAccess) {
                break;
            } else {
                return false;
            }
        }

        autil::ScopedTime2 outputTimer;
        bool hasSoftFailure;
        table = fillTableResult(result.get(), hasSoftFailure);
        if (!table) {
            SQL_LOG(ERROR, "fill table[%s] result failed", _scanInitParamR->tableName.c_str());
            if (_tableConfig->allowDegradedAccess) {
                break;
            } else {
                return false;
            }
        }
        if (hasSoftFailure) {
            if (!_tableConfig->allowDegradedAccess) {
                SQL_LOG(ERROR, "soft failure occured, but degraded access not allowed");
                return false;
            }
            SQL_LOG(DEBUG, "soft failure occured");
            _scanInitParamR->incDegradedDocsCount(1);
        }
        _scanInitParamR->incOutputTime(outputTimer.done_us());
        SQL_LOG(TRACE3, "table before evaluate: [%s]", TableUtil::toString(table, 10).c_str());

        autil::ScopedTime2 evaluteTimer;
        _seekCount += table->getRowCount();
        if (!_calcTableR->projectTable(table)) {
            // filter process has been done by SQL query, only project needed
            SQL_LOG(ERROR, "project table failed");
            return false;
        }
        _scanInitParamR->incEvaluateTime(evaluteTimer.done_us());
        SQL_LOG(TRACE3, "table after evaluate: [%s]", TableUtil::toString(table, 10).c_str());

        eof = true;
        return true;
    } while (false);

    // return empty table
    const auto &outputFields = _scanInitParamR->calcInitParamR->outputFields;
    const auto &outputFieldsType = _scanInitParamR->calcInitParamR->outputFieldsType;
    table
        = ScanUtil::createEmptyTable(outputFields, outputFieldsType, _graphMemoryPoolR->getPool());
    eof = true;
    _scanInitParamR->incDegradedDocsCount(1);
    return true;
}

void Ha3SqlRemoteScanR::onBatchScanFinish() {
    const auto &metricsReporter = _queryMetricReporterR->getReporter();
    if (metricsReporter) {
        auto reporter = metricsReporter->getSubReporter(
            "ha3sqlExt", {{{"table_name", getTableNameForMetrics()}}});
        if (!_ctx->tryReportMetrics(*reporter)) {
            SQL_LOG(TRACE3, "ignore report metrics");
        }
    }
}

bool Ha3SqlRemoteScanR::doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
    _extraQuery = inputQuery;
    if (!_extraQuery) {
        SQL_LOG(TRACE3, "init query for empty join");
        _extraQuery.reset(new StreamQuery());
    }
    if (_extraQuery->query) {
        SQL_LOG(ERROR, "only pk is supported");
        return false;
    }
    if (!fireQuery()) {
        SQL_LOG(ERROR, "fire query to external table failed");
        return false;
    }
    return true;
}

bool Ha3SqlRemoteScanR::genQueryString(const QueryGenerateParam &queryParam,
                                       std::string &queryStr) {
    // ATTENTION:
    // DO NOT concat content from dynamic params to SQL query string
    // otherwise SQL injection risk will be brought-in
    std::stringstream ssQuery;
    ssQuery << "SELECT ";
    bool isFirstField = true;
    for (const auto &column : _outputFieldsVisitor->getUsedFieldsColumnSet()) {
        auto name = column;
        KernelUtil::stripName(name);
        Ha3SqlExprGeneratorVisitor::wrapBacktick(name);
        ssQuery << (isFirstField ? "" : ", ") << name;
        isFirstField = false;
    }
    for (const auto &item : _outputFieldsVisitor->getUsedFieldsItemSet()) {
        auto fullName = Ha3SqlExprGeneratorVisitor::getItemFullName(item.first, item.second);
        ssQuery << (isFirstField ? "" : ", ") << fullName;
        isFirstField = false;
    }
    ssQuery << " FROM `" << queryParam.dbName << "`.`" << queryParam.tableName << "`";

    ssQuery << " WHERE 1=1";
    std::string conditionStr = _conditionVisitor->getConditionStr();
    std::string dynamicParamsStr = _conditionVisitor->getDynamicParamsStr();
    if (!conditionStr.empty()) {
        ssQuery << " AND " << conditionStr;
    }
    if (_extraQuery) {
        auto pkFieldName = _scanInitParamR->tableMeta.getPkIndexName();
        if (pkFieldName.empty()) {
            SQL_LOG(ERROR, "pkFieldName not found");
            return false;
        }
        Ha3SqlExprGeneratorVisitor::wrapBacktick(pkFieldName);
        ssQuery << " AND contain(" << pkFieldName << ", ?)";
        std::string param = autil::StringUtil::toString(_extraQuery->primaryKeys, "|");
        auto tempPool = _graphMemoryPoolR->getPool();
        autil::AutilPoolAllocator allocator(tempPool.get());
        autil::SimpleDocument document(&allocator);
        document.Parse(dynamicParamsStr.c_str());
        assert(!document.HasParseError());
        autil::SimpleValue val(param.c_str(), param.size(), allocator);
        document[0].PushBack(val, allocator);
        dynamicParamsStr = autil::RapidJsonHelper::SimpleValue2Str(document);
    }
    ssQuery << " LIMIT " << _limit;

    std::stringstream ssKvpair;
    ssKvpair << "formatType:flatbuffers;exec.source.spec:external;dynamic_params:"
             << dynamicParamsStr << ";databaseName:" << queryParam.dbName << ";"
             << SQL_GET_RESULT_COMPRESS_TYPE << ":lz4;timeout:" << queryParam.leftTimeMs;

    std::string queryClause = std::move(ssQuery).str();
    std::string kvpairClause = std::move(ssKvpair).str();
    std::stringstream ss;
    ss << "query=" << queryClause << "&&kvpair=" << kvpairClause;
    if (queryParam.enableAuth && !queryParam.authToken.empty() && !queryParam.authKey.empty()) {
        autil::legacy::Md5Stream stream;
        stream.Put((const uint8_t *)queryClause.c_str(), queryClause.size());
        stream.Put((const uint8_t *)kvpairClause.c_str(), kvpairClause.size());
        stream.Put((const uint8_t *)queryParam.authKey.c_str(), queryParam.authKey.size());
        ss << "&&authToken=" << queryParam.authToken << "&&authSignature=" << stream.GetMd5String();
    }
    queryStr = std::move(ss).str();
    return true;
}

bool Ha3SqlRemoteScanR::prepareQueryGenerateParam(QueryGenerateParam &queryParam) const {
    queryParam.serviceName = _tableConfig->serviceName;
    queryParam.dbName = _tableConfig->dbName;
    queryParam.tableName = _tableConfig->tableName;

    auto timeoutThresholdMs = _serviceConfig->timeoutThresholdMs;
    queryParam.leftTimeMs = autil::TimeUtility::us2ms(getTimeout() * 0.8);
    if (queryParam.leftTimeMs < timeoutThresholdMs) {
        SQL_LOG(ERROR,
                "left time[%ldms] less than table[%s] timeout threshold[%ldms]",
                queryParam.leftTimeMs,
                _scanInitParamR->tableName.c_str(),
                timeoutThresholdMs);
        return false;
    }
    if (_miscConfig->authEnabled) {
        queryParam.enableAuth = true;
        queryParam.authToken = _miscConfig->authToken;
        queryParam.authKey = _miscConfig->authKey;
    } else {
        queryParam.enableAuth = false;
    }

    SQL_LOG(TRACE3,
            "query param[%s], table[%s]",
            queryParam.toString().c_str(),
            _scanInitParamR->tableName.c_str());
    return true;
}

std::shared_ptr<multi_call::RequestGenerator> Ha3SqlRemoteScanR::constructRequestGenerator() {
    QueryGenerateParam queryParam;
    if (!prepareQueryGenerateParam(queryParam)) {
        SQL_LOG(ERROR,
                "parse table config to query param failed, table[%s]",
                _scanInitParamR->tableName.c_str());
        return nullptr;
    }
    Ha3SqlRequestGenerator::Ha3SqlParam params;
    if (!genQueryString(queryParam, params.assemblyQuery)) {
        SQL_LOG(ERROR, "generate query string failed");
        return nullptr;
    }
    params.timeoutInMs = getTimeout();
    params.bizName = queryParam.serviceName;
    SQL_LOG(TRACE3,
            "construct ha3 request assemblyQuery[%s] timeout[%ld]ms bizName[%s]",
            params.assemblyQuery.c_str(),
            params.timeoutInMs,
            params.bizName.c_str());
    return std::make_shared<Ha3SqlRequestGenerator>(params);
}

TablePtr Ha3SqlRemoteScanR::fillTableResult(
    const std::vector<std::shared_ptr<multi_call::Response>> &responseVec, bool &hasSoftFailure) {
    assert(responseVec.size() == 1 && "only get one response");
    auto response = responseVec[0];
    _scanInitParamR->updateExtraInfo(response->toString());
    if (response->isFailed()) {
        auto ec = response->getErrorCode();
        SQL_LOG(ERROR,
                "gig has error, error code[%s], errorMsg[%s], response[%s]",
                multi_call::translateErrorCode(ec),
                response->errorString().c_str(),
                response->toString().c_str());
        return nullptr;
    }
    auto *ha3Response = dynamic_cast<GigHa3SqlResponse *>(response.get());
    if (!ha3Response) {
        SQL_LOG(ERROR, "get ha3 response failed, invalid response type");
        return nullptr;
    }
    auto *qrsResponse = dynamic_cast<isearch::proto::QrsResponse *>(ha3Response->getMessage());
    if (!qrsResponse) {
        SQL_LOG(ERROR, "get qrs response failed, null pb response");
        return nullptr;
    }
    std::string sqlResult;
    if (!getDecompressedResult(qrsResponse, sqlResult)) {
        SQL_LOG(ERROR, "decompress qrs response failed");
        return nullptr;
    }
    SQL_LOG(TRACE3, "response result size after decompress[%lu]", sqlResult.size());
    flatbuffers::Verifier verifier((const uint8_t *)sqlResult.c_str(), sqlResult.size());
    if (!isearch::fbs::VerifySqlResultBuffer(verifier)) {
        SQL_LOG(ERROR, "verify flatbuffer format for sql result failed");
        return nullptr;
    }
    const isearch::fbs::SqlResult *fbsSqlResult
        = flatbuffers::GetRoot<isearch::fbs::SqlResult>(sqlResult.c_str());
    if (!fbsSqlResult) {
        SQL_LOG(ERROR, "get flatbuffer sql result from data failed");
        return nullptr;
    }
    ErrorResult errorResult;
    hasSoftFailure = false;
    TablePtr table = std::make_shared<table::Table>(_graphMemoryPoolR->getPool());
    if (!SqlResultUtil().FromFBSqlResult(fbsSqlResult, errorResult, hasSoftFailure, table)) {
        SQL_LOG(ERROR, "from fb sql result failed");
        return nullptr;
    }
    if (errorResult.hasError()) {
        SQL_LOG(
            ERROR, "get result from external failed, err[%s]", errorResult.toJsonString().c_str());
        return nullptr;
    }
    if (hasSoftFailure) {
        SQL_LOG(WARN, "get result from external has soft failure");
    }
    return table;
}

bool Ha3SqlRemoteScanR::getDecompressedResult(isearch::proto::QrsResponse *response,
                                              std::string &decompressedResult) {
    const auto &sqlResult = response->assemblyresult();
    SQL_LOG(TRACE3, "response result size before decompress[%lu]", sqlResult.size());
    auto compressType = (autil::CompressType)(response->compresstype());
    auto tempPool = _graphMemoryPoolR->getPool();
    if (!autil::CompressionUtil::decompress(
            sqlResult, compressType, decompressedResult, tempPool.get())) {
        SQL_LOG(ERROR,
                "decompress result failed, compressType[%d], result size[%lu]",
                (int)compressType,
                sqlResult.size());
        return false;
    }
    return true;
}

int64_t Ha3SqlRemoteScanR::getTimeout() const {
    return _timeoutTerminatorR->getTimeoutTerminator()->getLeftTime();
}

REGISTER_RESOURCE(Ha3SqlRemoteScanR);

} // namespace sql
