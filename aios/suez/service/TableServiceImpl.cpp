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
#include "suez/service/TableServiceImpl.h"

#include <functional>
#include <memory>
#include <tuple>
#include <utility>

#include "autil/ClosureGuard.h"
#include "autil/EnvUtil.h"
#include "autil/RangeUtil.h"
#include "autil/Scope.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/base64.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/legacy/tools/partition_querier/executors/IndexTableExecutor.h"
#include "indexlib/legacy/tools/partition_querier/executors/KkvTableExecutor.h"
#include "indexlib/legacy/tools/partition_querier/executors/KvTableExecutor.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/util/ProtoJsonizer.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "suez/sdk/RpcServer.h"
#include "suez/sdk/TableWriter.h"
#include "suez/service/KVTableSearcher.h"
#include "suez/service/SchemaUtil.h"
#include "suez/service/TableServiceHelper.h"
#include "suez/service/WriteDone.h"
#include "suez/service/WriteTableAccessLog.h"


using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::config;
using namespace kmonitor;

using indexlib::Status;
using indexlib::partition::IndexPartitionPtr;
using indexlib::tools::IndexTableExecutor;
using indexlib::tools::KkvTableExecutor;
using indexlib::tools::KvTableExecutor;
using indexlibv2::base::PartitionQuery;
using indexlibv2::base::PartitionResponse;

namespace suez {

constexpr char TABLE_SERVICE_TOPO_NAME[] = "table_service";
constexpr char ZONE_BIZ_NAME_SPLITTER[] = ".";

AUTIL_LOG_SETUP(suez, TableServiceImpl);

TableServiceImpl::TableServiceImpl() {}

TableServiceImpl::~TableServiceImpl() {}

bool TableServiceImpl::init(const SearchInitParam &initParam) {
    const auto &kmonMetaInfo = initParam.kmonMetaInfo;
    _metricsReporter.reset(new kmonitor::MetricsReporter(kmonMetaInfo.metricsPrefix, "", {}, "table_service"));
    _executor = initParam.asyncIntraExecutor;
    _rpcServer = initParam.rpcServer;
    _enablePublishTopoInfo = autil::EnvUtil::getEnv("ENABLE_PUBLISH_TABLE_TOPO_INFO", false);
    return initParam.rpcServer->RegisterService(this);
}

UPDATE_RESULT TableServiceImpl::update(const suez::UpdateArgs &updateArgs,
                                       UpdateIndications &updateIndications,
                                       SuezErrorCollector &errorCollector) {

    bool enableUpdateTopoInfo = _enablePublishTopoInfo && needUpdateTopoInfo(updateArgs);
    setIndexProvider(updateArgs.indexProvider);
    if (enableUpdateTopoInfo && !updateTopoInfo(updateArgs)) {
        return UR_NEED_RETRY;
    }
    return UR_REACH_TARGET;
}

void TableServiceImpl::stopService() {
    AUTIL_LOG(INFO, "stop table service.");
    setIndexProvider(nullptr);
}

void TableServiceImpl::stopWorker() {
    AUTIL_LOG(INFO, "stop worker.");
    stopService();
}

bool TableServiceImpl::needUpdateTopoInfo(const UpdateArgs &updateArgs) const {
    assert(updateArgs.indexProvider);
    if (!_indexProvider) {
        return true;
    }
    if (*_indexProvider == *(updateArgs.indexProvider)) {
        return false;
    } else {
        return true;
    }
}

static regionid_t getRegionId(indexlibv2::config::ITabletSchema *schema, const string &regionName) {
    if (regionName.empty()) {
        return DEFAULT_REGIONID;
    }
    auto legacySchemaAdapter = dynamic_cast<indexlib::config::LegacySchemaAdapter *>(schema);
    if (!legacySchemaAdapter) {
        return DEFAULT_REGIONID;
    }
    return legacySchemaAdapter->GetLegacySchema()->GetRegionId(regionName);
}

#define COMMON_CHECK(tablename_func)                                                                                   \
    TableServiceHelper::trySetGigEc(done, multi_call::MULTI_CALL_ERROR_NONE);                                          \
    ClosureGuard guard(done);                                                                                          \
    auto indexProvider = getIndexProvider();                                                                           \
    if (!indexProvider) {                                                                                              \
        ERROR_THEN_RETURN(done,                                                                                        \
                          TBS_ERROR_SERVICE_NOT_READY,                                                                 \
                          "service not ready",                                                                         \
                          kQueryTable,                                                                                 \
                          kUnknownTable,                                                                               \
                          _metricsReporter.get());                                                                     \
    }                                                                                                                  \
    if (!request->has_##tablename_func() || request->tablename_func().empty()) {                                       \
        ERROR_THEN_RETURN(                                                                                             \
            done, TBS_ERROR_NO_TABLE, "no table specified.", kQueryTable, kUnknownTable, _metricsReporter.get());      \
    }

#define ERROR_THEN_RETURN(done, error_code, msg, queryType, tableName, metricsReporter)                                \
    do {                                                                                                               \
        TableServiceHelper::trySetGigEc(done, multi_call::MULTI_CALL_REPLY_ERROR_RESPONSE);                            \
        TableServiceHelper::setErrorInfo(                                                                              \
            response->mutable_errorinfo(), done, error_code, msg, queryType, tableName, metricsReporter);              \
        return;                                                                                                        \
    } while (false)

void TableServiceImpl::querySchema(google::protobuf::RpcController *controller,
                                   const SchemaQueryRequest *request,
                                   SchemaQueryResponse *response,
                                   google::protobuf::Closure *done) {
    int64_t startTime = TimeUtility::currentTime();
    COMMON_CHECK(tablename);
    const auto &tableName = request->tablename();

    std::shared_ptr<indexlibv2::config::ITabletSchema> schema;
    auto tablets = indexProvider->multiTableReader.getTablets(tableName);
    if (!tablets.empty()) {
        auto firstTablet = tablets[0];
        schema = firstTablet->GetTabletSchema();
    } else {
        auto indexPartitions = indexProvider->multiTableReader.getIndexPartitions(tableName);
        if (indexPartitions.empty()) {
            ERROR_THEN_RETURN(done,
                              TBS_ERROR_NO_TABLE,
                              "can't get index partition of table",
                              kQuerySchema,
                              tableName,
                              _metricsReporter.get());
        }

        auto indexPartitionPtr = indexPartitions.begin()->second;
        schema = std::make_shared<indexlib::config::LegacySchemaAdapter>(indexPartitionPtr->GetSchema());
    }

    ErrorInfo errInfo;
    ResultSchema resultSchema;
    if (request->has_jsonschema() && request->jsonschema() == true) {
        errInfo = SchemaUtil::getJsonSchema(schema, resultSchema);
    } else {
        errInfo = SchemaUtil::getPbSchema(schema, "", resultSchema);
    }
    *(response->mutable_res()) = resultSchema;
    if (errInfo.errorcode() != TBS_ERROR_NONE) {
        *(response->mutable_errorinfo()) = errInfo;
    }

    if (nullptr != _metricsReporter) {
        MetricsTags baseTags = {{{kQueryType, kQuerySchema}, {kTableName, tableName}}};
        int64_t latency = TimeUtility::currentTime() - startTime;
        _metricsReporter->report(latency, kLatency, kmonitor::GAUGE, &baseTags);
        _metricsReporter->report(1, kQps, kmonitor::QPS, &baseTags);
    }
}

void TableServiceImpl::queryVersion(google::protobuf::RpcController *controller,
                                    const VersionQueryRequest *request,
                                    VersionQueryResponse *response,
                                    google::protobuf::Closure *done) {
    COMMON_CHECK(tablename);
    const auto &tableName = request->tablename();
    SingleTableReaderPtr tableReader;

    if (request->partition().size() == 2) {
        RangeType from = request->partition()[0];
        RangeType to = request->partition()[1];
        tableReader = indexProvider->multiTableReader.getTableReaderByRange(from, to, tableName);
        if (tableReader == nullptr) {
            ERROR_THEN_RETURN(done, TBS_ERROR_NO_TABLE, "not exist", kQueryVersion, tableName, _metricsReporter.get());
        }
    } else {
        AUTIL_LOG(INFO, "query version use first table reader");

        auto tableReaders = indexProvider->multiTableReader.getTableReaders(tableName);
        if (tableReaders.empty()) {
            ERROR_THEN_RETURN(done, TBS_ERROR_NO_TABLE, "not exist", kQueryVersion, tableName, _metricsReporter.get());
        }
        tableReader = tableReaders.begin()->second;
    }
    auto indexPartition = tableReader->getIndexPartition();
    if (indexPartition) {
        auto version = indexPartition->GetReader()->GetVersion();
        response->set_publishversioninfo(version.ToString());
    } else {
        auto tablet = tableReader->getTablet();
        if (!tablet) {
            ERROR_THEN_RETURN(done,
                              TBS_ERROR_NOT_SUPPORTED,
                              "table type unsupported",
                              kQueryVersion,
                              tableName,
                              _metricsReporter.get());
        } else {
            auto tabletInfo = tablet->GetTabletInfos();
            response->set_publishversioninfo(tabletInfo->GetLoadedPublishVersion().ToString());
            response->set_privateversioninfo(tabletInfo->GetLoadedPrivateVersion().ToString());
            response->set_doccount(tabletInfo->GetTabletDocCount());
            auto buildLocator = tabletInfo->GetBuildLocator();
            auto *buildLocatorProto = response->mutable_buildlocator();
            buildLocatorProto->set_src(buildLocator.GetSrc());
            buildLocatorProto->set_offset(buildLocator.GetOffset().first);
            auto *commitLocatorProto = response->mutable_lastcommittedlocator();
            auto commitLocator = tabletInfo->GetBuildLocator();
            commitLocatorProto->set_src(commitLocator.GetSrc());
            commitLocatorProto->set_offset(commitLocator.GetOffset().first);
        }
    }
}

void TableServiceImpl::queryTable(google::protobuf::RpcController *controller,
                                  const TableQueryRequest *request,
                                  TableQueryResponse *response,
                                  google::protobuf::Closure *done) {
    int64_t startTime = TimeUtility::currentTime();
    COMMON_CHECK(table);
    const string &tableName = request->table();

    SingleTableReaderPtr singleTableReader;
    if (request->partition().empty()) {
        SingleTableReaderMap readerMap = indexProvider->multiTableReader.getTableReaders(tableName);
        if (readerMap.empty()) {
            ERROR_THEN_RETURN(done,
                              TBS_ERROR_NO_TABLE,
                              "table[" + tableName + "] not found",
                              kQueryTable,
                              tableName,
                              _metricsReporter.get());
        }
        auto pid = readerMap.begin()->first;
        singleTableReader = readerMap.begin()->second;
        if (readerMap.size() > 1) {
            AUTIL_LOG(WARN,
                      "table[%s] has [%zu] partitions, but no partition specified, use first partition[%d,%d]",
                      tableName.c_str(),
                      readerMap.size(),
                      pid.from,
                      pid.to);
        }
    } else if (2 == request->partition().size()) {
        RangeType from = request->partition()[0];
        RangeType to = request->partition()[1];
        singleTableReader = indexProvider->multiTableReader.getTableReaderByRange(from, to, tableName);
    } else {
        ERROR_THEN_RETURN(
            done, TBS_ERROR_NO_TABLE, "invalid partitions", kQueryTable, tableName, _metricsReporter.get());
    }

    if (nullptr == singleTableReader) {
        ERROR_THEN_RETURN(
            done, TBS_ERROR_NO_TABLE, "cannot find table", kQueryTable, tableName, _metricsReporter.get());
    }

    const auto query = convertRequestToPartitionQuery(request);

    PartitionResponse partitionResponse;
    indexlib::Status status;
    std::shared_ptr<indexlibv2::config::ITabletSchema> schema;

    auto tablet = singleTableReader->getTablet();
    auto indexPartition = singleTableReader->getIndexPartition();
    if (nullptr != tablet) {
        status = queryTablet(tablet, query, partitionResponse, schema);
    } else {
        status = queryIndexPartition(indexPartition, request, query, partitionResponse, schema);
    }

    ErrorInfo errorInfo = convertStatusToErrorInfo(status);
    if (errorInfo.errorcode() != TBS_ERROR_NONE) {
        AUTIL_LOG(ERROR, "SuezTableService Query Failed. status msg: %s", errorInfo.errormsg().c_str());
        ERROR_THEN_RETURN(
            done, errorInfo.errorcode(), errorInfo.errormsg(), kQueryTable, tableName, _metricsReporter.get());
    }
    convertPartitionResponseToResponse(request, partitionResponse, response);
    auto &tableQueryResult = *response->mutable_res();
    tableQueryResult.set_tablename(tableName);
    if (request->has_showschema() && request->showschema()) {
        ResultSchema resultSchema;
        SchemaUtil::getPbSchema(schema, "", resultSchema);
        *(tableQueryResult.mutable_resultschema()) = resultSchema;
    }
    if (tableQueryResult.docids().empty() && tableQueryResult.docvalueset().empty()) {
        ERROR_THEN_RETURN(
            done, TBS_ERROR_NO_RECORD, "no record found.", kQueryTable, tableName, _metricsReporter.get());
    }
    if (!tableQueryResult.docvalueset().empty()) {
        tableQueryResult.set_resultcount(tableQueryResult.docvalueset_size());
    } else {
        tableQueryResult.set_resultcount(tableQueryResult.docids_size());
    }

    if (nullptr != _metricsReporter) {
        MetricsTags baseTags = {{{kQueryType, kQueryTable}, {kTableName, tableName}}};
        int64_t latency = TimeUtility::currentTime() - startTime;
        _metricsReporter->report(latency, kLatency, kmonitor::GAUGE, &baseTags);
        _metricsReporter->report(1, kQps, kmonitor::QPS, &baseTags);
        _metricsReporter->report(tableQueryResult.resultcount(), kDocCount, kmonitor::GAUGE, &baseTags);
    }
}

void TableServiceImpl::simpleQuery(google::protobuf::RpcController *controller,
                                   const SimpleQueryRequest *request,
                                   SimpleQueryResponse *response,
                                   google::protobuf::Closure *done) {
    int64_t startTime = TimeUtility::currentTime();
    COMMON_CHECK(table);
    const string &tableName = request->table();
    SingleTableReaderPtr singleTableReader;
    if (request->partition().empty()) {
        SingleTableReaderMap readerMap = indexProvider->multiTableReader.getTableReaders(tableName);
        if (readerMap.empty()) {
            ERROR_THEN_RETURN(done,
                              TBS_ERROR_NO_TABLE,
                              "table[" + tableName + "] not found",
                              kQueryTable,
                              tableName,
                              _metricsReporter.get());
        }
        auto pid = readerMap.begin()->first;
        singleTableReader = readerMap.begin()->second;
        if (readerMap.size() > 1) {
            AUTIL_LOG(WARN,
                      "table[%s] has [%zu] partitions, but no partition specified, use first partition[%d,%d]",
                      tableName.c_str(),
                      readerMap.size(),
                      pid.from,
                      pid.to);
        }
    } else if (2 == request->partition().size()) {
        RangeType from = request->partition()[0];
        RangeType to = request->partition()[1];
        singleTableReader = indexProvider->multiTableReader.getTableReaderByRange(from, to, tableName);
    } else {
        ERROR_THEN_RETURN(
            done, TBS_ERROR_INVALID_PARAMETER, "invalid partitions", kQueryTable, tableName, _metricsReporter.get());
    }
    if (nullptr == singleTableReader) {
        ERROR_THEN_RETURN(
            done, TBS_ERROR_NO_TABLE, "cannot find table", kQueryTable, tableName, _metricsReporter.get());
    }
    auto tablet = singleTableReader->getTablet();
    if (tablet == nullptr) {
        ERROR_THEN_RETURN(done, TBS_ERROR_NO_TABLE, "tablet is null", kQueryTable, tableName, _metricsReporter.get());
    }
    auto tabletReader = tablet->GetTabletReader();
    std::string query = std::string(request->request().data(), request->request().size());
    std::string result;
    auto status = tabletReader->Search(query, result);
    if (!status.IsOK()) {
        ERROR_THEN_RETURN(
            done, TBS_ERROR_GET_FAILED, "simple query failed", kQueryTable, tableName, _metricsReporter.get());
    }
    response->mutable_errorinfo()->set_errorcode(TBS_ERROR_NONE);
    response->mutable_errorinfo()->set_errormsg("");
    response->set_response(std::move(result));
    if (nullptr != _metricsReporter) {
        MetricsTags baseTags = {{{kQueryType, kQueryTable}, {kTableName, tableName}}};
        int64_t latency = TimeUtility::currentTime() - startTime;
        _metricsReporter->report(latency, kLatency, kmonitor::GAUGE, &baseTags);
        _metricsReporter->report(1, kQps, kmonitor::QPS, &baseTags);
    }
}

Status TableServiceImpl::queryTablet(shared_ptr<indexlibv2::framework::ITablet> tablet,
                                     const PartitionQuery &query,
                                     PartitionResponse &partitionResponse,
                                     std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
    std::string queryStr;
    RETURN_STATUS_DIRECTLY_IF_ERROR(indexlib::util::ProtoJsonizer::ToJson(query, &queryStr));
    schema = tablet->GetTabletSchema();
    auto tabletReader = tablet->GetTabletReader();
    if (!tabletReader) {
        return Status::InternalError("tablet reader is nullptr");
    }
    std::string result;
    const std::string tableType = schema->GetTableType();
    if (tableType == indexlib::table::TABLE_TYPE_NORMAL || tableType == indexlib::table::TABLE_TYPE_KV ||
        tableType == indexlib::table::TABLE_TYPE_KKV
    ) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(tabletReader->Search(queryStr, result));
        RETURN_STATUS_DIRECTLY_IF_ERROR(indexlib::util::ProtoJsonizer::FromJson(result, &partitionResponse));
        return Status::OK();
    }
    return Status::InvalidArgs("table schema type [%s] not supported", tableType.c_str());
}

Status TableServiceImpl::queryIndexPartition(IndexPartitionPtr indexPartitionPtr,
                                             const TableQueryRequest *request,
                                             const PartitionQuery &query,
                                             PartitionResponse &partitionResponse,
                                             std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
    auto legacySchema = indexPartitionPtr->GetSchema();
    assert(legacySchema);
    schema = std::make_shared<indexlib::config::LegacySchemaAdapter>(legacySchema);

    Status status;
    const auto reader = indexPartitionPtr->GetReader();
    const auto &tableType = schema->GetTableType();
    if (tableType == indexlib::table::TABLE_TYPE_NORMAL) {
        status = IndexTableExecutor::QueryIndex(reader, query, partitionResponse);
    } else if (tableType == indexlib::table::TABLE_TYPE_KV) {
        status = KvTableExecutor::QueryKVTable(
            reader, getRegionId(schema.get(), request->region()), query, partitionResponse);
    } else if (tableType == indexlib::table::TABLE_TYPE_KKV) {
        status = KkvTableExecutor::QueryKkvTable(
            reader, getRegionId(schema.get(), request->region()), query, partitionResponse);
    } else {
        status = Status::InvalidArgs("table schema type [%d] not supported", schema->GetTableType().c_str());
    }
    return status;
}

void TableServiceImpl::writeTable(google::protobuf::RpcController *controller,
                                  const WriteRequest *request,
                                  WriteResponse *response,
                                  google::protobuf::Closure *done) {
    auto accessLog = make_shared<WriteTableAccessLog>();
    accessLog->collectRequest(request);
    accessLog->collectClosure(done);
    accessLog->collectRPCController(controller);
    int64_t startTime = TimeUtility::currentTime();
    TableServiceHelper::trySetGigEc(done, multi_call::MULTI_CALL_ERROR_NONE);

    ScopeGuard scopeGuard([response, accessLog, done]() {
        accessLog->collectResponse(response);
        done->Run();
    });

    auto indexProvider = getIndexProvider();
    if (!indexProvider) {
        ERROR_THEN_RETURN(done,
                          TBS_ERROR_SERVICE_NOT_READY,
                          "service not ready",
                          kWrite,
                          request->tablename(),
                          _metricsReporter.get());
    }
    map<shared_ptr<TableWriter>, vector<pair<uint16_t, string>>> useTableWriters;
    map<shared_ptr<TableWriter>, vector<int>> tableWriterToDocIndex;
    for (int i = 0; i < request->writes().size(); i++) {
        auto write = request->writes()[i];
        bool found = false;
        for (const auto &[pid, tableWriter] : indexProvider->tableWriters) {
            if (pid.getTableName() != request->tablename() || !pid.covers(write.hashid())) {
                continue;
            }
            useTableWriters[tableWriter].emplace_back(make_pair(write.hashid(), write.str()));
            tableWriterToDocIndex[tableWriter].emplace_back(i);
            found = true;
            break;
        }
        if (!found) {
            ERROR_THEN_RETURN(done,
                              TBS_ERROR_OTHERS,
                              "no valid table/range for WriteRequest table: " + request->tablename() +
                                  " hashid: " + to_string(write.hashid()),
                              kWrite,
                              request->tablename(),
                              _metricsReporter.get());
        }
    }
    scopeGuard.release();

    auto singleWriteDone = make_shared<WriteDone>(request,
                                                  response,
                                                  done,
                                                  _metricsReporter.get(),
                                                  accessLog,
                                                  startTime,
                                                  std::move(indexProvider),
                                                  request->writes().size());

    for (const auto &useTableWriter : useTableWriters) {
        auto docIndexList = tableWriterToDocIndex[useTableWriter.first];
        auto currentWriteDone = [singleWriteDone, docIndexList](Result<WriteResult> result) {
            singleWriteDone->run(std::move(result), docIndexList);
        };

        useTableWriter.first->write(request->format(), useTableWriter.second, currentWriteDone);
    }
}

void TableServiceImpl::updateSchema(google::protobuf::RpcController *controller,
                                    const UpdateSchemaRequest *request,
                                    UpdateSchemaResponse *response,
                                    google::protobuf::Closure *done) {
    TableServiceHelper::trySetGigEc(done, multi_call::MULTI_CALL_ERROR_NONE);
    ClosureGuard guard(done);

    AUTIL_LOG(INFO, "update schema for table [%s], part[%d]", request->tablename().c_str(), request->partids(0));

    if (request->partids().size() != 1) {
        ERROR_THEN_RETURN(done,
                          TBS_ERROR_UPDATE_SCHEMA,
                          "only support update one part",
                          kUpdateSchema,
                          request->tablename(),
                          _metricsReporter.get());
    }

    auto indexProvider = getIndexProvider();
    if (!indexProvider) {
        ERROR_THEN_RETURN(done,
                          TBS_ERROR_SERVICE_NOT_READY,
                          "service not ready",
                          kUpdateSchema,
                          request->tablename(),
                          _metricsReporter.get());
    }

    const auto &partId = request->partids(0);

    std::shared_ptr<TableWriter> tableWriter;
    string configPath;
    bool found = false;
    for (const auto &iter : indexProvider->tableWriters) {
        if (iter.first.getTableName() != request->tablename()) {
            continue;
        }
        // TODO: admin set index
        if (iter.first.index == partId) {
            tableWriter = iter.second;
            configPath = request->configpath();
            found = true;
            break;
        }
    }

    if (!found) {
        ERROR_THEN_RETURN(done,
                          TBS_ERROR_UPDATE_SCHEMA,
                          "no valid table/part for update schema table: " + request->tablename() +
                              " partid: " + to_string(partId),
                          kUpdateSchema,
                          request->tablename(),
                          _metricsReporter.get());
    }

    auto updateDone = [this, done = guard.steal(), request, response](autil::Result<int64_t> result) {
        if (!result.is_ok()) {
            AUTIL_LOG(ERROR, "update schema failed, error msg: %s", result.get_error().message().c_str());
            TableServiceHelper::trySetGigEc(done, multi_call::MULTI_CALL_REPLY_ERROR_RESPONSE);
            TableServiceHelper::setErrorInfo(response->mutable_errorinfo(),
                                             done,
                                             TBS_ERROR_UPDATE_SCHEMA,
                                             "update schema failed, error msg: " + result.get_error().message(),
                                             kUpdateSchema,
                                             request->tablename(),
                                             _metricsReporter.get());
        }
        done->Run();
    };

    tableWriter->updateSchema(request->schemaversion(), configPath, std::move(updateDone));
}

void TableServiceImpl::getSchemaVersion(google::protobuf::RpcController *controller,
                                        const GetSchemaVersionRequest *request,
                                        GetSchemaVersionResponse *response,
                                        google::protobuf::Closure *done) {
    TableServiceHelper::trySetGigEc(done, multi_call::MULTI_CALL_ERROR_NONE);
    ClosureGuard guard(done);

    auto indexProvider = getIndexProvider();
    if (!indexProvider) {
        ERROR_THEN_RETURN(done,
                          TBS_ERROR_SERVICE_NOT_READY,
                          "service not ready",
                          kGetSchemaVersion,
                          request->tablename(),
                          _metricsReporter.get());
    }

    if (request->partids().size() != 1) {
        ERROR_THEN_RETURN(done,
                          TBS_ERROR_GET_SCHEMA_VERSION,
                          "only support get one part",
                          kGetSchemaVersion,
                          request->tablename(),
                          _metricsReporter.get());
    }

    const auto &partId = request->partids(0);
    const auto &tableName = request->tablename();
    if (tableName.empty()) {
        ERROR_THEN_RETURN(
            done, TBS_ERROR_NO_TABLE, "no table specified", kGetSchemaVersion, tableName, _metricsReporter.get());
    }

    auto tablet = indexProvider->multiTableReader.getTabletByIdx(tableName, partId);
    if (tablet == nullptr) {
        ERROR_THEN_RETURN(
            done, TBS_ERROR_NO_TABLE, "tablet not found", kGetSchemaVersion, tableName, _metricsReporter.get());
    }

    auto tabletReader = tablet->GetTabletReader();
    if (!tabletReader) {
        ERROR_THEN_RETURN(
            done, TBS_ERROR_OTHERS, "fail to get tablet reader", kGetSchemaVersion, tableName, _metricsReporter.get());
    }
    auto tabletSchema = tabletReader->GetSchema();
    if (!tabletSchema) {
        ERROR_THEN_RETURN(
            done, TBS_ERROR_OTHERS, "fail to get schema", kGetSchemaVersion, tableName, _metricsReporter.get());
    }

    auto schemaId = tabletSchema->GetSchemaId();
    auto schemaVersion = response->add_schemaversions();
    schemaVersion->set_partid(partId);
    schemaVersion->set_currentversionid(schemaId);
}
#define IF_NOT_OK_THEN_RETURN(IS_OK, MSG)                                                                              \
    do {                                                                                                               \
        if (unlikely(!(IS_OK))) {                                                                                      \
            ERROR_THEN_RETURN(done, TBS_ERROR_OTHERS, MSG, kKvBatchGet, tableName, _metricsReporter.get());            \
        }                                                                                                              \
    } while (0)

void TableServiceImpl::kvBatchGet(google::protobuf::RpcController *controller,
                                  const KVBatchGetRequest *request,
                                  KVBatchGetResponse *response,
                                  google::protobuf::Closure *done) {
    COMMON_CHECK(tablename);
    response->set_notfoundcount(0);
    response->set_failedcount(0);
    int64_t startTime = TimeUtility::currentTime();

    const string &tableName = request->tablename();
    string indexName = request->indexname();
    std::vector<std::string> attrs;
    for (size_t i = 0; i < request->attrs_size(); i++) {
        attrs.emplace_back(request->attrs(i));
    }
    IF_NOT_OK_THEN_RETURN(_executor, "batch search need executor");
    IF_NOT_OK_THEN_RETURN(request->inputkeys_size(), "input keys are empty ");
    // 构造searcher
    autil::mem_pool::Pool pool(1024);
    std::unique_ptr<LookupOptions> options =
        std::make_unique<LookupOptions>(attrs, request->timeoutinus(), _executor, &pool);

    auto searcher = std::make_shared<KVTableSearcher>(&(indexProvider->multiTableReader));
    auto result = searcher->init(tableName, indexName);
    IF_NOT_OK_THEN_RETURN(result.is_ok(), "kv table searcher init failed, msg:" + result.get_error().message());

    // 查询
    if (request->resulttype() == KVBatchGetResultType::DEBUG) {
        // debug 接口
        KVTableSearcher::InputKeys inputKeys;
        for (auto i = 0; i < request->inputkeys_size(); i++) {
            auto inputKey = request->inputkeys(i);
            inputKeys.emplace_back(
                make_pair<int, vector<string>>(inputKey.partid(), {inputKey.keys().begin(), inputKey.keys().end()}));
        }
        auto result = searcher->query(inputKeys, options.get());
        IF_NOT_OK_THEN_RETURN(result.is_ok(), result.get_error().message());
        auto values = result.get();
        *response->mutable_values() = {values.begin(), values.end()};
    } else {
        // bytes和table
        // 构造inputHashKeys
        KVTableSearcher::InputHashKeys inputHashKeys;
        for (auto i = 0; i < request->inputkeys_size(); i++) {
            auto inputKey = request->inputkeys(i);
            auto partId = inputKey.partid();
            if (request->usehashkey()) {
                auto hashKeys = inputKey.hashkeys();
                inputHashKeys.emplace_back(
                    make_pair<int, vector<uint64_t>>(partId, {hashKeys.begin(), hashKeys.end()}));
            } else {
                auto hashKeys = searcher->genHashKeys(partId, {inputKey.keys().begin(), inputKey.keys().end()});
                IF_NOT_OK_THEN_RETURN(hashKeys.is_ok(), hashKeys.get_error().message());
                inputHashKeys.emplace_back(make_pair((int)partId, hashKeys.get()));
            }
        }

        auto lookupResult = searcher->lookup(inputHashKeys, options.get());
        IF_NOT_OK_THEN_RETURN(lookupResult.is_ok(), "look up failed, errormsg: " + lookupResult.get_error().message());
        if (request->resulttype() == KVBatchGetResultType::FLATBYTES) {
            if (lookupResult.get().foundValues.size()) {
                int index = 0;
                auto valueSize = lookupResult.get().foundValues.begin()->second.size();
                std::string *buffers = response->mutable_binvalues();
                buffers->resize(valueSize * lookupResult.get().foundValues.size());
                for (auto [hashKey, value] : lookupResult.get().foundValues) {
                    response->add_foundkeys(hashKey);
                    if (unlikely(value.size() != valueSize)) {
                        IF_NOT_OK_THEN_RETURN(value.size() == valueSize, "look up failed, dimension not consistant.");
                    }
                    memcpy(buffers->data() + index, value.data(), valueSize);
                    index += valueSize;
                }
            } else {
                response->set_binvalues("");
            }
        } else if (request->resulttype() == KVBatchGetResultType::BYTES) {
            for (auto [hashKey, value] : lookupResult.get().foundValues) {
                response->add_foundkeys(hashKey);
                response->add_values(value.data(), value.size());
            }
        } else if (request->resulttype() == KVBatchGetResultType::TABLE) {
            std::vector<autil::StringView> values;
            for (auto [hashKey, value] : lookupResult.get().foundValues) {
                values.emplace_back(value);
            }
            auto inputKey = request->inputkeys(0);
            auto partId = inputKey.partid();
            auto valueStr = searcher->convertResult(partId, values, options.get());
            IF_NOT_OK_THEN_RETURN(valueStr.is_ok(), valueStr.get_error().message());
            response->set_binvalues(valueStr.get().data(), valueStr.get().size());
        }
        response->set_failedcount(lookupResult.get().failedCount);
        response->set_notfoundcount(lookupResult.get().notFoundCount);
    }
    if (_metricsReporter) {
        MetricsTags baseTags = {{{kQueryType, kKvBatchGet}, {kTableName, tableName}}};
        int64_t latency = TimeUtility::currentTime() - startTime;
        _metricsReporter->report(latency, kLatency, kmonitor::GAUGE, &baseTags);
        _metricsReporter->report(1, kQps, kmonitor::QPS, &baseTags);
        searcher->reportMetrics(_metricsReporter.get());
    } else {
        AUTIL_LOG(WARN, "cannot find metric reporter")
    }
}

void TableServiceImpl::health_check(google::protobuf::RpcController *controller,
                                    const HealthCheckRequest *request,
                                    HealthCheckResponse *response,
                                    google::protobuf::Closure *done) {
    TableServiceHelper::trySetGigEc(done, multi_call::MULTI_CALL_ERROR_NONE);

    ClosureGuard guard(done);
    auto indexProvider = getIndexProvider();
    response->set_serviceready(indexProvider.get() != nullptr);
}

void TableServiceImpl::setIndexProvider(const shared_ptr<IndexProvider> &provider) {
    ScopedWriteLock lock(_lock);
    _indexProvider = provider;
}

shared_ptr<IndexProvider> TableServiceImpl::getIndexProvider() const {
    ScopedReadLock lock(_lock);
    return _indexProvider;
}

PartitionQuery TableServiceImpl::convertRequestToPartitionQuery(const TableQueryRequest *const request) {
    PartitionQuery partitionQuery;
    if (request->has_attr()) {
        partitionQuery.add_attrs(request->attr());
    }
    if (request->has_pk()) {
        partitionQuery.add_pk(request->pk());
    }
    if (request->has_docid()) {
        partitionQuery.add_docid(request->docid());
    }
    if (request->has_region()) {
        partitionQuery.set_region(request->region());
    }
    if (request->has_indexname()) {
        partitionQuery.mutable_condition()->set_indexname(request->indexname());
        if (request->has_indexvalue()) {
            partitionQuery.mutable_condition()->add_values(request->indexvalue());
        }
    }
    if (request->has_limit()) {
        partitionQuery.set_limit(request->limit());
    }
    if (!request->sk().empty()) {
        partitionQuery.add_skey(request->sk());
    }
    if (request->has_pknumber()) {
        partitionQuery.add_pknumber(request->pknumber());
    }
    if (request->has_ignoredeletionmap()) {
        partitionQuery.set_ignoredeletionmap(request->ignoredeletionmap());
    } else {
        partitionQuery.set_ignoredeletionmap(false);
    }
    if (request->has_needsectioninfo()) {
        partitionQuery.set_needsectioninfo(request->needsectioninfo());
    } else {
        partitionQuery.set_needsectioninfo(false);
    }
    for (int i = 0; i < request->summarys_size(); ++i) {
        *partitionQuery.add_summarys() = request->summarys(i);
    }
    return partitionQuery;
}

void TableServiceImpl::convertPartitionResponseToResponse(const TableQueryRequest *const request,
                                                          const PartitionResponse &partitionResponse,
                                                          TableQueryResponse *const response) {

    auto tableQueryResult = response->mutable_res();
    tableQueryResult->set_resultcount(partitionResponse.rows_size());
    const bool hasDocId = partitionResponse.rows_size() > 0 ? partitionResponse.rows(0).has_docid() : false;
    const auto &attrInfo = partitionResponse.attrinfo();
    tableQueryResult->clear_docids();
    for (int64_t i = 0; i < partitionResponse.rows_size(); ++i) {
        const auto &row = partitionResponse.rows(i);
        auto &docValue = *tableQueryResult->add_docvalueset();
        if (hasDocId) {
            int64_t docId = row.docid();
            docValue.set_docid(docId);
            tableQueryResult->add_docids(docId);
        }
        docValue.clear_attrvalue();
        for (int j = 0; j < attrInfo.fields_size(); ++j) {
            auto &singleAttrValue = *docValue.add_attrvalue();
            singleAttrValue.set_attrname(attrInfo.fields(j).attrname());
            setSingleAttr<TableQueryRequest>(request, singleAttrValue, row.attrvalues(j));
        }
        for (int k = 0; k < row.summaryvalues_size(); ++k) {
            const auto &srcValue = row.summaryvalues(k);
            auto targetValue = docValue.add_summaryvalues();
            targetValue->set_fieldname(srcValue.fieldname());
            targetValue->set_value(srcValue.value());
        }
    }

    if (partitionResponse.has_termmeta()) {
        tableQueryResult->mutable_indextermmeta()->set_docfreq(partitionResponse.termmeta().docfreq());
        tableQueryResult->mutable_indextermmeta()->set_totaltermfreq(partitionResponse.termmeta().totaltermfreq());
        tableQueryResult->mutable_indextermmeta()->set_payload(partitionResponse.termmeta().payload());
    }

    for (int i = 0; i < partitionResponse.sectionmetas_size(); ++i) {
        auto sectionMeta = tableQueryResult->add_sectionmetas();
        sectionMeta->set_fieldid(partitionResponse.sectionmetas(i).fieldid());
        sectionMeta->set_sectionweight(partitionResponse.sectionmetas(i).sectionweight());
        sectionMeta->set_sectionlen(partitionResponse.sectionmetas(i).sectionlen());
    }

    for (int i = 0; i < partitionResponse.matchvalues_size(); ++i) {
        tableQueryResult->add_matchvalues(partitionResponse.matchvalues(i));
    }
}

ErrorInfo TableServiceImpl::convertStatusToErrorInfo(const indexlib::Status &status) {
    if (status.IsOK()) {
        return ErrorInfo::default_instance();
    }

    const string partitionQuerierStatusMsg = status.ToString();

    AUTIL_LOG(ERROR, "SuezTableService Query Failed. status msg: %s", partitionQuerierStatusMsg.c_str());

    ErrorInfo errorInfo;
    errorInfo.set_errormsg(partitionQuerierStatusMsg);

    if (status.IsNotFound()) {
        errorInfo.set_errorcode(TBS_ERROR_NO_RECORD);
    } else if (status.IsInvalidArgs()) {
        if (strstr(partitionQuerierStatusMsg.c_str(), "can not find index")) {
            errorInfo.set_errorcode(TBS_ERROR_CREATE_READER);
        } else {
            errorInfo.set_errorcode(TBS_ERROR_INVALID_ATTR_NAME);
        }

    } else {
        errorInfo.set_errorcode(TBS_ERROR_OTHERS);
    }

    return errorInfo;
}

template <typename T>
void TableServiceImpl::setSingleAttr(const T *const request,
                                     SingleAttrValue &singleAttrValue,
                                     const indexlibv2::base::AttrValue &attrValue) {

    const bool isSingle = attrValue.has_int32_value() || attrValue.has_uint32_value() || attrValue.has_int64_value() ||
                          attrValue.has_uint64_value() || attrValue.has_float_value() || attrValue.has_double_value() ||
                          attrValue.has_bytes_value();

#define GET_VALUE_FROM_ATTRVALUE(SUEZ_SINGLE_FIELD, SUEZ_MULTI_FIELD, INDEXLIB_SINGLE_FIELD, INDEXLIB_MULTI_FIELD)     \
    if (isSingle) {                                                                                                    \
        singleAttrValue.set_##SUEZ_SINGLE_FIELD(attrValue.INDEXLIB_SINGLE_FIELD());                                    \
    } else {                                                                                                           \
        for (int i = 0; i < attrValue.INDEXLIB_MULTI_FIELD().value_size(); ++i) {                                      \
            singleAttrValue.add_##SUEZ_MULTI_FIELD(attrValue.INDEXLIB_MULTI_FIELD().value(i));                         \
        }                                                                                                              \
    }                                                                                                                  \
    break;

    switch (attrValue.type()) {

    case indexlibv2::base::INT_8:
    case indexlibv2::base::INT_16:
    case indexlibv2::base::INT_32:
        GET_VALUE_FROM_ATTRVALUE(intvalue, multiintvalue, int32_value, multi_int32_value);
    case indexlibv2::base::UINT_8:
    case indexlibv2::base::UINT_16:
    case indexlibv2::base::UINT_32:
        GET_VALUE_FROM_ATTRVALUE(intvalue, multiintvalue, uint32_value, multi_uint32_value);
    case indexlibv2::base::INT_64:
        GET_VALUE_FROM_ATTRVALUE(intvalue, multiintvalue, int64_value, multi_int64_value);
    case indexlibv2::base::UINT_64:
        GET_VALUE_FROM_ATTRVALUE(intvalue, multiintvalue, uint64_value, multi_uint64_value);
    case indexlibv2::base::FLOAT:
        GET_VALUE_FROM_ATTRVALUE(doublevalue, multidoublevalue, float_value, multi_float_value);
    case indexlibv2::base::DOUBLE:
        GET_VALUE_FROM_ATTRVALUE(doublevalue, multidoublevalue, double_value, multi_double_value);
    case indexlibv2::base::STRING: {
        const bool encodeStr = request->has_encodestr() && request->encodestr();
        if (isSingle) {
            const string &&str =
                encodeStr ? autil::legacy::Base64EncodeFast(attrValue.bytes_value()) : attrValue.bytes_value();
            singleAttrValue.set_strvalue(str);
        } else {
            for (int i = 0; i < attrValue.multi_bytes_value().value_size(); ++i) {
                const string &&str = encodeStr ? autil::legacy::Base64EncodeFast(attrValue.multi_bytes_value().value(i))
                                               : attrValue.multi_bytes_value().value(i);
                singleAttrValue.add_multistrvalue(str);
            }
        }
        break;
    }
    case indexlibv2::base::INT_128:
    default:;
    }

#undef GET_VALUE_FROM_ATTRVALUE

    return;
}

bool TableServiceImpl::updateTopoInfo(const UpdateArgs &updateArgs) {
    multi_call::TopoInfoBuilder infoBuilder;
    if (buildTableTopoInfo(_rpcServer, updateArgs, infoBuilder) &&
        !publishTopoInfo(_rpcServer, (multi_call::PublishGroupTy)this, infoBuilder)) {
        return false;
    }
    return true;
}

bool TableServiceImpl::buildTableTopoInfo(const suez::RpcServer *rpcServer,
                                          const UpdateArgs &updateArgs,
                                          multi_call::TopoInfoBuilder &infoBuilder) {
    const auto &serviceInfo = updateArgs.serviceInfo;
    const auto &indexProvider = updateArgs.indexProvider;
    string zoneName = serviceInfo.getZoneName() + ZONE_BIZ_NAME_SPLITTER + TABLE_SERVICE_TOPO_NAME;
    int64_t version = multi_call::INVALID_VERSION_ID;
    int64_t protocolVersion = multi_call::INVALID_VERSION_ID;
    int32_t weight = multi_call::MAX_WEIGHT;
    const auto &tableReaders = indexProvider->multiTableReader.getAllTableReaders();
    int32_t grpcPort = -1;
    if (rpcServer) {
        grpcPort = rpcServer->gigRpcServer->getGrpcPort();
    }
    for (const auto &tableReader : tableReaders) {
        const auto &tableName = tableReader.first;
        for (auto &it : tableReader.second) {
            const auto &partId = it.first;
            if (!partId.validate()) {
                AUTIL_LOG(INFO, "partition id is invalid, table service will not publish");
                return false;
            }
            infoBuilder.addBiz(zoneName + ZONE_BIZ_NAME_SPLITTER + tableName,
                               partId.partCount,
                               partId.index,
                               version,
                               weight,
                               protocolVersion,
                               grpcPort);
            AUTIL_LOG(DEBUG,
                      "fill topo info: zone [%s], biz [%s], part count [%d],"
                      " partid [%d], version[%ld], protocol version[%ld], grpcport[%d]",
                      zoneName.c_str(),
                      tableName.c_str(),
                      partId.partCount,
                      partId.index,
                      version,
                      protocolVersion,
                      grpcPort);
        }
    }
    auto topoInfo = infoBuilder.build();
    AUTIL_LOG(INFO, "table service update topo info : %s", topoInfo.c_str());
    return true;
}

bool TableServiceImpl::publishTopoInfo(const suez::RpcServer *rpcServer,
                                       const multi_call::PublishGroupTy group,
                                       const multi_call::TopoInfoBuilder &infoBuilder) const {
    if (rpcServer == nullptr) {
        AUTIL_LOG(ERROR, "rpc server is nullptr");
        return false;
    }
    const auto &topoVec = infoBuilder.getBizTopoInfo();
    auto gigRpcServer = rpcServer->gigRpcServer;
    std::vector<multi_call::SignatureTy> signatureVec;
    if (!gigRpcServer->publishGroup(group, topoVec, signatureVec)) {
        AUTIL_LOG(ERROR, "table service publish topo info failed");
        return false;
    }
    return true;
}

} // namespace suez
