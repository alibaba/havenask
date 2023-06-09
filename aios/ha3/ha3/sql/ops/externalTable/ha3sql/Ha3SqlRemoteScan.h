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

#include "ha3/sql/ops/scan/ScanBase.h"

namespace navi {
class AsyncPipe;
class GigQuerySessionR;
} // namespace navi

namespace multi_call {
class Response;
class QuerySession;
class RequestGenerator;
} // namespace multi_call

namespace table {
class Table;
} // namespace table

namespace isearch::proto {
class QrsResponse;
} // namespace isearch::proto

namespace isearch {
namespace sql {

class OutputFieldsVisitor;
class GigQuerySessionCallbackCtx;
class CalcTable;
class Ha3SqlConditionVisitor;
struct StreamQuery;
class TableConfig;
class ServiceConfig;
class Ha3ServiceMiscConfig;

class Ha3SqlRemoteScan : public ScanBase {
public:
    Ha3SqlRemoteScan(const ScanInitParam &param,
                     const std::shared_ptr<navi::AsyncPipe> &asyncPipe,
                     bool initFireQuery = true);
    ~Ha3SqlRemoteScan();

private:
    class QueryGenerateParam {
    public:
        std::string toString() const;
    public:
        std::string serviceName;
        std::string dbName;
        std::string tableName;
        std::string authToken;
        std::string authKey;
        int64_t leftTimeMs;
        bool enableAuth;
    };

private:
    bool doInit() override;
    bool doBatchScan(std::shared_ptr<table::Table> &table, bool &eof) override;
    bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) override;
    void onBatchScanFinish() override;
    
    bool initWithExternalTableConfig();
    bool initOutputFieldsVisitor();
    bool initConditionVisitor();
    bool initCalcTable();
    bool fireQuery();
    bool genQueryString(const QueryGenerateParam &queryParam, std::string &queryStr);
    bool prepareQueryGenerateParam(QueryGenerateParam &queryParam) const;
    std::shared_ptr<multi_call::RequestGenerator> constructRequestGenerator();
    std::shared_ptr<table::Table>
    fillTableResult(const std::vector<std::shared_ptr<multi_call::Response>> &responseVec,
                    bool &hasSoftFailure);
    bool getDecompressedResult(isearch::proto::QrsResponse *response,
                               std::string &decompressedResult);
    int64_t getTimeout() const;

private:
    const TableConfig *_tableConfig = nullptr;
    const ServiceConfig *_serviceConfig = nullptr;
    const Ha3ServiceMiscConfig *_miscConfig = nullptr;
    std::shared_ptr<GigQuerySessionCallbackCtx> _ctx;
    std::shared_ptr<navi::AsyncPipe> _asyncPipe;
    bool _initFireQuery;
    kmonitor::MetricsReporter *_metricsReporter = nullptr;
    std::unique_ptr<OutputFieldsVisitor> _outputFieldsVisitor;
    std::unique_ptr<Ha3SqlConditionVisitor> _conditionVisitor;
    std::unique_ptr<CalcTable> _calcTable;
    std::shared_ptr<StreamQuery> _extraQuery;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
