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

#include "navi/resource/QuerySessionR.h"
#include "sql/config/ExternalTableConfigR.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/externalTable/GigQuerySessionCallbackCtxR.h"
#include "sql/ops/scan/ScanBase.h"

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

namespace sql {

class OutputFieldsVisitor;
class Ha3SqlConditionVisitor;
struct StreamQuery;
class TableConfig;
class ServiceConfig;
class Ha3ServiceMiscConfig;

class Ha3SqlRemoteScanR : public ScanBase {
public:
    Ha3SqlRemoteScanR();
    ~Ha3SqlRemoteScanR();

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

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
    bool doBatchScan(std::shared_ptr<table::Table> &table, bool &eof) override;
    bool doUpdateScanQuery(const StreamQueryPtr &inputQuery) override;
    void onBatchScanFinish() override;
    bool initWithExternalTableConfig();
    bool initOutputFieldsVisitor();
    bool initConditionVisitor();
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
    RESOURCE_DEPEND_DECLARE_BASE(ScanBase);

private:
    RESOURCE_DEPEND_ON(ExternalTableConfigR, _externalTableConfigR);
    RESOURCE_DEPEND_ON(GigQuerySessionCallbackCtxR, _ctx);
    RESOURCE_DEPEND_ON(CalcTableR, _calcTableR);
    const TableConfig *_tableConfig = nullptr;
    const ServiceConfig *_serviceConfig = nullptr;
    const Ha3ServiceMiscConfig *_miscConfig = nullptr;
    bool _initFireQuery = true;
    std::unique_ptr<OutputFieldsVisitor> _outputFieldsVisitor;
    std::unique_ptr<Ha3SqlConditionVisitor> _conditionVisitor;
    std::shared_ptr<StreamQuery> _extraQuery;
};

} // namespace sql
