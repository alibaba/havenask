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

#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>

#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/SqlPlan.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/IquanR.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "sql/resource/SqlConfigResource.h"

namespace iquan {
class DynamicParams;
} // namespace iquan

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace sql {
class SqlQueryRequest;

struct SqlParseMetricsCollector {
    int64_t planTime = 0;
    bool cacheHit = false;
};

class SqlParseKernel : public navi::Kernel {
public:
    SqlParseKernel();
    ~SqlParseKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

    static bool getOutputSqlPlan(const sql::SqlQueryRequest *sqlRequest);

private:
    bool parseSqlPlan(const sql::SqlQueryRequest *sqlRequest,
                      iquan::IquanDqlRequestPtr &iquanRequest,
                      iquan::SqlPlanPtr &sqlPlan);
    bool transToIquanRequest(const sql::SqlQueryRequest *sqlRequest,
                             iquan::IquanDqlRequest &iquanRequest);
    bool addIquanDynamicParams(const sql::SqlQueryRequest *sqlRequest,
                               iquan::DynamicParams &dynamicParams);
    bool addIquanDynamicKVParams(const sql::SqlQueryRequest *sqlRequest,
                                 iquan::DynamicParams &dynamicParams);
    void addIquanHintParams(const sql::SqlQueryRequest *sqlRequest,
                            iquan::DynamicParams &dynamicParams);
    bool addIquanSqlParams(const sql::SqlQueryRequest *sqlRequest,
                           iquan::IquanDqlRequest &iquanRequest);
    void addUserKv(const std::unordered_map<std::string, std::string> &kvPair,
                   std::map<std::string, std::string> &execParams);
    std::string getDefaultCatalogName(const sql::SqlQueryRequest *sqlRequest);
    std::string getDefaultDatabaseName(const sql::SqlQueryRequest *sqlRequest);
    void reportMetrics() const;
    std::string ToSqlPlanString(const iquan::SqlPlan &sqlPlan);

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_DEPEND_ON(IquanR, _iquanR);
    KERNEL_DEPEND_ON(SqlConfigResource, _sqlConfigResource);
    KERNEL_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    KERNEL_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    std::string _dbName;
    std::map<std::string, std::string> _dbNameAlias;
    bool _enableTurboJet;
    SqlParseMetricsCollector _metricsCollector;
};

typedef std::shared_ptr<SqlParseKernel> SqlParseKernelPtr;
} // namespace sql
