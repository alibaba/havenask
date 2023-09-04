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

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/IquanR.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "sql/resource/SqlConfigResource.h"

namespace iquan {
class DynamicParams;
class ExecConfig;
class IquanDqlRequest;
class SqlPlan;
} // namespace iquan
namespace navi {
class GraphDef;
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace sql {
class SqlConfig;
class SqlQueryRequest;

struct PlanTransformMetricsCollector {
    int64_t plan2GraphTime = 0;
};

class PlanTransformKernel : public navi::Kernel {
public:
    PlanTransformKernel();
    ~PlanTransformKernel() = default;
    PlanTransformKernel(const PlanTransformKernel &) = delete;
    PlanTransformKernel &operator=(const PlanTransformKernel &) = delete;

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

public:
    bool initExecConfig(const SqlQueryRequest *sqlQueryRequest,
                        const SqlConfig &sqlConfig,
                        iquan::ExecConfig &execConfig);
    size_t getParallelNum(const sql::SqlQueryRequest *sqlQueryRequest, const SqlConfig &sqlConfig);
    std::vector<std::string> getParallelTalbes(const sql::SqlQueryRequest *sqlQueryRequest,
                                               const SqlConfig &sqlConfig);

private:
    bool transform(iquan::SqlPlan &sqlPlan,
                   const iquan::IquanDqlRequest &request,
                   const iquan::ExecConfig &execConfig,
                   navi::GraphDef &graphDef,
                   std::vector<std::string> &outputPort,
                   std::vector<std::string> &outputNode);
    static std::string ToSqlPlanString(const iquan::SqlPlan &sqlPlan,
                                       const iquan::DynamicParams &dynamicParams);
    void reportMetrics() const;

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_DEPEND_ON(IquanR, _iquanR);
    KERNEL_DEPEND_ON(SqlConfigResource, _sqlConfigResource);
    KERNEL_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    KERNEL_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    PlanTransformMetricsCollector _metricsCollector;
};

typedef std::shared_ptr<PlanTransformKernel> PlanTransformKernelPtr;

} // namespace sql
