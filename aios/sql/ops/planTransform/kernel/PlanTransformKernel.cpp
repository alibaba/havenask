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
#include "sql/ops/planTransform/PlanTransformKernel.h"

#include <algorithm>
#include <cstddef>
#include <exception>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "iquan/common/catalog/LayerTableDef.h"
#include "iquan/config/ExecConfig.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/SqlPlan.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/proto/GraphDef.pb.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/data/SqlGraphData.h"
#include "sql/data/SqlGraphType.h"
#include "sql/data/SqlPlanData.h"
#include "sql/data/SqlPlanType.h"
#include "sql/data/SqlQueryConfigData.h"
#include "sql/data/SqlQueryRequest.h"
#include "sql/data/SqlRequestData.h"
#include "sql/data/SqlRequestType.h"
#include "sql/ops/planTransform/GraphTransform.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "sql/resource/SqlConfig.h"
#include "sql/resource/SqlConfigResource.h"

namespace iquan {
class DynamicParams;
} // namespace iquan
namespace kmonitor {
class MetricsTags;
} // namespace kmonitor
namespace navi {
class KernelInitContext;
} // namespace navi

using namespace std;
using namespace navi;
using namespace iquan;

namespace sql {

class PlanTransformMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_LATENCY_MUTABLE_METRIC(_plan2GraphTime, "plan2GraphTime");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, const PlanTransformMetricsCollector *collector) {
        REPORT_MUTABLE_METRIC(_plan2GraphTime, autil::US_TO_MS(collector->plan2GraphTime));
    }

private:
    kmonitor::MutableMetric *_plan2GraphTime = nullptr;
};

PlanTransformKernel::PlanTransformKernel() {}

void PlanTransformKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.PlanTransformKernel")
        .input("input0", SqlPlanType::TYPE_ID)
        .input("input1", SqlRequestType::TYPE_ID)
        .output("output0", SqlGraphType::TYPE_ID)
        .output("output1", SqlQueryConfigType::TYPE_ID);
}

bool PlanTransformKernel::config(navi::KernelConfigContext &ctx) {
    return true;
}

navi::ErrorCode PlanTransformKernel::init(navi::KernelInitContext &context) {
    _thisBizName = context.getThisBizName();
    if (_iquanR == nullptr || _sqlConfigResource == nullptr) {
        return navi::EC_INIT_RESOURCE;
    }
    return navi::EC_NONE;
}

navi::ErrorCode PlanTransformKernel::compute(navi::KernelComputeContext &ctx) {
    autil::ScopedTime2 timer;
    navi::PortIndex inputIndex0(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex0, data, isEof);
    if (data == nullptr) {
        SQL_LOG(ERROR, "data is nullptr");
        return navi::EC_ABORT;
    }
    SqlPlanData *sqlPlanData = dynamic_cast<SqlPlanData *>(data.get());
    if (sqlPlanData == nullptr || sqlPlanData->getSqlPlan() == nullptr) {
        SQL_LOG(ERROR, "sql plan data is nullptr");
        return navi::EC_ABORT;
    }
    navi::PortIndex inputIndex1(1, navi::INVALID_INDEX);
    ctx.getInput(inputIndex1, data, isEof);
    if (data == nullptr) {
        SQL_LOG(ERROR, "data is nullptr");
        return navi::EC_ABORT;
    }
    SqlRequestData *sqlRequestData = dynamic_cast<SqlRequestData *>(data.get());
    if (sqlRequestData == nullptr || sqlRequestData->getSqlRequest() == nullptr) {
        SQL_LOG(ERROR, "sql request data is nullptr");
        return navi::EC_ABORT;
    }
    auto sqlQueryConfigData = createSqlQueryConfigData(*sqlRequestData->getSqlRequest());

    iquan::ExecConfig execConfig;
    if (!initExecConfig(sqlRequestData->getSqlRequest().get(),
                        _sqlConfigResource->getSqlConfig(),
                        sqlQueryConfigData->getConfig(),
                        execConfig)) {
        return navi::EC_ABORT;
    }

    auto sqlClient = _iquanR->getSqlClient();
    if (sqlClient == nullptr) {
        SQL_LOG(ERROR, "sql client is empty!");
        return navi::EC_ABORT;
    }
    iquan::SqlPlanPtr sqlPlan = sqlPlanData->getSqlPlan();
    iquan::IquanDqlRequestPtr iquanRequest = sqlPlanData->getIquanSqlRequest();
    vector<string> outputPorts, outputNodes;
    std::unique_ptr<navi::GraphDef> graphDef(new navi::GraphDef());
    auto ret = transform(*sqlPlan, *iquanRequest, execConfig, *graphDef, outputPorts, outputNodes);
    if (!ret) {
        SQL_LOG(ERROR, "failed to call transform");
        return navi::EC_ABORT;
    }

    _metricsCollector.plan2GraphTime = timer.done_us();
    SQL_LOG(TRACE1, "transform sql plan use [%ld] us", _metricsCollector.plan2GraphTime);
    _sqlSearchInfoCollectorR->getCollector()->setSqlPlan2GraphTime(
        _metricsCollector.plan2GraphTime);
    reportMetrics();

    SQL_LOG(TRACE3,
            "transform sql graph success, graph def is: [%s]",
            graphDef->ShortDebugString().c_str());
    SqlGraphDataPtr sqlGraphData(new SqlGraphData(outputPorts, outputNodes, graphDef));
    navi::PortIndex sqlGraphDataIdx(0, navi::INVALID_INDEX);
    navi::PortIndex sqlQueryConfigDataIdx(1, navi::INVALID_INDEX);
    ctx.setOutput(sqlGraphDataIdx, sqlGraphData, true);
    ctx.setOutput(sqlQueryConfigDataIdx, sqlQueryConfigData, true);
    return navi::EC_NONE;
}

bool PlanTransformKernel::initExecConfig(const SqlQueryRequest *sqlQueryRequest,
                                         const SqlConfig &sqlConfig,
                                         const SqlQueryConfig &sqlQueryConfig,
                                         iquan::ExecConfig &execConfig) {
    execConfig.parallelConfig.parallelNum = getParallelNum(sqlQueryRequest, sqlConfig);
    execConfig.parallelConfig.parallelTables = getParallelTalbes(sqlQueryRequest, sqlConfig);
    execConfig.naviConfig.runtimeConfig.threadLimit = sqlConfig.subGraphThreadLimit;
    execConfig.resultAllowSoftFailure = sqlQueryConfig.resultallowsoftfailure();
    execConfig.thisBizName = _thisBizName;
    // execConfig.naviConfig.runtimeConfig.timeout = subGraphTimeout;
    // execConfig.naviConfig.runtimeConfig.traceLevel = traceLevel;

    if (!execConfig.isValid()) {
        std::string errorMsg = "";
        SQL_LOG(ERROR, "invalid execConfig");
        return false;
    }
    return true;
}

size_t PlanTransformKernel::getParallelNum(const SqlQueryRequest *sqlQueryRequest,
                                           const SqlConfig &sqlConfig) {
    size_t parallelNum = PARALLEL_NUM_MIN_LIMIT;
    std::string parallelNumStr;
    if (sqlQueryRequest->getValue(SQL_PARALLEL_NUMBER, parallelNumStr)) {
        parallelNum = autil::StringUtil::fromString<size_t>(parallelNumStr);
    } else {
        parallelNum = sqlConfig.parallelNum;
    }
    parallelNum = std::min(PARALLEL_NUM_MAX_LIMIT, parallelNum);
    parallelNum = std::max(PARALLEL_NUM_MIN_LIMIT, parallelNum);
    return parallelNum;
}

vector<string> PlanTransformKernel::getParallelTalbes(const SqlQueryRequest *sqlQueryRequest,
                                                      const SqlConfig &sqlConfig) {
    vector<string> parallelTables;
    std::string parallelTablesStr;
    if (sqlQueryRequest->getValue(SQL_PARALLEL_TABLES, parallelTablesStr)) {
        autil::StringUtil::split(parallelTables, parallelTablesStr, SQL_PARALLEL_TABLES_SPLIT);
    } else {
        parallelTables = sqlConfig.parallelTables;
    }
    return parallelTables;
}

bool PlanTransformKernel::transform(SqlPlan &sqlPlan,
                                    const iquan::IquanDqlRequest &request,
                                    const ExecConfig &execConfig,
                                    navi::GraphDef &graphDef,
                                    std::vector<std::string> &outputPort,
                                    std::vector<std::string> &outputNode) {
    try {
        GraphTransform::Config config(execConfig, request, sqlPlan.innerDynamicParams);
        GraphTransform graphTransform(config);
        bool ret = graphTransform.sqlPlan2Graph(sqlPlan, graphDef, outputPort, outputNode);
        if (!ret) {
            SQL_LOG(ERROR, "sql plan convert to graph failed");
            SQL_LOG(ERROR,
                    "sql plan is: [%s]",
                    ToSqlPlanString(sqlPlan, request.dynamicParams).c_str());
            return false;
        }
    } catch (const std::exception &e) {
        SQL_LOG(ERROR, "graph transform except [%s]", e.what());
        return false;
    }

    return true;
}

std::string PlanTransformKernel::ToSqlPlanString(const SqlPlan &sqlPlan,
                                                 const DynamicParams &dynamicParams) {
    SqlPlanWrapper sqlPlanWrapper;
    if (!sqlPlanWrapper.convert(sqlPlan, dynamicParams)) {
        return "convert sql plan failed";
    }
    return autil::legacy::FastToJsonString(sqlPlanWrapper, true);
}

std::shared_ptr<SqlQueryConfigData>
PlanTransformKernel::createSqlQueryConfigData(const SqlQueryRequest &request) const {
    SqlQueryConfig config;
    config.set_resultallowsoftfailure(request.isResultAllowSoftFailure(
        _sqlConfigResource->getSqlConfig().resultAllowSoftFailure));
    SQL_LOG(TRACE1, "sql query config[%s]", config.ShortDebugString().c_str());
    return std::make_shared<SqlQueryConfigData>(std::move(config));
}

void PlanTransformKernel::reportMetrics() const {
    if (_queryMetricReporterR) {
        static const std::string path = "sql.builtin.ops.planTransform";
        auto opMetricsReporter = _queryMetricReporterR->getReporter()->getSubReporter(path, {});
        opMetricsReporter->report<PlanTransformMetrics>(nullptr, &_metricsCollector);
    }
}

REGISTER_KERNEL(PlanTransformKernel);

} // namespace sql
