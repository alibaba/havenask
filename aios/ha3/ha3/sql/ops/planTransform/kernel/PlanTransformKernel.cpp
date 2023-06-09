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
#include "ha3/sql/ops/planTransform/PlanTransformKernel.h"

#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/SqlGraphData.h"
#include "ha3/sql/data/SqlGraphType.h"
#include "ha3/sql/data/SqlPlanData.h"
#include "ha3/sql/data/SqlPlanType.h"
#include "ha3/sql/data/SqlRequestData.h"
#include "ha3/sql/data/SqlRequestType.h"
#include "ha3/sql/ops/planTransform/GraphTransform.h"
#include "ha3/sql/resource/IquanResource.h"
#include "ha3/sql/resource/SqlConfigResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/proto/GraphDef.pb.h"

using namespace std;
using namespace navi;
using namespace iquan;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(kernel, PlanTransformKernel);

PlanTransformKernel::PlanTransformKernel() {}

void PlanTransformKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("PlanTransformKernel")
        .input("input0", SqlPlanType::TYPE_ID)
        .input("input1", SqlRequestType::TYPE_ID)
        .output("output0", SqlGraphType::TYPE_ID)
        .resource(IquanResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_iquanResource))
        .resource(SqlConfigResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_sqlConfigResource))
        .resource(navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource));
}

bool PlanTransformKernel::config(navi::KernelConfigContext &ctx) {
    return true;
}

navi::ErrorCode PlanTransformKernel::init(navi::KernelInitContext &context) {
    if (_iquanResource == nullptr || _sqlConfigResource == nullptr) {
        return navi::EC_INIT_RESOURCE;
    }
    return navi::EC_NONE;
}

navi::ErrorCode PlanTransformKernel::compute(navi::KernelComputeContext &ctx) {
    int64_t begTime = autil::TimeUtility::currentTime();
    SqlSearchInfoCollector *sqlSearchInfoCollector = nullptr;
    if (_metaInfoResource) {
        sqlSearchInfoCollector = _metaInfoResource->getOverwriteInfoCollector();
    }
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

    iquan::ExecConfig execConfig;
    auto sqlConfig = _sqlConfigResource->getSqlConfig();
    if (!initExecConfig(sqlRequestData->getSqlRequest().get(), sqlConfig, execConfig)) {
        return navi::EC_ABORT;
    }

    auto sqlClient = _iquanResource->getSqlClient();
    if (sqlClient == nullptr) {
        SQL_LOG(ERROR, "sql client is empty!");
        return navi::EC_ABORT;
    }
    iquan::SqlPlanPtr sqlPlan = sqlPlanData->getSqlPlan();
    iquan::IquanDqlRequestPtr iquanRequest = sqlPlanData->getIquanSqlRequest();
    vector<string> outputPorts, outputNodes;
    std::unique_ptr<navi::GraphDef> graphDef(new navi::GraphDef());
    auto ret = transform(
        *sqlPlan, *iquanRequest, execConfig, *graphDef, outputPorts, outputNodes);
    if (!ret) {
        SQL_LOG(ERROR,
                "failed to call transform");
        return navi::EC_ABORT;
    }
    int64_t endTime = autil::TimeUtility::currentTime();
    SQL_LOG(DEBUG, "transform sql plan use [%ld] us", endTime - begTime);
    if (sqlSearchInfoCollector) {
        sqlSearchInfoCollector->setSqlPlan2GraphTime(endTime - begTime);
    }
    SQL_LOG(DEBUG, "transform sql graph success, graph def is: [%s]",
            graphDef->ShortDebugString().c_str());
    SqlGraphDataPtr sqlGraphData(new SqlGraphData(outputPorts, outputNodes, graphDef));
    navi::PortIndex index(0, navi::INVALID_INDEX);
    ctx.setOutput(index, sqlGraphData, true);
    return navi::EC_NONE;
}

bool PlanTransformKernel::initExecConfig(const SqlQueryRequest *sqlQueryRequest,
                                         const SqlConfig &sqlConfig,
                                         iquan::ExecConfig &execConfig) {
    execConfig.parallelConfig.parallelNum = getParallelNum(sqlQueryRequest, sqlConfig);
    execConfig.parallelConfig.parallelTables = getParallelTalbes(sqlQueryRequest, sqlConfig);
    execConfig.naviConfig.runtimeConfig.threadLimit = sqlConfig.subGraphThreadLimit;
    // execConfig.naviConfig.runtimeConfig.timeout = subGraphTimeout;
    // execConfig.naviConfig.runtimeConfig.traceLevel = traceLevel;
    bool lackResultEnable = sqlConfig.lackResultEnable;
    string lackResultEnableStr;
    if (sqlQueryRequest->getValue(SQL_LACK_RESULT_ENABLE, lackResultEnableStr)) {
        lackResultEnable = autil::StringUtil::fromString<bool>(lackResultEnableStr);
    }
    execConfig.lackResultEnable = lackResultEnable;
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

bool PlanTransformKernel::transform(SqlPlan &sqlPlan, const iquan::IquanDqlRequest &request,
                            const ExecConfig &execConfig,
                            navi::GraphDef &graphDef,
                            std::vector<std::string> &outputPort,
                            std::vector<std::string> &outputNode)
{
    try {
        GraphTransform::Config config(execConfig, request, sqlPlan.innerDynamicParams);
        GraphTransform graphTransform(config);
        bool ret = graphTransform.sqlPlan2Graph(
                sqlPlan, graphDef, outputPort, outputNode);
        if (!ret) {
            SQL_LOG(ERROR, "sql plan convert to graph failed, sql plan is: [%s]",
                    ToSqlPlanString(sqlPlan, request.dynamicParams).c_str());
            return false;
        } else {
            SQL_LOG(DEBUG, "sql plan convert to graph success, sql plan is: [%s]",
                ToSqlPlanString(sqlPlan, request.dynamicParams).c_str());
        }
    } catch (const std::exception &e) {
        SQL_LOG(ERROR, "graph transform except [%s]" , e.what());
        return false;
    }

    return true;
}

std::string PlanTransformKernel::ToSqlPlanString(
        const SqlPlan &sqlPlan,
        const DynamicParams &dynamicParams)
{
    SqlPlanWrapper sqlPlanWrapper;
    if (!sqlPlanWrapper.convert(sqlPlan, dynamicParams)) {
        return "convert sql plan failed";
    }
    return autil::legacy::ToJsonString(sqlPlanWrapper);
}

REGISTER_KERNEL(PlanTransformKernel);

} // namespace sql
} // end namespace isearch
