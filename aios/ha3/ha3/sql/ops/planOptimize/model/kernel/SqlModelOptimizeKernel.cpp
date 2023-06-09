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
#include "ha3/sql/ops/planOptimize/model/SqlModelOptimizeKernel.h"

#include "autil/TimeoutTerminator.h"
#include "ha3/sql/ops/planOptimize/model/SqlModelGraphOptimizer.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/SqlPlanData.h"
#include "ha3/sql/data/SqlPlanType.h"
#include "ha3/sql/resource/IquanResource.h"
#include "ha3/sql/resource/ModelConfigResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "navi/resource/GraphMemoryPoolResource.h"

using namespace std;
using namespace iquan;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, SqlModelOptimizeKernel);

SqlModelOptimizeKernel::SqlModelOptimizeKernel() {}
SqlModelOptimizeKernel::~SqlModelOptimizeKernel() {
}

void SqlModelOptimizeKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("SqlModelOptimizeKernel")
        .input("input0", SqlPlanType::TYPE_ID)
        .output("output0", SqlPlanType::TYPE_ID)
        .resource(IquanResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_iquanResource))
        .resource(SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource))
        .resource(navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource))
        .resource(ModelConfigResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_modelConfigResource));
}

bool SqlModelOptimizeKernel::config(navi::KernelConfigContext &ctx) {
    return true;
}

navi::ErrorCode SqlModelOptimizeKernel::init(navi::KernelInitContext &context) {
    return navi::EC_NONE;
}

navi::ErrorCode SqlModelOptimizeKernel::compute(navi::KernelComputeContext &ctx) {
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex, data, isEof);
    if (data == nullptr) {
        SQL_LOG(ERROR, "data is nullptr");
        return navi::EC_ABORT;
    }
    SqlPlanData *sqlPlanData = dynamic_cast<SqlPlanData *>(data.get());
    if (sqlPlanData == nullptr || sqlPlanData->getSqlPlan() == nullptr
        || sqlPlanData->getIquanSqlRequest() == nullptr) {
        SQL_LOG(ERROR, "sql plan data is nullptr");
        return navi::EC_ABORT;
    }
    auto iquanRequest = sqlPlanData->getIquanSqlRequest();
    auto sqlClient = _iquanResource->getSqlClient();
    if (sqlClient == nullptr) {
        SQL_LOG(ERROR, "sql client is empty!");
        return navi::EC_ABORT;
    }
    int64_t leftTimeMs = 0;
    if (_queryResource->getTimeoutTerminator()) {
        leftTimeMs = 0.3 * _queryResource->getTimeoutTerminator()->getLeftTime() / 1000;
    }
    SqlPlanPtr sqlPlan = sqlPlanData->getSqlPlan();
    GraphOptimizerPtr graphOptimizer(new SqlModelGraphOptimizer(_modelConfigResource->getModelConfigMap(),
                    _queryResource->getPool(),
                    _queryResource->getGigQuerySession(),
                    iquanRequest->dynamicParams,
                    _queryResource->getTracer(),
                    int64_t(leftTimeMs)));
    try {
        if (!graphOptimizer->optimize(*sqlPlan)) {
            SQL_LOG(ERROR, "optimize graph fail [%s]", graphOptimizer->getName().c_str());
            return navi::EC_ABORT;

        }
    } catch (const IquanException &e) {
        std::string errorMsg = "optimizer [" + graphOptimizer->getName() +
                               "] failed to optimize graph, error code is " +
                               std::to_string(e.code()) + ", error message is " +
                               std::string(e.what());
        SQL_LOG(WARN, "%s", errorMsg.c_str());
        return navi::EC_ABORT;
    }

    navi::PortIndex index(0, navi::INVALID_INDEX);
    ctx.setOutput(index, data, true);
    return navi::EC_NONE;
}

REGISTER_KERNEL(SqlModelOptimizeKernel);

} // namespace sql
} // end namespace isearch
