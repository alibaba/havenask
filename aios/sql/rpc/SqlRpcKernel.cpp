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
#include "sql/rpc/SqlRpcKernel.h"

#include <stdint.h>

#include "autil/StringUtil.h"
#include "ha3/proto/QrsService.pb.h"
#include "multi_call/interface/QuerySession.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/log/NaviLogger.h"
#include "navi/resource/QuerySessionR.h"
#include "navi/rpc_server/NaviArpcRequestData.h"
#include "sql/common/common.h"
#include "sql/data/SqlQueryRequest.h"
#include "sql/data/SqlRequestData.h"

namespace navi {
class KernelInitContext;
} // namespace navi

namespace sql {

const std::string SqlRpcKernel::KERNEL_ID = "sql.rpc.k";
const std::string SqlRpcKernel::OUTPUT_PORT = "out";

SqlRpcKernel::SqlRpcKernel() {}

SqlRpcKernel::~SqlRpcKernel() {}

void SqlRpcKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name(KERNEL_ID).output(OUTPUT_PORT, "");
}

bool SqlRpcKernel::config(navi::KernelConfigContext &ctx) {
    return true;
}

navi::ErrorCode SqlRpcKernel::init(navi::KernelInitContext &ctx) {
    return navi::EC_NONE;
}

navi::ErrorCode SqlRpcKernel::compute(navi::KernelComputeContext &ctx) {
    auto pbMsg = _requestData->getRequest<isearch::proto::QrsRequest>();
    if (!pbMsg) {
        return navi::EC_ABORT;
    }
    if (pbMsg->has_traceid() || pbMsg->has_rpcid() || pbMsg->has_userdata()) {
        const auto &querySession = _querySessionR->getQuerySession();
        if (querySession) {
            querySession->setEagleeyeUserData(pbMsg->traceid(), pbMsg->rpcid(), pbMsg->userdata());
        }
    }
    const auto &assemblyQuery = pbMsg->assemblyquery();
    SqlQueryRequestPtr sqlRequest(new SqlQueryRequest());
    if (!sqlRequest->init(assemblyQuery)) {
        NAVI_KERNEL_LOG(ERROR, "init sql request failed [%s]", assemblyQuery.c_str());
        return navi::EC_ABORT;
    }
    if (!checkAuth(sqlRequest)) {
        NAVI_KERNEL_LOG(ERROR, "request unauthorized, query rejected: %s", assemblyQuery.c_str());
        return navi::EC_ABORT;
    }
    if (std::string traceLevel; sqlRequest->getValue(SQL_TRACE_LEVEL, traceLevel)) {
        ctx.updateTraceLevel(traceLevel);
    }
    if (std::string timeoutMsStr; sqlRequest->getValue(SQL_TIMEOUT, timeoutMsStr)) {
        int64_t timeoutMs;
        if (autil::StringUtil::fromString(timeoutMsStr, timeoutMs)) {
            ctx.updateTimeoutMs(timeoutMs);
        }
    }
    SqlRequestDataPtr sqlRequestData(new SqlRequestData(sqlRequest));
    ctx.setOutput(0, sqlRequestData, true);
    return navi::EC_NONE;
}

bool SqlRpcKernel::checkAuth(const SqlQueryRequestPtr &sqlRequest) const {
    return _sqlAuthManagerR->check(sqlRequest->getSqlQuery(),
                                   sqlRequest->getKvPairStr(),
                                   sqlRequest->getAuthTokenStr(),
                                   sqlRequest->getAuthSignatureStr());
}

REGISTER_KERNEL(SqlRpcKernel);

} // namespace sql
