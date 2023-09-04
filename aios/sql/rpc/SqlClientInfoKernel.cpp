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
#include "sql/rpc/SqlClientInfoKernel.h"

#include <utility>

#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/proto/QrsService.pb.h"
#include "iquan/common/Status.h"
#include "iquan/jni/Iquan.h"
#include "multi_call/common/ErrorCode.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/rpc_server/NaviArpcResponseData.h"
#include "sql/common/Log.h"

namespace navi {
class KernelInitContext;
} // namespace navi

namespace sql {

const std::string SqlClientInfoKernel::KERNEL_ID = "sql.ClientInfoKernel";
const std::string SqlClientInfoKernel::OUTPUT_PORT = "out";

SqlClientInfoKernel::SqlClientInfoKernel() {}

SqlClientInfoKernel::~SqlClientInfoKernel() {}

void SqlClientInfoKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name(KERNEL_ID).output(OUTPUT_PORT, "");
}

bool SqlClientInfoKernel::config(navi::KernelConfigContext &ctx) {
    return true;
}

navi::ErrorCode SqlClientInfoKernel::init(navi::KernelInitContext &ctx) {
    return navi::EC_NONE;
}

navi::ErrorCode SqlClientInfoKernel::compute(navi::KernelComputeContext &ctx) {
    auto response = new isearch::proto::SqlClientResponse();
    auto sqlClient = _iquanR->getSqlClient();
    std::string result;
    iquan::Status status = sqlClient->dumpCatalog(result);
    if (!status.ok()) {
        iquan::StatusJsonWrapper statusWrapper;
        statusWrapper.status = status;
        response->set_assemblyresult(autil::legacy::FastToJsonString(statusWrapper, true));
        SQL_LOG(WARN, "dump catalog failed, error msg: [%s].", status.errorMessage().c_str());
    } else {
        response->set_assemblyresult(std::move(result));
    }
    response->set_multicall_ec(multi_call::MULTI_CALL_ERROR_NONE);
    navi::DataPtr data(new navi::NaviArpcResponseData(response));
    ctx.setOutput(0, data, true);
    return navi::EC_NONE;
}

REGISTER_KERNEL(SqlClientInfoKernel);

} // namespace sql
