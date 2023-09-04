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
#include "suez_navi/health_check/HealthCheckKernel.h"

#include "navi/rpc_server/NaviArpcResponseData.h"
#include "suez_navi/health_check/HealthCheck.pb.h"

namespace suez_navi {

const std::string HealthCheckKernel::KERNEL_ID = "suez_navi.health_check_k";
const std::string HealthCheckKernel::INPUT_PORT = "suez_navi.health_check_k.in";
const std::string HealthCheckKernel::OUTPUT_PORT = "suez_navi.health_check_k.out";

HealthCheckKernel::HealthCheckKernel() {
}

HealthCheckKernel::~HealthCheckKernel() {
}

void HealthCheckKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name(KERNEL_ID)
        .input(INPUT_PORT, "")
        .output(OUTPUT_PORT, "");
}

navi::ErrorCode HealthCheckKernel::init(navi::KernelInitContext &ctx) {
    return navi::EC_NONE;
}

navi::ErrorCode HealthCheckKernel::compute(navi::KernelComputeContext &ctx) {
    navi::DataPtr inputData;
    bool eof = false;
    ctx.getInput(0, inputData, eof);
    health_check::HealthCheckResponse *response = new health_check::HealthCheckResponse();
    NAVI_KERNEL_LOG(DEBUG, "health check request: %s", _requestData->getRequest()->ShortDebugString().c_str());
    navi::DataPtr data(new navi::NaviArpcResponseData(response));
    response->set_serviceready(eof);
    ctx.setOutput(0, data, eof);
    return navi::EC_NONE;
}

REGISTER_KERNEL(HealthCheckKernel);

}

