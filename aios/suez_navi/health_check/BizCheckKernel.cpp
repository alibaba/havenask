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
#include "suez_navi/health_check/BizCheckKernel.h"

namespace suez_navi {

const std::string BizCheckKernel::KERNEL_ID = "suez_navi.biz_check_k";
const std::string BizCheckKernel::INPUT_PORT = "suez_navi.biz_check_k.in";
const std::string BizCheckKernel::OUTPUT_PORT = "suez_navi.biz_check_k.out";

BizCheckKernel::BizCheckKernel() {
}

BizCheckKernel::~BizCheckKernel() {
}

void BizCheckKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name(KERNEL_ID)
        .input(INPUT_PORT, "")
        .output(OUTPUT_PORT, "");
}

bool BizCheckKernel::config(navi::KernelConfigContext &ctx) {
    return true;
}

navi::ErrorCode BizCheckKernel::init(navi::KernelInitContext &ctx) {
    return navi::EC_NONE;
}

navi::ErrorCode BizCheckKernel::compute(navi::KernelComputeContext &ctx) {
    navi::DataPtr data;
    bool eof = false;
    ctx.getInput(0, data, eof);
    ctx.setOutput(0, data, eof);
    return navi::EC_NONE;
}

REGISTER_KERNEL(BizCheckKernel);

}
