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
#include "navi/ops/TerminatorKernel.h"
#include "navi/engine/LocalSubGraph.h"

namespace navi {

TerminatorKernel::TerminatorKernel() {
}

TerminatorKernel::~TerminatorKernel() {
}

ErrorCode TerminatorKernel::init(KernelInitContext &ctx) {
    return EC_NONE;
}

void TerminatorKernel::def(KernelDefBuilder &builder) const {
    builder
        .name(TERMINATOR_KERNEL_NAME)
        .inputGroup(TERMINATOR_INPUT_PORT, "", IT_REQUIRE);
}

ErrorCode TerminatorKernel::compute(KernelComputeContext &ctx) {
    ctx.setIgnoreDeadlock();
    ctx._subGraph->notifyFinish(ctx._node, nullptr);
    ctx.stopSchedule();
    return EC_NONE;
}

REGISTER_KERNEL(TerminatorKernel);

}
