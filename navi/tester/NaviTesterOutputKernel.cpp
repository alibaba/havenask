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
#include "navi/tester/NaviTesterOutputKernel.h"
#include "navi/engine/Node.h"
#include "navi/tester/TesterDef.h"

namespace navi {

NaviTesterOutputKernel::NaviTesterOutputKernel() {
}

NaviTesterOutputKernel::~NaviTesterOutputKernel() {
}

void NaviTesterOutputKernel::def(KernelDefBuilder &builder) const {
    builder
        .name(NAVI_TESTER_OUTPUT_KERNEL)
        .input(NAVI_TESTER_OUTPUT_KERNEL_INPUT, "", IT_REQUIRE)
        .resource(NAVI_TESTER_RESOURCE, true, BIND_RESOURCE_TO(_testerResource));
}

ErrorCode NaviTesterOutputKernel::init(KernelInitContext &ctx) {
    auto node = ctx._node;
    _testerResource->addOutputNode(node);
    return EC_NONE;
}
ErrorCode NaviTesterOutputKernel::compute(KernelComputeContext &ctx) {
    ctx.setIgnoreDeadlock();
    StreamData data;
    if (!ctx.getInput(0, data)) {
        return EC_ABORT;
    }
    if (!data.hasValue) {
        return EC_ABORT;
    }
    NAVI_KERNEL_LOG(DEBUG, "collect output, data [%p] eof [%d]",
                    data.data.get(), data.eof);
    const auto &portName = getNodeName();
    if (!_testerResource->setOutput(portName, data)) {
        return EC_ABORT;
    }
    return EC_NONE;
}

REGISTER_KERNEL(navi::NaviTesterOutputKernel);

}

