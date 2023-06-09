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
#include "navi/tester/NaviTesterInputKernel.h"
#include "navi/engine/Node.h"
#include "navi/tester/TesterDef.h"

namespace navi {

NaviTesterInputKernel::NaviTesterInputKernel()
    : _resource(nullptr)
{
}

NaviTesterInputKernel::~NaviTesterInputKernel() {
}

void NaviTesterInputKernel::def(KernelDefBuilder &builder) const {
    builder
        .name(NAVI_TESTER_INPUT_KERNEL)
        .output(NAVI_TESTER_INPUT_KERNEL_OUTPUT, "")
        .resource(NAVI_TESTER_RESOURCE, true, BIND_RESOURCE_TO(_resource));
}

ErrorCode NaviTesterInputKernel::init(KernelInitContext &ctx) {
    auto node = ctx._node;
    _resource->addInputNode(node);
    return EC_NONE;
}

ErrorCode NaviTesterInputKernel::compute(KernelComputeContext &ctx) {
    ctx.setIgnoreDeadlock();
    const auto &portName = getNodeName();
    StreamData data;
    if (!_resource->getInput(portName, data)) {
        return EC_ABORT;
    }
    if (!data.hasValue) {
        return EC_NONE;
    }
    NAVI_KERNEL_LOG(DEBUG, "flush input, data [%p] eof [%d]", data.data.get(),
                    data.eof);
    if (!ctx.setOutput(0, data.data, data.eof)) {
        return EC_ABORT;
    }
    return EC_NONE;
}

REGISTER_KERNEL(navi::NaviTesterInputKernel);

}

