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
#include "navi/tester/NaviTesterControlKernel.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/Node.h"
#include "navi/tester/NaviTesterResource.h"
#include "navi/tester/TesterDef.h"

namespace navi {

NaviTesterControlKernel::NaviTesterControlKernel() {
}

NaviTesterControlKernel::~NaviTesterControlKernel() {
}

void NaviTesterControlKernel::def(KernelDefBuilder &builder) const {
    builder
        .name(NAVI_TESTER_CONTROL_KERNEL)
        .output(NAVI_TESTER_CONTROL_KERNEL_FAKE_OUTPUT, "")
        .resource(NAVI_TESTER_RESOURCE, true, BIND_RESOURCE_TO(_testerResource));
}

ErrorCode NaviTesterControlKernel::init(KernelInitContext &ctx) {
    _testerResource->setControlNode(ctx._node);
    auto testNode = ctx._subGraph->getNode(NAVI_TESTER_NODE);
    if (!testNode) {
        NAVI_KERNEL_LOG(ERROR, "test node [%s] not exist",
                        NAVI_TESTER_NODE.c_str());
        return EC_ABORT;
    }
    _testerResource->setTestNode(testNode);
    return EC_NONE;
}

ErrorCode NaviTesterControlKernel::compute(KernelComputeContext &ctx) {
    ctx.setIgnoreDeadlock();
    return EC_NONE;
}

REGISTER_KERNEL(navi::NaviTesterControlKernel);

}

