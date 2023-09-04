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
    builder.name(NAVI_TESTER_CONTROL_KERNEL)
        .output(NAVI_TESTER_CONTROL_KERNEL_FAKE_OUTPUT, "")
        .dependOn(NaviTesterResource::RESOURCE_ID, true,
                  BIND_RESOURCE_TO(_testerResource));
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

REGISTER_KERNEL(NaviTesterControlKernel);

}

