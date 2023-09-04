#include "navi/tester/NaviTesterOutputKernel.h"
#include "navi/engine/Node.h"
#include "navi/tester/TesterDef.h"

namespace navi {

NaviTesterOutputKernel::NaviTesterOutputKernel() {
}

NaviTesterOutputKernel::~NaviTesterOutputKernel() {
}

void NaviTesterOutputKernel::def(KernelDefBuilder &builder) const {
    builder.name(NAVI_TESTER_OUTPUT_KERNEL)
        .input(NAVI_TESTER_OUTPUT_KERNEL_INPUT, "", IT_REQUIRE)
        .dependOn(NaviTesterResource::RESOURCE_ID, true,
                  BIND_RESOURCE_TO(_testerResource));
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

REGISTER_KERNEL(NaviTesterOutputKernel);

}

