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
    builder.name(NAVI_TESTER_INPUT_KERNEL)
        .output(NAVI_TESTER_INPUT_KERNEL_OUTPUT, "")
        .dependOn(NaviTesterResource::RESOURCE_ID, true,
                  BIND_RESOURCE_TO(_resource));
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

REGISTER_KERNEL(NaviTesterInputKernel);

}

