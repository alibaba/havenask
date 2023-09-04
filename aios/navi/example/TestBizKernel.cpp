#include "navi/example/TestKernel.h"
#include "autil/StringUtil.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/PortMergeKernel.h"
#include "navi/engine/PortSplitKernel.h"
#include "navi/example/TestData.h"
#include "navi/example/TestResource.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include <unistd.h>
#include <vector>

namespace navi {

static const std::string BIZ_INT_ARRAY_TYPE_ID = "BizIntArrayType";
class BizIntArrayType : public Type {
public:
    BizIntArrayType()
        : Type(__FILE__, BIZ_INT_ARRAY_TYPE_ID)
    {
    }
    ~BizIntArrayType() {
    }
};
// register type in biz module is not supported
// REGISTER_TYPE(BizIntArrayType);

class BizSourceKernel : public Kernel {
public:
    BizSourceKernel()
        : _times(0)
        , _sleepMs(0) {}

public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("BizSourceKernel")
            .output("output1", CHAR_ARRAY_TYPE_ID)
            .dependOn("TEST_BIZ_RESOURCE", true, nullptr);
    }
    bool config(KernelConfigContext &ctx) override {
        ctx.Jsonize("times", _times, 0);
        ctx.Jsonize("sleep_ms", _sleepMs, 0);
        if (_times < 0) {
            NAVI_KERNEL_LOG(DEBUG, "times is negative");
            return false;
        }
        return true;
    }
    ErrorCode init(KernelInitContext &ctx) override {
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        PortIndex index(0, INVALID_INDEX);
        ++_computeCount;
        if (_computeCount > _times) {
            ctx.setOutput(index, nullptr, true);
            return EC_NONE;
        }
        auto queryId = NAVI_TLS_LOGGER->logger->getLoggerId().queryId;
        std::string dataStr = std::to_string(queryId) + "_" + getNodeName() + "_" + std::to_string(_computeCount);
        DataPtr outData(new HelloData(dataStr));
        NAVI_KERNEL_LOG(DEBUG, "create hello data [%s]", dataStr.c_str());

        if (_sleepMs > 0) {
            usleep(_sleepMs * 1000);
        }
        ctx.setOutput(index, outData, false);
        return EC_NONE;
    }
private:
    KERNEL_DEPEND_DECLARE();
private:
    KERNEL_DEPEND_ON(GraphMemoryPoolR, _poolResource);
    int32_t _times;
    int32_t _sleepMs;
    int32_t _computeCount = 0;
};

REGISTER_KERNEL(BizSourceKernel);


namespace test_ns {
bool test_kernel_init() {
    return true;
}
}

REGISTER_KERNEL_INIT(test_ns::test_kernel_init);

}
