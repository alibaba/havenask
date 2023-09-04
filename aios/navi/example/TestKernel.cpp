#include "navi/example/TestKernel.h"
#include "autil/StringUtil.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/PortMergeKernel.h"
#include "navi/engine/PortSplitKernel.h"
#include "navi/engine/ScopeTerminatorKernel.h"
#include "navi/example/TestData.h"
#include "navi/example/TestResource.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/resource/GraphMemoryPoolR.h"
// #include "navi/tensorflow/TensorData.h"
#include "navi/util/CommonUtil.h"
#include "autil/Thread.h"
#include <exception>
#include <unistd.h>
#include <vector>
#include <fstream>

using namespace autil;

namespace navi {

class SourceKernel : public Kernel {
public:
    SourceKernel()
        : _times(0)
        , _sleepMs(0) {}

public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("SourceKernel")
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    bool config(KernelConfigContext &ctx) override {
        NAVI_JSONIZE(ctx, "times", _times, 0);
        NAVI_JSONIZE(ctx, "sleep_ms", _sleepMs, 0);
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
        if (_times == 0) {
            ctx.setOutput(index, nullptr, true);
            return EC_NONE;
        }
        ++_computeCount;
        auto queryId = NAVI_TLS_LOGGER->logger->getLoggerId().queryId;
        std::string dataStr = std::to_string(queryId) + "_" + getNodeName() + "_" + std::to_string(_computeCount);
        DataPtr outData(new HelloData(dataStr));
        NAVI_KERNEL_LOG(DEBUG, "create hello data [%s]", dataStr.c_str());

        if (_sleepMs > 0) {
            usleep(_sleepMs * 1000);
        }
        ctx.setOutput(index, outData, _computeCount >= _times);
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

void AsyncInInitSourceKernel::def(KernelDefBuilder &builder) const {
    builder.name("AsyncInInitSourceKernel")
        .output("output1", CHAR_ARRAY_TYPE_ID);
}

ErrorCode AsyncInInitSourceKernel::init(KernelInitContext &ctx) {
    _asyncPipe = ctx.createAsyncPipe();
    if (!_asyncPipe) {
        NAVI_KERNEL_LOG(ERROR, "create async pipe failed");
        return EC_INIT_SUB_GRAPH;
    }

    lastPipe = _asyncPipe;
    return EC_NONE;
}

AsyncPipePtr AsyncInInitSourceKernel::lastPipe;

ErrorCode AsyncInInitSourceKernel::compute(KernelComputeContext &ctx) {
    DataPtr outData;
    bool eof = true;
    _asyncPipe->getData(outData, eof);
    if (dynamic_cast<ErrorCommandData *>(outData.get()) != nullptr) {
        NAVI_KERNEL_LOG(ERROR, "error command got, will abort");
        return EC_ABORT;
    }

    PortIndex index(0, INVALID_INDEX);
    NAVI_KERNEL_LOG(DEBUG, "setoutput data [%p] eof [%d]", outData.get(), eof);
    ctx.setOutput(index, outData, eof);
    return EC_NONE;
}

void AsyncInComputeSourceKernel::def(KernelDefBuilder &builder) const {
    builder.name("AsyncInComputeSourceKernel")
        .output("output1", CHAR_ARRAY_TYPE_ID);
}

AsyncPipePtr AsyncInComputeSourceKernel::lastPipe;

ErrorCode AsyncInComputeSourceKernel::compute(KernelComputeContext &ctx) {
    DataPtr outData;
    bool eof = true;
    if (!_asyncPipe) {
        _asyncPipe = ctx.createAsyncPipe();
        if (!_asyncPipe) {
            NAVI_KERNEL_LOG(ERROR, "create async pipe failed");
            return EC_INIT_SUB_GRAPH;
        }
        lastPipe = _asyncPipe;
        return EC_NONE;
    }
    _asyncPipe->getData(outData, eof);
    if (dynamic_cast<ErrorCommandData *>(outData.get()) != nullptr) {
        NAVI_KERNEL_LOG(ERROR, "error command got, will abort");
        return EC_ABORT;
    }

    PortIndex index(0, INVALID_INDEX);
    NAVI_KERNEL_LOG(DEBUG, "setoutput data [%p] eof [%d]", outData.get(), eof);
    ctx.setOutput(index, outData, eof);
    return EC_NONE;
}

void AsyncWithoutOutputSourceKernel::def(KernelDefBuilder &builder) const {
    builder.name("AsyncWithoutOutputSourceKernel")
        .input("input1", CHAR_ARRAY_TYPE_ID);
}

AsyncPipePtr AsyncWithoutOutputSourceKernel::lastPipe;

ErrorCode AsyncWithoutOutputSourceKernel::compute(KernelComputeContext &ctx) {
    if (!_asyncPipe) {
        PortIndex index(0, INVALID_INDEX);
        DataPtr data;
        bool inputEof;
        ctx.getInput(index, data, inputEof);

        if (inputEof) {
            _asyncPipe = ctx.createAsyncPipe();
            if (!_asyncPipe) {
                NAVI_KERNEL_LOG(ERROR, "create async pipe failed");
                return EC_INIT_SUB_GRAPH;
            }
            lastPipe = _asyncPipe;
        }
    } else {
        bool eof = true;
        DataPtr outData;
        _asyncPipe->getData(outData, eof);
        if (dynamic_cast<ErrorCommandData *>(outData.get()) != nullptr) {
            NAVI_KERNEL_LOG(ERROR, "error command got, will abort");
            return EC_ABORT;
        }
    }
    return EC_NONE;
}

void AsyncInitSlowSourceKernel::def(KernelDefBuilder &builder) const {
    builder.name("AsyncInitSlowSourceKernel")
        .output("output1", CHAR_ARRAY_TYPE_ID);
}

ErrorCode AsyncInitSlowSourceKernel::init(KernelInitContext &ctx) {
    _asyncPipe = ctx.createAsyncPipe();
    if (!_asyncPipe) {
        NAVI_KERNEL_LOG(ERROR, "create async pipe failed");
        return EC_INIT_SUB_GRAPH;
    }

    lastPipe = _asyncPipe;
    lastClosure = &_waitClosure;

    NAVI_KERNEL_LOG(DEBUG, "init wait closure start");
    _waitClosure.wait();
    NAVI_KERNEL_LOG(DEBUG, "init wait closure end");
    return EC_NONE;
}

AsyncPipePtr AsyncInitSlowSourceKernel::lastPipe;
WaitClosure *AsyncInitSlowSourceKernel::lastClosure;

ErrorCode AsyncInitSlowSourceKernel::compute(KernelComputeContext &ctx) {
    DataPtr outData;
    bool eof = true;
    _asyncPipe->getData(outData, eof);
    if (dynamic_cast<ErrorCommandData *>(outData.get()) != nullptr) {
        NAVI_KERNEL_LOG(ERROR, "error command got, will abort");
        return EC_ABORT;
    }

    PortIndex index(0, INVALID_INDEX);
    NAVI_KERNEL_LOG(DEBUG, "setoutput data [%p] eof [%d]", outData.get(), eof);
    ctx.setOutput(index, outData, eof);
    return EC_NONE;
}

void AsyncOptionalMixKernel::def(KernelDefBuilder &builder) const {
    builder.name("AsyncOptionalMixKernel")
            .input("input1", CHAR_ARRAY_TYPE_ID, IT_OPTIONAL)
            .output("output1", CHAR_ARRAY_TYPE_ID);
}

ErrorCode AsyncOptionalMixKernel::init(KernelInitContext &ctx) {
    _asyncPipe = ctx.createAsyncPipe(ActivateStrategy::AS_OPTIONAL);
    if (!_asyncPipe) {
        NAVI_KERNEL_LOG(ERROR, "create optional async pipe failed");
        return EC_INIT_SUB_GRAPH;
    }

    lastPipe = _asyncPipe;
    return EC_NONE;
}

AsyncPipePtr AsyncOptionalMixKernel::lastPipe;

ErrorCode AsyncOptionalMixKernel::compute(KernelComputeContext &ctx) {
    PortIndex index(0, INVALID_INDEX);

    DataPtr inputData, pipeData;
    bool inputEof, pipeEof;
    ctx.getInput(index, inputData, inputEof);
    _asyncPipe->getData(pipeData, pipeEof);
    if (pipeData || pipeEof) {
        NAVI_KERNEL_LOG(DEBUG, "setoutput data [%p] eof [%d] from pipe",
                        pipeData.get(), pipeEof);
        if (pipeEof) {
            _asyncPipe.reset();
        }
        ctx.setOutput(index, pipeData, pipeEof);
        return EC_NONE;
    }

    NAVI_KERNEL_LOG(DEBUG, "setoutput data [%p] eof [%d] from input",
                    inputData.get(), inputEof);
    ctx.setOutput(index, inputData, inputEof);
    return EC_NONE;
}

// class TFSourceKernel : public Kernel {
// public:
//     TFSourceKernel()
//         : _times(0)
//         , _value(-1)
//     {
//     }
// public:
//     void def(KernelDefBuilder &builder) const override {
//         builder
//             .name("TFSourceKernel")
//             .output("output1", TensorType::TYPE_ID);
//     }
//     bool config(KernelConfigContext &ctx) override {
//         NAVI_JSONIZE(ctx, "times", _times);
//         NAVI_JSONIZE(ctx, "value", _value, 0);
//         if (_times < 0) {
//             NAVI_KERNEL_LOG(DEBUG, "times is negative");
//             return false;
//         }
//         return true;
//     }
//     ErrorCode init(KernelInitContext &ctx) override {
//         return EC_NONE;
//     }
//     ErrorCode compute(KernelComputeContext &ctx) override {
//         auto count = ctx.getMetric().scheduleCount();
//         PortIndex index(0, INVALID_INDEX);
//         tensorflow::Tensor tensor(tensorflow::DT_FLOAT, {3});
//         tensor.flat<float>()(0) = -42.0;
//         tensor.flat<float>()(1) = count;
//         tensor.flat<float>()(2) = _value;
//         auto tensorData = new TensorData(tensor);
//         DataPtr outData(tensorData);
//         NAVI_KERNEL_LOG(DEBUG,
//                         "tensortData: %p, tensor: %p, type: %d, count: %ld",
//                         tensorData, tensorData->getTensorValue().tensor,
//                         tensor.dtype(), count);
//         ctx.setOutput(index, outData, count >= _times);
//         return EC_NONE;
//     }
// private:
//     int32_t _times;
//     int32_t _value;
// };

class HelloKernelOld : public Kernel {
public:
    HelloKernelOld()
        : _times(0)
        , _count(0)
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("HelloKernelOld")
            .input("input1", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    bool config(KernelConfigContext &ctx) override {
        NAVI_JSONIZE(ctx, "times", _times);
        if (_times < 0) {
            NAVI_KERNEL_LOG(DEBUG, "times is negative");
            return false;
        }
        return true;
    }
    ErrorCode init(KernelInitContext &ctx) override {
        auto resource = ctx.getDependResource<Resource>("Resource");
        if (resource) {
            NAVI_KERNEL_LOG(DEBUG, "[%s] get resource: %s",
                            getNodeName().c_str(), resource->getName().c_str());
        }
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        NAVI_KERNEL_LOG(DEBUG, "[HelloKernelOld] compute");
        PortIndex index(0, INVALID_INDEX);
        DataPtr data;
        bool inputEof;
        ctx.getInput(index, data, inputEof);
        if (data) {
            _input = dynamic_cast<HelloData *>(data.get())->getData()[0];
            NAVI_KERNEL_LOG(DEBUG, "[%s] input [%s]", getNodeName().c_str(),
                            _input.c_str());
        }
        DataPtr outData(new HelloData(
                        autil::StringUtil::toString(_count) +
                        "_hello:" + autil::StringUtil::toString(_times) + "_" + _input));
        _count++;
        NAVI_KERNEL_LOG(DEBUG, "[%s] count [%d]", getNodeName().c_str(), _count);
        bool eof = false;
        if (_count > _times) {
            NAVI_KERNEL_LOG(DEBUG, "[HelloKernelOld] set finish");
            eof = true;
        }
        ctx.setOutput(index, outData, eof);
        return EC_NONE;
    }
private:
    int32_t _times;
    std::string _input;
    int32_t _count;
};

class HelloKernel : public Kernel {
public:
    HelloKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("HelloKernel")
            .input("input1", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID)
            .dependOn("A", true, nullptr)
            .dependOn("External", false, nullptr)
            .dependOn(GraphMemoryPoolR::RESOURCE_ID, true, nullptr);
    }
    bool config(KernelConfigContext &ctx) override {
        return true;
    }
    ErrorCode init(KernelInitContext &ctx) override {
        auto resource = ctx.getDependResource<Resource>("A");
        if (resource) {
            NAVI_KERNEL_LOG(DEBUG, "get resource: %s success, %p",
                            resource->getName().c_str(), resource);
        } else {
            NAVI_KERNEL_LOG(ERROR, "get resource [%s] failed", "A");
        }
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        PortIndex index(0, INVALID_INDEX);
        DataPtr data;
        bool eof = false;
        if (!ctx.getInput(index, data, eof)) {
            return EC_ABORT;
        }
        HelloData *typeData = nullptr;
        if (data) {
            typeData = new HelloData(*dynamic_cast<HelloData *>(data.get()));
            typeData->mark(getNodeName() + "_" + std::to_string(ctx.getThisPartId()));
        } else {
            // if (!eof) {
            //     NAVI_KERNEL_LOG(ERROR, "null data and not eof");
            //     return EC_ABORT;
            // }
        }
        // if ("fork" == getNodeName() && ctx.getMetric().scheduleCount() > 2) {
        //     ctx.setOutput(index, nullptr, true);
        //     return EC_NONE;
        // }
        DataPtr outputData(typeData);
        // if ((ctx.getMetric().scheduleCount() > 2) &&
        //     "client_hello_dup_1" == getNodeName()) {
        //     ctx.setOutput(0, nullptr, true);
        //     return EC_NONE;
        // }

        ctx.setOutput(index, outputData, eof);
        if (eof) {
            NAVI_KERNEL_LOG(DEBUG, "set finish");
        }
        return EC_NONE;
    }
};

class WorldKernel : public Kernel {
public:
    WorldKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("WorldKernel")
            .inputGroup("input1", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID)
            .dependOn("C", true, nullptr)
            .dependOn("F", true, nullptr)
            .dependOn("H", true, nullptr)
            .dependOn("ProduceTest", true, nullptr)
            .dynamicResourceGroup("group1", BIND_DYNAMIC_RESOURCE_TO(_uSet));
    }
    ErrorCode init(KernelInitContext &ctx) override {
        if (_uSet.empty()) {
            return EC_KERNEL;
        }
        NAVI_KERNEL_LOG(DEBUG, "uSet first: %p", *_uSet.begin());
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        // for (size_t i = 0; i < 1000000; i++) {
        //     for (size_t i = 0; i < 1000; i++) {

        // }

        // }
        size_t inputGroupSize;
        ctx.getInputGroupSize(0, inputGroupSize);
        NAVI_KERNEL_LOG(DEBUG, "input group size: %ld", inputGroupSize);
        GroupDatas datas;
        if (!ctx.getGroupInput(0, datas)) {
            return EC_ABORT;
        }
        auto eof = datas.eof();
        if (eof) {
            NAVI_KERNEL_LOG(DEBUG, "eof");
        }
        if ((ctx.getMetric().scheduleCount() > 100) &&
            "client_hello_1" == getNodeName()) {
            ctx.setOutput(0, nullptr, true);
            return EC_NONE;
        }

        if (datas[0].data) {
            auto data = datas[0].data;
            auto typeData = new HelloData(*dynamic_cast<HelloData *>(data.get()));
            typeData->mark(getNodeName());
            if (!ctx.setOutput(0, DataPtr(typeData), eof)) {
                return EC_ABORT;
            }
        } else {
            // if (!eof) {
            //     return EC_ABORT;
            // } else
            {
                // NAVI_KERNEL_LOG(DEBUG, "setEof");
                if (!ctx.setOutput(0, nullptr, eof)) {
                    return EC_ABORT;
                }
            }
        }
        return EC_NONE;
    }
private:
    std::set<U *> _uSet;
};

class MergeKernel : public Kernel {
public:
    MergeKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("MergeKernel")
            .input("input1", CHAR_ARRAY_TYPE_ID)
            .input("input2", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        PortIndex index1(0, INVALID_INDEX);
        PortIndex index2(1, INVALID_INDEX);
        DataPtr data1, data2;
        bool eof = true;
        ctx.getInput(index1, data1, eof);
        ctx.getInput(index2, data2, eof);
        auto typeData1 = dynamic_cast<HelloData *>(data1.get());
        auto typeData2 = dynamic_cast<HelloData *>(data2.get());
        if (typeData1) {
            typeData1->merge(typeData2);
        }
        ctx.setOutput(index1, data1, true);
        return EC_NONE;
    }
};

class SplitKernel : public Kernel {
public:
    SplitKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("SplitKernel")
            .input("input1", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID)
            .output("output2", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        PortIndex index1(0, INVALID_INDEX);
        PortIndex index2(1, INVALID_INDEX);
        DataPtr data;
        bool eof = true;
        ctx.getInput(index1, data, eof);
        const auto &dataVec = dynamic_cast<HelloData *>(data.get())->getData();
        HelloData *outData1 = new HelloData();
        HelloData *outData2 = new HelloData();
        for (const auto &dataStr : dataVec) {
            size_t pos = dataStr.rfind("_");
            outData1->append(dataStr.substr(0, pos));
            outData2->append(dataStr.substr(pos));
        }
        ctx.setOutput(index1, DataPtr(outData1), true);
        ctx.setOutput(index2, DataPtr(outData2), true);
        return EC_NONE;
    }
};

class AbortKernel : public Kernel {
public:
    AbortKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("AbortKernel")
            .input("input1", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        PortIndex index(0, INVALID_INDEX);
        DataPtr data;
        bool eof = true;
        ctx.getInput(index, data, eof);
        NAVI_KERNEL_LOG(ERROR, "abort");
        return EC_ABORT;
    }
};

class StopKernel : public Kernel {
public:
    StopKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("StopKernel")
            .input("input1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        PortIndex index(0, INVALID_INDEX);
        DataPtr data;
        bool eof = true;
        ctx.getInput(index, data, eof);
        ctx.stopSchedule();
        return EC_NONE;
    }
};

class PartCancelSourceKernel : public Kernel {
public:
    PartCancelSourceKernel() = default;
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("PartCancelSourceKernel")
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        if (ctx.getThisPartId() == 0) {
            return EC_PART_CANCEL;
        }
        usleep(0.5 * 1000 * 1000);
        char buf[1024];
        snprintf(buf, 1024, "Hello from %s(%p), part [%d]",
                 getNodeName().c_str(), this, ctx.getThisPartId());
        DataPtr outData(new HelloData(buf));

        PortIndex outputIndex(0, INVALID_INDEX);
        ctx.setOutput(outputIndex, outData, true);
        return EC_NONE;
    }
};

class PartTimeoutSourceKernel : public Kernel {
public:
    PartTimeoutSourceKernel() = default;
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("PartTimeoutSourceKernel")
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        if (ctx.getThisPartId() == 0) {
            return EC_TIMEOUT;
        }
        usleep(0.5 * 1000 * 1000);
        char buf[1024];
        snprintf(buf, 1024, "Hello from %s(%p), part [%d]",
                 getNodeName().c_str(), this, ctx.getThisPartId());
        DataPtr outData(new HelloData(buf));

        PortIndex outputIndex(0, INVALID_INDEX);
        ctx.setOutput(outputIndex, outData, true);
        return EC_NONE;
    }
};


class InitAbortKernel : public Kernel {
public:
    InitAbortKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("InitAbortKernel")
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }

    ErrorCode init(KernelInitContext &ctx) override {
        return EC_ABORT;
    }

    ErrorCode compute(KernelComputeContext &ctx) override {
        return EC_NONE;
    }
};

class SleepKernel : public Kernel {
public:
    SleepKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("SleepKernel")
            .input("input1", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        PortIndex index(0, INVALID_INDEX);
        DataPtr data;
        bool eof = true;
        ctx.getInput(index, data, eof);
        HelloData *typeData = nullptr;
        if (data) {
            typeData = new HelloData(*dynamic_cast<HelloData *>(data.get()));
            typeData->mark(getNodeName());
        }
        usleep(1 * 1000 * 1000);
        PortIndex outputIndex(0, INVALID_INDEX);
        ctx.setOutput(outputIndex, DataPtr(typeData), eof);
        return EC_NONE;
    }
};

class SubGraphKernel : public Kernel {
public:
    SubGraphKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("SubGraphKernel")
            .input("input1", CHAR_ARRAY_TYPE_ID)
            .output("output2", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    bool config(KernelConfigContext &ctx) override {
        NAVI_JSONIZE(ctx, "abort", _abort, _abort);
        return true;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        auto graphDef = new GraphDef();
        GraphBuilder builder(graphDef);
        std::vector<std::string> itemList(
            { "server_biz", "server_biz_1", "server_biz_2", "client_biz" });
        //const auto &bizName = itemList[CommonUtil::random64() % itemList.size()];
        auto nodeName = getNodeName();
        auto helloName = "subhello_" + nodeName;
        builder.newSubGraph("server_biz_1");
        auto kernelName = "IdentityTestKernel";
        if (_abort) {
            kernelName = "AbortKernel";
        }
        auto hello = builder.node(helloName).kernel(kernelName);
        auto world = builder.node("subworld_" + nodeName).kernel(kernelName);
        hello.in("input1").fromForkNodeInput("input1");
        world.in("input1").from(hello.out("output1"));
        world.out("output1").toForkNodeOutput("output1").merge("StringMergeKernel");
        auto scopeTerminator = builder.getScopeTerminator();
        scopeTerminator.kernel("TestScopeTerminator").out("out").toForkNodeOutput("output2").merge("StringMergeKernel");

        ForkGraphParam param;
        // param.errorAsEof = true;
        // std::string graphFileName("sub_searcher_graph.def");
        // std::ofstream graphFile(graphFileName);
        // graphFile << graphDef->SerializeAsString();
        return ctx.fork(graphDef, param);
    }
private:
    int64_t _abort = 0;
};

class StringSplitKernel : public PortSplitKernel {
public:
    StringSplitKernel()
    {
    }
    std::string getName() const override {
        return "StringSplitKernel";
    }
    std::string dataType() const override {
        return CHAR_ARRAY_TYPE_ID;
    }
    ErrorCode doCompute(const StreamData &streamData,
                        std::vector<DataPtr> &dataVec) override
    {
        auto helloData = dynamic_cast<HelloData *>(streamData.data.get());
        if (!helloData) {
            return EC_NONE;
        }
        int32_t partCount = dataVec.size();
        for (int32_t i = 0; i < partCount; i++) {
            auto splitData = new HelloData(*helloData);
            splitData->mark("split_value_" + autil::StringUtil::toString(i));
            dataVec[i].reset(splitData);
        }
        return EC_NONE;
    }
};

class StringMergeKernel : public PortMergeKernel {
public:
    StringMergeKernel() {
    }
    std::string getName() const override {
        return "StringMergeKernel";
    }
    std::string dataType() const override {
        return CHAR_ARRAY_TYPE_ID;
    }
    ErrorCode doCompute(const std::vector<StreamData> &dataVec,
                        StreamData &outputData) override
    {
        bool retEof = true;
        auto helloData = new HelloData();
        for (const auto &streamData : dataVec) {
            if (streamData.data) {
                helloData->merge(dynamic_cast<HelloData *>(streamData.data.get()));
            }
            retEof &= streamData.eof;
        }
        if (0 == helloData->size()) {
            delete helloData;
            helloData = nullptr;
        }
        outputData.data.reset(helloData);
        outputData.eof = retEof;
        return EC_NONE;
    }
};

class DeadKernel : public Kernel {
public:
    DeadKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("DeadKernel");
    }
    bool config(KernelConfigContext &ctx) override {
        return true;
    }
    ErrorCode init(KernelInitContext &ctx) override {
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        return EC_NONE;
    }
};

class BadInputTypeKernel : public Kernel {
public:
    BadInputTypeKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("BadInputTypeKernel")
            .input("input1", "not_exist_input_type");
    }
    bool config(KernelConfigContext &ctx) override {
        return true;
    }
    ErrorCode init(KernelInitContext &ctx) override {
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        return EC_NONE;
    }
};

class BadOutputTypeKernel : public Kernel {
public:
    BadOutputTypeKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("BadOutputTypeKernel")
            .output("output1", "not_exist_output_type");
    }
    bool config(KernelConfigContext &ctx) override {
        return true;
    }
    ErrorCode init(KernelInitContext &ctx) override {
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        return EC_NONE;
    }
};

class IntArrayTypeKernel : public Kernel {
public:
    IntArrayTypeKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("IntArrayTypeKernel")
            .output("output1", INT_ARRAY_TYPE_ID);
    }
    bool config(KernelConfigContext &ctx) override {
        return true;
    }
    ErrorCode init(KernelInitContext &ctx) override {
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        return EC_NONE;
    }
};

class IdentityTestKernel : public Kernel {
public:
    IdentityTestKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("IdentityTestKernel")
            .input("input1", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        PortIndex index(0, INVALID_INDEX);
        DataPtr data;
        bool eof = false;
        if (!ctx.getInput(index, data, eof)) {
            return EC_ABORT;
        }
        NAVI_KERNEL_LOG(DEBUG, "set output data [%p], eof [%d]", data.get(), eof);
        ctx.setOutput(index, data, eof);
        return EC_NONE;
    }
};

class NamedDataKernel : public Kernel {
public:
    NamedDataKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("NamedDataKernel")
            .input("input1", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode init(KernelInitContext &ctx) override {
        if (!_data) {
            NAVI_KERNEL_LOG(ERROR, "null named data");
            return EC_KERNEL;
        }
        if (!_w) {
            NAVI_KERNEL_LOG(ERROR, "null resource W");
            return EC_KERNEL;
        }
        const auto &dataVec = _data->getData();
        if (!dataVec.empty()) {
            NAVI_KERNEL_LOG(DEBUG, "named data index 0: [%s]",
                            dataVec[0].c_str());
        }
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        PortIndex index(0, INVALID_INDEX);
        DataPtr data;
        bool eof = false;
        if (!ctx.getInput(index, data, eof)) {
            return EC_ABORT;
        }
        NAVI_KERNEL_LOG(DEBUG, "set output data [%p], eof [%d]", data.get(), eof);
        ctx.setOutput(index, data, eof);
        return EC_NONE;
    }
private:
    KERNEL_DEPEND_DECLARE();
private:
    KERNEL_NAMED_DATA(HelloData, _data, "test_named_data");
    KERNEL_DEPEND_ON_ID(W, _w, "W", true);
    KERNEL_DEPEND_ON_ID(V, _v, "V", true);
    KERNEL_DEPEND_ON_ID(X, _x, "X", true);
    KERNEL_DEPEND_ON_ID(DependOnBizResource, _dependOnBizResource, "DependOnBizResource", true);
};

class NamedDataKernelSub : public NamedDataKernel {
public:
    NamedDataKernelSub()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("NamedDataKernelSub")
            .input("input1", CHAR_ARRAY_TYPE_ID)
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode init(KernelInitContext &ctx) override {
        if (!_a) {
            NAVI_KERNEL_LOG(ERROR, "null resource A");
            return EC_KERNEL;
        }
        if (!_data1) {
            NAVI_KERNEL_LOG(ERROR, "null named data");
            return EC_KERNEL;
        }
        return NamedDataKernel::init(ctx);
    }
private:
    KERNEL_DEPEND_DECLARE_BASE(NamedDataKernel);
private:
    KERNEL_DEPEND_ON_ID(A, _a, "A", true);
    KERNEL_NAMED_DATA(HelloData, _data1, "test_named_data");
};

class ForkGraphKernel : public Kernel {
public:
    ForkGraphKernel()
    {
        _forkGraphDef.reset(new GraphDef());
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder
            .name("ForkGraphKernel")
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }

    bool config(KernelConfigContext &ctx) override {
        std::string forkGraphStr;
        NAVI_JSONIZE(ctx, "output_node_name", _outputNodeName);
        NAVI_JSONIZE(ctx, "output_node_port", _outputNodePort);
        NAVI_JSONIZE(ctx, "fork_graph", forkGraphStr);
        NAVI_JSONIZE(ctx, "local_biz_name", _localBizName, _localBizName);
        if (!_forkGraphDef->ParseFromString(forkGraphStr)) {
            NAVI_KERNEL_LOG(ERROR, "forkGraphDef is invalid");
            return false;
        }
        NAVI_JSONIZE(ctx, "timeout_ms", _timeoutMs, _timeoutMs);
        NAVI_JSONIZE(ctx, "error_as_eof", _errorAsEof, _errorAsEof);
        return true;
    }

    ErrorCode compute(KernelComputeContext &ctx) override {
        NAVI_KERNEL_LOG(DEBUG, "fork graph def compute, def [%s]", _forkGraphDef->DebugString().c_str());
        std::vector<GraphId> graphIds;
        for (const auto &subGraph : _forkGraphDef->sub_graphs()) {
            auto &location = subGraph.location();
            if (location.biz_name() == _localBizName) {
                graphIds.emplace_back(subGraph.graph_id());
            }
        }
        NAVI_KERNEL_LOG(DEBUG, "to be modified graph ids [%s] with biz name [%s]",
                        StringUtil::toString(graphIds).c_str(),
                        _localBizName.c_str());
        GraphBuilder builder(_forkGraphDef.get());
        for (const auto graphId : graphIds) {
            builder.subGraph(graphId).location(
                    ctx.getThisBizName(),
                    INVALID_NAVI_PART_COUNT,
                    ctx.getThisPartId());
                // .partIds({ctx.getThisPartId()});
        }

        builder.subGraph(0)
            .node(_outputNodeName)
            .out(_outputNodePort)
            .toForkNodeOutput("output1");

        if (!builder.ok()) {
            NAVI_KERNEL_LOG(ERROR, "builder graph failed");
            return EC_ABORT;
        }
        std::string graphFileName("fork_searcher_graph.def");
        std::ofstream graphFile(graphFileName);
        graphFile << _forkGraphDef->SerializeAsString();
        ForkGraphParam param;
        param.errorAsEof = _errorAsEof;
        if (_timeoutMs > 0) {
            param.timeoutMs = _timeoutMs;
        }
        return ctx.fork(_forkGraphDef.release(), param);
    }
private:
    std::unique_ptr<GraphDef> _forkGraphDef;
    std::string _outputNodeName;
    std::string _outputNodePort;
    std::string _localBizName = "invalid_biz_name";
    int64_t _timeoutMs = -1;
    bool _errorAsEof = true;
};

class ForkGraphWithOverrideKernel : public Kernel {
public:
    ForkGraphWithOverrideKernel()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("ForkGraphWithOverrideKernel")
            .inputGroup("input1", CHAR_ARRAY_TYPE_ID)
            .outputGroup("output1", CHAR_ARRAY_TYPE_ID)
            .dependOn("A", true, nullptr)
            .dependOn("test_async_pipe_create_r", true, nullptr)
            .dependOn("KernelTestR1", true, nullptr)
            .dependOn("KernelTestR5", true, nullptr)
            .selectR(
                {"KernelTestR3", "KernelTestR4", "B", "KernelTestRFailed", "KernelTestBuildR"},
                [](KernelConfigContext &ctx) {
                    int64_t index = 0;
                    NAVI_JSONIZE(ctx, "select_index", index);
                    return index;
                },
                true,
                BIND_RESOURCE_TO(_selectR))
            .enableBuildR({"A", "B", "C"})
            .jsonAttrs(R"json({"times" : 2})json")
            .resourceConfigKey("KernelTestR5", "r5");
    }
    bool config(KernelConfigContext &ctx) override {
        NAVI_JSONIZE(ctx, "biz_name", _bizName);
        NAVI_JSONIZE(ctx, "biz_name", _bizName, _bizName);
        NAVI_JSONIZE(ctx, "biz_name", _bizName, "");
        NAVI_JSONIZE(ctx, "biz_name", _bizName, "");
        NAVI_JSONIZE(ctx, "part_count", _partCount, _partCount);
        NAVI_JSONIZE(ctx, "select_index", _selectIndex);
        NAVI_JSONIZE(ctx, NAVI_BUILDIN_ATTR_NODE, _nodeName);
        NAVI_JSONIZE(ctx, NAVI_BUILDIN_ATTR_KERNEL, _kernelName);
        if (getNodeName() != _nodeName) {
            NAVI_KERNEL_LOG(ERROR, "invalid node name: %s", _nodeName.c_str());
            return false;
        }
        if (getKernelName() != _kernelName) {
            NAVI_KERNEL_LOG(ERROR, "invalid kernel name: %s", _kernelName.c_str());
            return false;
        }
        int32_t times = 0;
        NAVI_JSONIZE(ctx, "times", times);
        if (2 != times) {
            NAVI_KERNEL_LOG(ERROR, "invalid attr times: %d", times);
            return false;
        }
        return true;
    }
    bool checkBuildR(KernelComputeContext &ctx) {
        ResourceMap inputMap;
        auto a = ctx.buildR("A", ctx.getConfigContext(), inputMap);
        if (!a) {
            NAVI_KERNEL_LOG(ERROR, "build resource [A] failed");
            return false;
        } else {
            NAVI_KERNEL_LOG(INFO, "build resource [A] success");
        }
        auto b = ctx.buildR("B", ctx.getConfigContext(), inputMap);
        if (!b) {
            NAVI_KERNEL_LOG(ERROR, "build resource [B] failed");
            return false;
        } else {
            NAVI_KERNEL_LOG(INFO, "build resource [B] success");
        }
        auto c = ctx.buildR("C", ctx.getConfigContext(), inputMap);
        if (!c) {
            NAVI_KERNEL_LOG(ERROR, "build resource [C] failed");
            return false;
        } else {
            NAVI_KERNEL_LOG(INFO, "build resource [C] success");
        }
        return true;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        if (!checkBuildR(ctx)) {
            return EC_ABORT;
        }
        if (!_selectR) {
            NAVI_KERNEL_LOG(ERROR, "null select resource");
            return EC_ABORT;
        }
        if (0 == _selectIndex && !dynamic_cast<KernelTestR3 *>(_selectR)) {
            NAVI_KERNEL_LOG(
                ERROR, "invalid select resource type [%s], index [%ld]", typeid(*_selectR).name(), _selectIndex);
            return EC_ABORT;
        }
        if (1 == _selectIndex && !dynamic_cast<KernelTestR4 *>(_selectR)) {
            NAVI_KERNEL_LOG(
                ERROR, "invalid select resource type [%s], index [%ld]", typeid(*_selectR).name(), _selectIndex);
            return EC_ABORT;
        }
        if (2 == _selectIndex && !dynamic_cast<B *>(_selectR)) {
            NAVI_KERNEL_LOG(
                ERROR, "invalid select resource type [%s], index [%ld]", typeid(*_selectR).name(), _selectIndex);
            return EC_ABORT;
        }
        std::unique_ptr<GraphDef> forkGraphDef(new GraphDef());
        GraphBuilder builder(forkGraphDef.get());
        size_t inputGroupSize = 0;
        if (!ctx.getInputGroupSize(0, inputGroupSize)) {
            NAVI_KERNEL_LOG(ERROR, "get input group size failed");
            return EC_ABORT;
        }
        size_t outputGroupSize = 0;
        if (!ctx.getOutputGroupSize(0, outputGroupSize)) {
            NAVI_KERNEL_LOG(ERROR, "get output group size failed");
            return EC_ABORT;
        }
        NAVI_KERNEL_LOG(DEBUG,
                        "part_count: %ld, inputGroupSize: %lu, outputGroupSize: %lu",
                        _partCount,
                        inputGroupSize,
                        outputGroupSize);
        if (inputGroupSize >= outputGroupSize) {
            NAVI_KERNEL_LOG(ERROR,
                            "input group size [%lu] must be smaller than output group size [%lu]",
                            inputGroupSize,
                            outputGroupSize);
            return EC_ABORT;
        }
        auto graphId = builder.newSubGraph(_bizName);
        for (size_t i = 0; i < inputGroupSize; i++) {
            auto n = builder.node("hello_" + std::to_string(i)).kernel("HelloKernel");
            n.in("input1").fromForkNodeInput("input1", i);
            n.out("output1").toForkNodeOutput("output1", i);
        }
        auto placeholder = builder.node("placeholder")
                           .kernel(PLACEHOLDER_KERNEL_NAME);
        auto hello = builder.node("hello")
                     .kernel("HelloKernel");
        placeholder.out(PLACEHOLDER_OUTPUT_PORT).to(hello.in("input1"));
        auto helloOut = hello.out("output1");
        for (size_t i = inputGroupSize; i < outputGroupSize; i++) {
            helloOut.toForkNodeOutput("output1", i);
        }
        if (!builder.ok()) {
            NAVI_KERNEL_LOG(ERROR, "builder graph failed");
            return EC_ABORT;
        }
        ForkGraphParam param;
        for (NaviPartId partId = 0; partId < _partCount; partId++) {
            OverrideData overrideData;
            overrideData.graphId = graphId;
            overrideData.partId = partId;
            overrideData.outputNode = "placeholder";
            overrideData.outputPort = PLACEHOLDER_OUTPUT_PORT;
            DataPtr data(new HelloData("override_data_with_part_id_" + std::to_string(partId)));
            overrideData.datas.push_back(data);
            param.overrideDataVec.push_back(overrideData);
        }
        auto resourceMap = std::make_shared<ResourceMap>();
        resourceMap->addResource(std::make_shared<A>());
        param.subResourceMaps[graphId] = resourceMap;
        return ctx.fork(forkGraphDef.release(), param);
    }
private:
    std::string _bizName;
    int64_t _partCount = 1;
    int64_t _selectIndex = 0;
    Resource *_selectR = nullptr;
    std::string _nodeName;
    std::string _kernelName;
};

class TestScopeTerminator : public ScopeTerminatorKernel {
public:
    TestScopeTerminator()
    {
    }
public:
    std::string getName() const override {
        return "TestScopeTerminator";
    }
    std::vector<std::string> getOutputs() const override {
        return {"out"};
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        GroupDatas datas;
        if (!ctx.getGroupInput(0, datas)) {
            return EC_ABORT;
        }
        DataPtr data = datas[0].data;
        std::string input;
        if (data) {
            input = dynamic_cast<HelloData *>(data.get())->getData()[0];
        }
        auto count = ctx.getMetric().scheduleCount();
        std::string dataStr = getNodeName() + "_" + std::to_string(count) + "#" + input;
        DataPtr outData(new HelloData(dataStr));
        ctx.setOutput(0, outData, count > 10);
        return EC_NONE;
    }
private:
    KERNEL_DEPEND_DECLARE();
private:
    KERNEL_DEPEND_ON_ID(A, _a, "A", false);
};

class TestResourceOverride : public Kernel {
public:
    TestResourceOverride()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("TestResourceOverride")
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode init(KernelInitContext &ctx) override {
        if (!_replaceA) {
            NAVI_KERNEL_LOG(ERROR, "invalid replaceA");
            return EC_ABORT;
        }
        if (!dynamic_cast<ReplaceA *>(_a)) {
            NAVI_KERNEL_LOG(ERROR, "invalid resource type [%s]", typeid(*_a).name());
            return EC_ABORT;
        }
        if (!dynamic_cast<ReplaceAA *>(_replaceA)) {
            NAVI_KERNEL_LOG(ERROR, "invalid resource type [%s]", typeid(*_replaceA).name());
            return EC_ABORT;
        }
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        auto count = ctx.getMetric().scheduleCount();
        std::string dataStr = getNodeName() + "_" + std::to_string(count);
        DataPtr outData(new HelloData(dataStr));
        ctx.setOutput(0, outData, count > 10);
        return EC_NONE;
    }
private:
    KERNEL_DEPEND_DECLARE();
private:
    KERNEL_DEPEND_ON_ID(A, _a, "A", true);
    KERNEL_DEPEND_ON_ID(ReplaceA, _replaceA, "ReplaceA", true);
};

class TestResourceOverride2 : public Kernel {
public:
    TestResourceOverride2()
    {
    }
public:
    void def(KernelDefBuilder &builder) const override {
        builder.name("TestResourceOverride2")
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    ErrorCode init(KernelInitContext &ctx) override {
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        auto count = ctx.getMetric().scheduleCount();
        std::string dataStr = getNodeName() + "_" + std::to_string(count);
        DataPtr outData(new HelloData(dataStr));
        ctx.setOutput(0, outData, count > 10);
        return EC_NONE;
    }
private:
    KERNEL_DEPEND_DECLARE();
private:
    KERNEL_DEPEND_ON_ID(A, _a, "A", true);
};

class FakeUnionKernel : public navi::Kernel {
public:
    FakeUnionKernel() {}
    ~FakeUnionKernel() {}

public:
    void def(navi::KernelDefBuilder &builder) const override {
        builder.name("FakeUnionKernel")
            .inputGroup("input1", CHAR_ARRAY_TYPE_ID, navi::IT_OPTIONAL)
            .output("output1", CHAR_ARRAY_TYPE_ID);
    }
    bool config(navi::KernelConfigContext &ctx) override {
        return true;
    }

    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override {
        navi::IndexType inputGroupIndex = 0;
        navi::GroupDatas datas;
        if (!runContext.getGroupInput(inputGroupIndex, datas)) {
            NAVI_KERNEL_LOG(ERROR, "get input group failed");
            return navi::EC_ABORT;
        }
        if (datas.eof()) {
            navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
            runContext.setOutput(outputIndex, nullptr, true);
        }
        return navi::EC_NONE;
    }
};

REGISTER_KERNEL(SourceKernel);
REGISTER_KERNEL(AsyncInInitSourceKernel);
REGISTER_KERNEL(AsyncInComputeSourceKernel);
REGISTER_KERNEL(AsyncOptionalMixKernel);
REGISTER_KERNEL(AsyncWithoutOutputSourceKernel);
REGISTER_KERNEL(AsyncInitSlowSourceKernel);
// REGISTER_KERNEL(TFSourceKernel);
REGISTER_KERNEL(HelloKernelOld);
REGISTER_KERNEL(HelloKernel);
REGISTER_KERNEL(WorldKernel);
REGISTER_KERNEL(MergeKernel);
REGISTER_KERNEL(SplitKernel);
REGISTER_KERNEL(AbortKernel);
REGISTER_KERNEL(StopKernel);
REGISTER_KERNEL(InitAbortKernel);
REGISTER_KERNEL(PartCancelSourceKernel);
REGISTER_KERNEL(PartTimeoutSourceKernel);
REGISTER_KERNEL(SleepKernel);
REGISTER_KERNEL(SubGraphKernel);

REGISTER_KERNEL(StringSplitKernel);
REGISTER_KERNEL(StringMergeKernel);

REGISTER_KERNEL(DeadKernel);
REGISTER_KERNEL(BadInputTypeKernel);
REGISTER_KERNEL(BadOutputTypeKernel);
REGISTER_KERNEL(IntArrayTypeKernel);

REGISTER_KERNEL(IdentityTestKernel);
REGISTER_KERNEL(ForkGraphKernel);
REGISTER_KERNEL(ForkGraphWithOverrideKernel);

REGISTER_KERNEL(FakeUnionKernel);
REGISTER_KERNEL(NamedDataKernel);
REGISTER_KERNEL(NamedDataKernelSub);

REGISTER_KERNEL(TestScopeTerminator);

REGISTER_KERNEL(TestResourceOverride);
REGISTER_KERNEL(TestResourceOverride2);

}
