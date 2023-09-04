#pragma once

#include "autil/StringUtil.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/PortMergeKernel.h"
#include "navi/engine/PortSplitKernel.h"
#include "navi/example/TestData.h"
#include "navi/example/TestResource.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/resource/GraphMemoryPoolR.h"
// #include "navi/tensorflow/TensorData.h"
#include "navi/util/CommonUtil.h"
#include "navi/util/NaviClosure.h"
#include "autil/Thread.h"
#include <exception>
#include <unistd.h>
#include <vector>

namespace navi {

class AsyncInInitSourceKernel : public Kernel {
public:
    AsyncInInitSourceKernel() = default;
public:
    void def(KernelDefBuilder &builder) const override;
    ErrorCode init(KernelInitContext &ctx) override;
    ErrorCode compute(KernelComputeContext &ctx) override;
    AsyncPipePtr &getAsyncPipe() {
        return _asyncPipe;
    }
private:
    AsyncPipePtr _asyncPipe;
public:
    static AsyncPipePtr lastPipe;
};

class AsyncInComputeSourceKernel : public Kernel {
public:
    AsyncInComputeSourceKernel() = default;
public:
    void def(KernelDefBuilder &builder) const override;
    ErrorCode compute(KernelComputeContext &ctx) override;
    AsyncPipePtr &getAsyncPipe() {
        return _asyncPipe;
    }
private:
    AsyncPipePtr _asyncPipe;
public:
    static AsyncPipePtr lastPipe;
};

class AsyncWithoutOutputSourceKernel : public Kernel {
public:
    AsyncWithoutOutputSourceKernel() = default;
public:
    void def(KernelDefBuilder &builder) const override;
    ErrorCode compute(KernelComputeContext &ctx) override;
    AsyncPipePtr &getAsyncPipe() {
        return _asyncPipe;
    }
private:
    AsyncPipePtr _asyncPipe;
public:
    static AsyncPipePtr lastPipe;
};

class AsyncInitSlowSourceKernel : public Kernel {
public:
    AsyncInitSlowSourceKernel() = default;

public:
    void def(KernelDefBuilder &builder) const override;
    ErrorCode init(KernelInitContext &ctx) override;
    ErrorCode compute(KernelComputeContext &ctx) override;
    AsyncPipePtr &getAsyncPipe() {
        return _asyncPipe;
    }
private:
    AsyncPipePtr _asyncPipe;
    WaitClosure _waitClosure;
public:
    static AsyncPipePtr lastPipe;
    static WaitClosure *lastClosure;
};

class AsyncOptionalMixKernel : public Kernel {
public:
    AsyncOptionalMixKernel() = default;
public:
    void def(KernelDefBuilder &builder) const override;
    ErrorCode init(KernelInitContext &ctx) override;
    ErrorCode compute(KernelComputeContext &ctx) override;
    AsyncPipePtr &getAsyncPipe() {
        return _asyncPipe;
    }
private:
    AsyncPipePtr _asyncPipe;
public:
    static AsyncPipePtr lastPipe;
};

}
