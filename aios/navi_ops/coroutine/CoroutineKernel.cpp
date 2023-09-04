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
#include "navi_ops/coroutine/CoroutineKernel.h"
#include "navi_ops/coroutine/NaviAsyncPipeExecutor.h"

namespace navi_ops {

AUTIL_LOG_SETUP(navi_ops, CoroutineKernel);

CoroutineKernel::CoroutineKernel() {}

CoroutineKernel::~CoroutineKernel() {
}

navi::ErrorCode CoroutineKernel::init(navi::KernelInitContext &ctx) {
    auto asyncPipe = ctx.createAsyncPipe();
    if (!asyncPipe) {
        AUTIL_LOG(ERROR, "create async pipe failed");
        return navi::EC_ABORT;
    }
    _asyncPipe = asyncPipe;
    return navi::EC_NONE;
}

navi::ErrorCode CoroutineKernel::compute(navi::KernelComputeContext &ctx) {
    bool eof = false;
    navi::DataPtr data;
    _asyncPipe->getData(data, eof);
    NAVI_KERNEL_LOG(TRACE3, "after get pipe data [%p] eof [%d]", data.get(), eof);
    auto funcData = std::dynamic_pointer_cast<FunctionData>(data);
    if (funcData) {
        funcData->run();
    }
    if (!funcData || eof) {
        // ActivationData
        return computeEnd(ctx);
    }
    return navi::EC_NONE;
}

} // namespace navi_ops
