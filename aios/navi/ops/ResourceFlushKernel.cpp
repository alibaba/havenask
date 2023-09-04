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
#include "navi/ops/ResourceFlushKernel.h"
#include "navi/ops/ResourceKernelDef.h"
#include "navi/ops/ResourceData.h"

namespace navi {

ResourceFlushKernel::ResourceFlushKernel() {
}

ResourceFlushKernel::~ResourceFlushKernel() {
}

void ResourceFlushKernel::def(KernelDefBuilder &builder) const {
    builder.name(RESOURCE_FLUSH_KERNEL)
        .output(RESOURCE_FLUSH_KERNEL_OUTPUT, ResourceFlushDataType::TYPE_ID);
}

bool ResourceFlushKernel::config(KernelConfigContext &ctx) {
    return true;
}

ErrorCode ResourceFlushKernel::init(KernelInitContext &ctx) {
    return EC_NONE;
}

ErrorCode ResourceFlushKernel::compute(KernelComputeContext &ctx) {
    if (!readInput()) {
        return EC_ABORT;;
    }
    if (!createData(ctx)) {
        NAVI_KERNEL_LOG(ERROR, "create flush data failed");
        return EC_ABORT;
    }
    NAVI_KERNEL_LOG(INFO, "flush output");
    ctx.setOutput(0, std::dynamic_pointer_cast<Data>(_data), false);
    return EC_NONE;
}

bool ResourceFlushKernel::readInput() {
    if (!_data) {
        return true;
    }
    const auto &pipe = _data->_pipe;
    while (true) {
        DataPtr data;
        bool eof = false;
        if (!pipe->getData(data, eof)) {
            break;
        }
        auto flushData = dynamic_cast<ResourceData *>(data.get());
        if (!flushData) {
            NAVI_KERNEL_LOG(ERROR, "wrong flush data");
            return false;
        }
        NAVI_KERNEL_LOG(INFO,
                        "flush info received, producer [%s], produced resource [%s] [%p]",
                        flushData->_name.c_str(),
                        flushData->_resource ? flushData->_resource->getName().c_str() : "",
                        flushData->_resource.get());
    }
    return true;
}

bool ResourceFlushKernel::createData(KernelComputeContext &ctx) {
    if (_data) {
        return true;
    }
    auto pipe = ctx.createAsyncPipe(ActivateStrategy::AS_OPTIONAL);
    if (!pipe) {
        NAVI_KERNEL_LOG(ERROR, "create flush pipe failed");
        return false;
    }
    _data = std::make_shared<ResourceFlushData>();
    _data->_pipe = pipe;
    return true;
}

REGISTER_KERNEL(ResourceFlushKernel);

}
