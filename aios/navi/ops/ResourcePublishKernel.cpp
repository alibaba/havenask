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
#include "navi/ops/ResourcePublishKernel.h"
#include "navi/ops/ResourceKernelDef.h"
#include "navi/ops/ResourceData.h"
#include "navi/engine/LocalSubGraph.h"

namespace navi {

ResourcePublishKernel::ResourcePublishKernel() {
}

ResourcePublishKernel::~ResourcePublishKernel() {
}

void ResourcePublishKernel::def(KernelDefBuilder &builder) const {
    builder.name(RESOURCE_PUBLISH_KERNEL)
        .inputGroup(RESOURCE_PUBLISH_KERNEL_INPUT, RESOURCE_DATA_TYPE_ID)
        .output(RESOURCE_PUBLISH_KERNEL_OUTPUT, "");
}

bool ResourcePublishKernel::config(KernelConfigContext &ctx) {
    return true;
}

ErrorCode ResourcePublishKernel::init(KernelInitContext &ctx) {
    return EC_NONE;
}

ErrorCode ResourcePublishKernel::compute(KernelComputeContext &ctx) {
    GroupDatas datas;
    if (!ctx.getGroupInput(0, datas)) {
        NAVI_KERNEL_LOG(ERROR, "get input failed");
    }
    if (!ctx._subGraph->publishResource()) {
        NAVI_KERNEL_LOG(ERROR, "publish resource failed");
        return EC_ABORT;
    }
    auto data = std::make_shared<ResourceData>();
    data->_require = false;
    ctx.setOutput(0, data, datas.eof());
    return EC_NONE;
}

REGISTER_KERNEL(ResourcePublishKernel);

}
