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
#include "navi/ops/ResourceSaveKernel.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/ops/ResourceData.h"
#include "navi/ops/ResourceKernelDef.h"

namespace navi {

ResourceSaveKernel::ResourceSaveKernel() {
}

ResourceSaveKernel::~ResourceSaveKernel() {
}

void ResourceSaveKernel::def(KernelDefBuilder &builder) const {
    builder.name(RESOURCE_SAVE_KERNEL)
        .inputGroup(RESOURCE_SAVE_KERNEL_INPUT, RESOURCE_DATA_TYPE_ID)
        .output(RESOURCE_SAVE_KERNEL_OUTPUT, "");
}

ErrorCode ResourceSaveKernel::init(KernelInitContext &ctx) {
    return EC_NONE;
}

ErrorCode ResourceSaveKernel::compute(KernelComputeContext &ctx) {
    GroupDatas datas;
    if (!ctx.getGroupInput(0, datas)) {
        NAVI_KERNEL_LOG(ERROR, "get input failed");
    }
    bool hasWait = false;
    for (const auto &streamData : datas) {
        if (!streamData.hasValue) {
            continue;
        }
        auto resourceData = std::dynamic_pointer_cast<ResourceData>(streamData.data);
        if (!resourceData) {
            NAVI_KERNEL_LOG(ERROR, "create resource failed, unknown error");
            return EC_ABORT;
        }
        const auto &resource = resourceData->_resource;
        if (resource) {
            NAVI_KERNEL_LOG(DEBUG, "create resource success [%s] [%p]", resource->getName().c_str(), resource.get());
            _resourceMap.overrideResource(resource);
            continue;
        }
        if (!resourceData->_require) {
            NAVI_KERNEL_LOG(DEBUG, "not required resource ignored [%s]", resourceData->_name.c_str());
            continue;
        }
        if (resourceData->_wait) {
            NAVI_KERNEL_LOG(INFO, "need wait for resource creation [%s]", resourceData->_name.c_str());
            hasWait = true;
            continue;
        }
        if (resourceData->validate(false)) {
            NAVI_KERNEL_LOG(DEBUG, "resource ignored [%s]", resourceData->_name.c_str());
            continue;
        }
        NAVI_KERNEL_LOG(ERROR, "create resource failed [%s]", resourceData->_name.c_str());
        return EC_ABORT;
    }
    if (hasWait) {
        NAVI_KERNEL_LOG(INFO, "save skipped, need wait");
        return EC_NONE;
    }
    if (!ctx._subGraph->saveResource(_resourceMap)) {
        NAVI_KERNEL_LOG(ERROR, "save resource failed");
        return EC_ABORT;
    }
    auto data = std::make_shared<ResourceData>();
    data->_require = false;
    ctx.setOutput(0, data, datas.eof());
    return EC_NONE;
}

REGISTER_KERNEL(ResourceSaveKernel);

}

