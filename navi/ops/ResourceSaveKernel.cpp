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
        .inputGroup(RESOURCE_SAVE_KERNEL_INPUT,
                    ResourceDataType::RESOURCE_DATA_TYPE_ID)
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
    ResourceMap resourceMap;
    for (const auto &streamData : datas) {
        auto resourceData = std::dynamic_pointer_cast<ResourceData>(streamData.data);
        if (!resourceData) {
            NAVI_KERNEL_LOG(ERROR, "create resource failed, unknown error");
            return EC_ABORT;
        }
        if (!resourceData->isValid()) {
            NAVI_KERNEL_LOG(ERROR, "create resource failed [%s]",
                            resourceData->_name.c_str());
            return EC_ABORT;
        } else {
            const auto &resource = resourceData->_resource;
            if (resource) {
                NAVI_KERNEL_LOG(DEBUG, "create resource success [%s] [%p]",
                                resource->getName().c_str(), resource.get());
                resourceMap.addResource(resource);
            }
        }
    }
    if (!ctx._subGraph->saveInitResource(resourceMap)) {
        NAVI_KERNEL_LOG(ERROR, "save init resource failed");
        return EC_ABORT;
    }
    ctx.setOutput(0, nullptr, true);
    return EC_NONE;
}

REGISTER_KERNEL(navi::ResourceSaveKernel);

}

