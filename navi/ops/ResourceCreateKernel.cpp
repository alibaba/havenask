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
#include "navi/ops/ResourceCreateKernel.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/ops/ResourceData.h"
#include "navi/ops/ResourceKernelDef.h"

namespace navi {

ResourceCreateKernel::ResourceCreateKernel()
    : _require(0)
{
}

ResourceCreateKernel::~ResourceCreateKernel() {
}

void ResourceCreateKernel::def(KernelDefBuilder &builder) const {
    builder.name(RESOURCE_CREATE_KERNEL)
        .output(RESOURCE_CREATE_OUTPUT,
                ResourceDataType::RESOURCE_DATA_TYPE_ID);
}

bool ResourceCreateKernel::config(KernelConfigContext &ctx) {
    ctx.Jsonize(RESOURCE_ATTR_NAME, _resourceName);
    ctx.Jsonize(RESOURCE_ATTR_REQUIRE, _require, _require);
    return true;
}

ErrorCode ResourceCreateKernel::init(KernelInitContext &ctx) {
    return EC_NONE;
}

ErrorCode ResourceCreateKernel::compute(KernelComputeContext &ctx) {
    MultiLayerResourceMap multiLayerResourceMap;
    multiLayerResourceMap.addResourceMap(&ctx.getDependResourceMap());
    ctx._subGraph->collectResourceMap(multiLayerResourceMap);
    auto resource =
        ctx._subGraph->createResource(_resourceName, multiLayerResourceMap);
    NAVI_KERNEL_LOG(TRACE3, "create resource [%s] [%p]", _resourceName.c_str(),
                    resource.get());
    auto resourceData = new ResourceData();
    resourceData->_name = _resourceName;
    resourceData->_require = (bool)_require;
    resourceData->_resource = std::move(resource);
    ctx.setOutput(0, DataPtr(resourceData), true);
    return EC_NONE;
}

REGISTER_KERNEL(navi::ResourceCreateKernel);

}
