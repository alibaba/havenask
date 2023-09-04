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
#include "navi/engine/Node.h"
#include "navi/ops/ResourceKernelDef.h"
#include "navi/ops/ResourceNotifyData.h"
#include "navi/ops/ResourceFlushData.h"

namespace navi {

ResourceCreateKernel::ResourceCreateKernel()
    : _init(false)
    , _require(false)
    , _produceRequire(false)
    , _inputEof(false)
    , _outputEof(false)
    , _produceEof(false)
    , _resourceGroupIndex(INVALID_INDEX)
    , _stage(RS_UNKNOWN)
{
}

ResourceCreateKernel::~ResourceCreateKernel() {
}

void ResourceCreateKernel::def(KernelDefBuilder &builder) const {
    builder.name(RESOURCE_CREATE_KERNEL)
        .output(RESOURCE_CREATE_OUTPUT, RESOURCE_DATA_TYPE_ID)
        .output(RESOURCE_PRODUCE_OUTPUT, RESOURCE_DATA_TYPE_ID);
}

bool ResourceCreateKernel::config(KernelConfigContext &ctx) {
    ctx.Jsonize(RESOURCE_ATTR_NAME, _resourceName);
    ctx.Jsonize(RESOURCE_ATTR_REQUIRE_NODE, _requireNode, _requireNode);
    int64_t init = 0;
    int64_t require = 0;
    int64_t produceRequire = 0;
    ctx.Jsonize(RESOURCE_ATTR_INIT, init, init);
    ctx.Jsonize(RESOURCE_ATTR_REQUIRE, require, require);
    ctx.Jsonize(RESOURCE_ATTR_PRODUCE_NAME, _produceName, _produceName);
    ctx.Jsonize(RESOURCE_ATTR_PRODUCE_REQUIRE, produceRequire, produceRequire);
    _init = init;
    _require = require;
    _produceRequire = produceRequire;
    return true;
}

ErrorCode ResourceCreateKernel::init(KernelInitContext &ctx) {
    _resourceGroupIndex = ctx._node->getResourceInputGroupIndex();
    _multiLayerResourceMap.addResourceMap(&_resourceMap);
    auto localSubGraph = ctx._subGraph;
    _stage = localSubGraph->getResourceStage(_resourceName);
    localSubGraph->collectResourceMap(_multiLayerResourceMap);
    _dependMap = localSubGraph->getResourceDependResourceMap(_resourceName);
    _replaceSet = localSubGraph->getResourceReplacerSet(_resourceName);
    _replaceInfoMap = &localSubGraph->getReplaceInfoMap();
    return EC_NONE;
}

bool ResourceCreateKernel::collectInput(KernelComputeContext &ctx, bool &hasWait) {
    if (INVALID_INDEX == _resourceGroupIndex) {
        _inputEof = true;
        return true;
    }
    GroupDatas datas;
    if (!ctx.doGetGroupInput(_resourceGroupIndex, datas)) {
        NAVI_KERNEL_LOG(ERROR, "collect input resource failed");
        return false;
    }
    bool allEof = true;
    for (const auto &streamData : datas) {
        if (!addData(streamData.data, streamData.eof, hasWait, allEof)) {
            return false;
        }
    }
    _replaceInfoMap->replace(_dependMap, _resourceMap);
    _inputEof = allEof;
    return true;
}

bool ResourceCreateKernel::addData(const DataPtr &data, bool eof, bool &hasWait, bool &allEof) {
    if (!data) {
        if (eof) {
            return true;
        } else {
            NAVI_KERNEL_LOG(ERROR, "null data not eof");
            return false;
        }
    }
    auto resourceData = std::dynamic_pointer_cast<ResourceData>(data);
    if (!resourceData) {
        auto resourceFlushData = dynamic_cast<ResourceFlushData *>(data.get());
        if (!resourceFlushData) {
            NAVI_KERNEL_LOG(ERROR, "wrong data type: %s", data->getTypeId().c_str());
            return false;
        }
        _flushPipe = resourceFlushData->_pipe;
        return true;
    }
    if (resourceData->_wait) {
        hasWait = true;
    }
    NAVI_KERNEL_LOG(DEBUG,
                    "resource [%s], input ResourceData [%s] resource [%p] eof [%d]",
                    _resourceName.c_str(),
                    resourceData->_name.c_str(),
                    resourceData->_resource.get(),
                    eof);
    if (!eof) {
        allEof = false;
    }
    _resourceMap.overrideResource(resourceData->_resource);
    if (_init) {
        _depends[resourceData->_name] = resourceData;
    }
    return true;
}

bool ResourceCreateKernel::createResource(KernelComputeContext &ctx, ResourceDataPtr &output) {
    bool hasWaitInput = false;
    if (!collectInput(ctx, hasWaitInput)) {
        NAVI_KERNEL_LOG(ERROR, "collect input resource failed");
        return false;
    }
    if (_producePipe && !_flushPipe) {
        NAVI_KERNEL_LOG(ERROR, "create resource failed, null flush pipe");
        return false;
    }
    output = createOutput(hasWaitInput);
    if (_init) {
        fillDependAndReplace(ctx, output);
        auto replaceOk = output->validateReplace();
        if (!replaceOk) {
            output->_ec = EC_LACK_REPLACE_RESOURCE;
            NAVI_KERNEL_LOG(WARN, "create resource failed, lack replacer");
            return true;
        }
    }
    if (canReuse()) {
        output->_resource = _output->_resource;
        output->_ec = _output->_ec;
        return true;
    }
    doCreateResource(ctx, output);
    return true;
}

ResourceDataPtr ResourceCreateKernel::createOutput(bool hasWaitInput) const {
    auto output = std::make_shared<ResourceData>();
    output->_name = _resourceName;
    output->_require = (bool)_require;
    output->_wait = hasWaitInput;
    output->_stage = _stage;
    return output;
}

bool ResourceCreateKernel::canReuse() const {
    if (!_output) {
        return false;
    }
    const auto &resource = _output->_resource;
    if (!resource) {
        return false;
    }
    const auto &dependMap = resource->getDepends().getMap();
    for (const auto &pair : dependMap) {
        const auto &name = pair.first;
        const auto &depend = pair.second;
        if (_multiLayerResourceMap.getResource(name) != depend) {
            return false;
        }
    }
    return true;
}

void ResourceCreateKernel::doCreateResource(KernelComputeContext &ctx, ResourceDataPtr &output) {
    ProduceInfo info;
    info._produce = _producePipe;
    info._flush = _flushPipe;
    ResourcePtr resource;
    NaviConfigContext *configContextPtr = nullptr;
    NaviConfigContext configContext;
    Node *requireNode = nullptr;
    if (RS_KERNEL == _stage && !_init) {
        requireNode = ctx._subGraph->getNode(_requireNode);
        if (!requireNode) {
            NAVI_KERNEL_LOG(ERROR, "create resource failed, get require kernel node [%s] failed", _requireNode.c_str());
            return;
        }
        if (!requireNode->getConfigContext(_resourceName, configContext)) {
            NAVI_KERNEL_LOG(ERROR,
                            "create resource failed, get require kernel config context node [%s] failed",
                            _requireNode.c_str());
            return;
        }
        configContextPtr = &configContext;
    }
    ErrorCode ec = EC_NONE;
    if (RS_KERNEL != _stage || !_init) {
        bool checkReuse = _init;
        ec = ctx._subGraph->createResource(
            _resourceName, _multiLayerResourceMap, info, configContextPtr, checkReuse, requireNode, resource);
    }
    output->_resource = std::move(resource);
    output->_ec = ec;
}

void ResourceCreateKernel::fillDependAndReplace(KernelComputeContext &ctx, ResourceDataPtr &resourceData) {
    resourceData->_dependMap = _dependMap;
    resourceData->_dependDatas = _depends;
    resourceData->_replaceSet = _replaceSet;
}

bool ResourceCreateKernel::collectProduceResult(const ResourceDataPtr &output, ResourceDataPtr &produceResult) {
    if (!_producePipe) {
        return true;
    }
    produceResult = std::make_shared<ResourceData>();
    produceResult->_name = _produceName;
    const auto &resource = output->_resource;
    bool ret = false;
    ResourcePtr pipeData;
    while (true) {
        DataPtr data;
        bool eof = false;
        if (!_producePipe->getData(data, eof)) {
            break;
        }
        auto notifyData = dynamic_cast<ResourceNotifyData *>(data.get());
        if (!notifyData || (notifyData->_resource != resource.get())) {
            continue;
        }
        const auto &result = notifyData->_result;
        if (result && (_produceName != result->getName())) {
            NAVI_KERNEL_LOG(ERROR,
                            "wrong produced resource, expect [%s], got [%s]",
                            _produceName.c_str(),
                            result->getName().c_str());
            continue;
        }
        pipeData = result;
    }
    produceResult->_require = _produceRequire;
    if (pipeData) {
        produceResult->_resource = pipeData;
    } else if (_produceResult) {
        produceResult->_resource = _produceResult->_resource;
    }
    if (!resource) {
        NAVI_KERNEL_LOG(
            WARN, "can't produce resource [%s], lack producer [%s]", _produceName.c_str(), _resourceName.c_str());
    }
    if (resource && !produceResult->_resource) {
        produceResult->_wait = true;
    } else {
        produceResult->_wait = false;
    }
    return ret;
}

ErrorCode ResourceCreateKernel::compute(KernelComputeContext &ctx) {
    if (needProduce() && !_producePipe) {
        _producePipe = ctx.createAsyncPipe(ActivateStrategy::AS_OPTIONAL);
        NAVI_KERNEL_LOG(INFO, "resource produce [%s]", _produceName.c_str());
    }
    ResourceDataPtr output;
    if (!createResource(ctx, output)) {
        NAVI_KERNEL_LOG(ERROR, "try create resource failed");
        return EC_ABORT;
    }
    if (!output) {
        NAVI_KERNEL_LOG(DEBUG, "resource [%s] not created", _resourceName.c_str());
        return EC_NONE;
    }
    if (!_outputEof) {
        _outputEof = _inputEof;
        NAVI_KERNEL_LOG(DEBUG,
                        "finish create resource [%s], resource data [%p] resource [%p] eof [%d] ec [%s]",
                        _resourceName.c_str(),
                        output.get(),
                        output ? output->_resource.get() : nullptr,
                        _outputEof,
                        CommonUtil::getErrorString(output ? output->_ec : EC_NONE));
        ctx.setOutput(0, std::dynamic_pointer_cast<Data>(output), _outputEof);
        _output = output;
    }
    ResourceDataPtr produceResult;
    collectProduceResult(output, produceResult);
    if (!_produceEof && _producePipe) {
        auto produceEof = _inputEof && (!needProduce());
        NAVI_KERNEL_LOG(INFO,
                        "pipe [%p], resource produce success, ResourceData[%p], resource[%p], eof [%d]",
                        _producePipe.get(),
                        produceResult.get(),
                        produceResult ? produceResult->_resource.get() : nullptr,
                        produceEof);
        ctx.setOutput(1, std::dynamic_pointer_cast<Data>(produceResult), produceEof);
        _produceEof = produceEof;
        _produceResult = produceResult;
    }
    return EC_NONE;
}

REGISTER_KERNEL(ResourceCreateKernel);

}
