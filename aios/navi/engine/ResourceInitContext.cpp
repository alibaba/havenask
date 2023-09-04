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
#include "navi/log/NaviLogger.h"
#include "navi/engine/ResourceInitContext.h"
#include "navi/engine/Node.h"
#include "navi/engine/ResourceManager.h"
#include "navi/ops/ResourceNotifyData.h"
#include "navi/ops/ResourceData.h"

namespace navi {

bool ProduceNotifier::notify(const std::shared_ptr<Resource> &produceResult) {
    if (!_info._produce || !_info._flush) {
        NAVI_KERNEL_LOG(ERROR, "notify failed, null pipe");
        return false;
    }
    if (produceResult) {
        // init def
        produceResult->getName();
    }
    auto notifyData = std::make_shared<ResourceNotifyData>();
    notifyData->_resource = _resource;
    notifyData->_result = produceResult;
    auto ec = _info._produce->setData(std::dynamic_pointer_cast<Data>(notifyData));
    if (ec != EC_NONE) {
        NAVI_KERNEL_LOG(ERROR, "set produce data failed");
        return false;
    }
    auto flushData = std::make_shared<ResourceData>();
    flushData->_name = _name;
    flushData->_resource = produceResult;
    ec = _info._flush->setData(flushData);
    return (ec == EC_NONE);
}

ResourceInitContext::ResourceInitContext() {
}

ResourceInitContext::ResourceInitContext(
        const ResourceManager *resourceManager,
        const ResourceCreator *creator,
        const std::string &configPath,
        const std::string &bizName,
        NaviPartId partCount,
        NaviPartId partId,
        const ResourceMap *resourceMap,
        const ProduceNotifierPtr &notifier,
        const NaviConfigContext &configContext,
        const SubNamedDataMap *namedDataMap,
        Node *requireKernelNode)
    : _resourceManager(resourceManager)
    , _creator(creator)
    , _configPath(&configPath)
    , _bizName(&bizName)
    , _partCount(partCount)
    , _partId(partId)
    , _resourceMap(resourceMap)
    , _notifier(notifier)
    , _configContext(configContext)
    , _namedDataMap(namedDataMap)
    , _requireKernelNode(requireKernelNode)
{
}

ResourceInitContext::~ResourceInitContext() {
}

Resource *ResourceInitContext::getDependResource(const std::string &name) const {
    return _resourceMap->getResource(name).get();
}

const ResourceMap &ResourceInitContext::getDependResourceMap() const {
    return *_resourceMap;
}

const std::string &ResourceInitContext::getConfigPath() const {
    return *_configPath;
}

ResourcePtr
ResourceInitContext::buildR(const std::string &name, KernelConfigContext ctx, ResourceMap &inputResourceMap) const {
    const auto &enableSet = _creator->getEnableBuildResourceSet();
    if (enableSet.end() == enableSet.find(name)) {
        NAVI_KERNEL_LOG(ERROR, "can't build resource [%s], not enabled", name.c_str());
        return nullptr;
    }
    return _resourceManager->buildKernelResourceRecur(
        name, &ctx, _namedDataMap, *_resourceMap, _requireKernelNode, inputResourceMap);
}

AsyncPipePtr ResourceInitContext::createRequireKernelAsyncPipe(ActivateStrategy activateStrategy) const {
    if (!_requireKernelNode) {
        NAVI_KERNEL_LOG(ERROR, "only RS_KERNEL resource can create async pipe");
        return nullptr;
    }
    return _requireKernelNode->createAsyncPipe(activateStrategy);
}

}
