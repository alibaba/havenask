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
#ifndef NAVI_RESOURCEINITCONTEXT_H
#define NAVI_RESOURCEINITCONTEXT_H

#include "navi/engine/ResourceMap.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/engine/AsyncPipe.h"

namespace navi {

class Node;
class ResourceManager;
struct NamedData;
typedef std::unordered_map<std::string, NamedData> SubNamedDataMap;
class ResourceCreator;
class AsyncPipe;
NAVI_TYPEDEF_PTR(AsyncPipe);

class ProduceInfo {
public:
    std::shared_ptr<AsyncPipe> _produce;
    std::shared_ptr<AsyncPipe> _flush;
};

class ProduceNotifier {
public:
    ProduceNotifier() {
    }
    ~ProduceNotifier() {
    }
public:
    bool notify(const std::shared_ptr<Resource> &produceResult);
private:
    friend class ResourceManager;
private:
    std::string _name;
    std::string _produce;
    Resource *_resource;
    ProduceInfo _info;
};

NAVI_TYPEDEF_PTR(ProduceNotifier);

class ResourceInitContext
{
public:
    ResourceInitContext();
    ResourceInitContext(const ResourceManager *resourceManager,
                        const ResourceCreator *creator,
                        const std::string &configPath,
                        const std::string &bizName,
                        NaviPartId partCount,
                        NaviPartId partId,
                        const ResourceMap *resourceMap,
                        const ProduceNotifierPtr &notifier,
                        const NaviConfigContext &configContext,
                        const SubNamedDataMap *namedDataMap,
                        Node *requireKernelNode);
    ~ResourceInitContext();
private:
    ResourceInitContext(const ResourceInitContext &);
public:
    Resource *getDependResource(const std::string &name) const;
    template <typename T>
    T *getDependResource(const std::string &name) const {
        return dynamic_cast<T *>(getDependResource(name));
    }
    const ResourceMap &getDependResourceMap() const;
    const std::string &getBizName() const {
        return *_bizName;
    }
    NaviPartId getPartCount() const {
        return _partCount;
    }
    NaviPartId getPartId() const {
        return _partId;
    }
    const std::string &getConfigPath() const;
    const ProduceNotifierPtr &getProduceNotifier() const {
        return _notifier;
    }
    ResourceConfigContext getConfigContext() const {
        return _configContext;
    }
    std::shared_ptr<Resource> buildR(const std::string &name, KernelConfigContext ctx, ResourceMap &inputResourceMap) const;
    AsyncPipePtr createRequireKernelAsyncPipe(ActivateStrategy activateStrategy = AS_REQUIRED) const;
private:
    const ResourceManager *_resourceManager = nullptr;
    const ResourceCreator *_creator = nullptr;
    const std::string *_configPath = nullptr;
    const std::string *_bizName = nullptr;
    NaviPartId _partCount = INVALID_NAVI_PART_COUNT;
    NaviPartId _partId = INVALID_NAVI_PART_ID;
    const ResourceMap *_resourceMap = nullptr;
    ProduceNotifierPtr _notifier;
    NaviConfigContext _configContext;
    const SubNamedDataMap *_namedDataMap = nullptr;
    Node *_requireKernelNode = nullptr;
};

}

#endif //NAVI_RESOURCEINITCONTEXT_H
