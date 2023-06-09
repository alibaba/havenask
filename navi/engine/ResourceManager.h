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
#ifndef NAVI_RESOURCEMANAGER_H
#define NAVI_RESOURCEMANAGER_H

#include "navi/config/NaviConfig.h"
#include "navi/engine/CreatorManager.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceMap.h"
#include "autil/Lock.h"
#include <unordered_set>

namespace autil {
namespace mem_pool {
class Pool;
}
}

namespace navi {

class ResourceCreator;
class Biz;

class ResourceManager
{
public:
    ResourceManager(const Biz *biz,
                    const std::string &bizName,
                    const std::string &configPath,
                    NaviPartId partCount,
                    NaviPartId partId,
                    const CreatorManager &creatorManager,
                    const NaviRegistryConfigMap &configMap);
    ~ResourceManager();
private:
    ResourceManager(const ResourceManager &);
    ResourceManager &operator=(const ResourceManager &);
public:
    bool preInit(ResourceManager *oldResourceManager);
    bool postInit(ResourceManager *defaultResourceManager);
    bool addResource(const ResourceMap &resourceMap);
    void collectResourceMap(MultiLayerResourceMap &multiLayerResourceMap) const;
    ResourcePtr createResource(const std::string &name,
                               const MultiLayerResourceMap &multiLayerResourceMap);
    NaviPartId getPartCount() const;
    bool hasConfig(const std::string &name) const;
private:
    ResourcePtr getExistResource(const std::string &name,
                                 const MultiLayerResourceMap &multiLayerResourceMap);
    ResourcePtr getResourceFromOld(const std::string &name) const;
    ResourcePtr newAndConfigResource(
            const std::string &name,
            const ResourceCreator *creator,
            autil::mem_pool::Pool *pool) const;
    ResourcePtr initResource(const ResourcePtr &resourcePtr,
                             const ResourceMap &baseResourceMap,
                             const ResourceBindInfos &binderInfos) const;
public:
    template <typename T>
    static bool bindResource(const std::string &name, T &obj,
                             const ResourceMap &baseResourceMap,
                             const ResourceBindInfos &binderInfos);
private:
    const Biz *_biz;
    std::string _bizName;
    std::string _configPath;
    NaviPartId _partCount;
    NaviPartId _partId;
    const CreatorManager &_creatorManager;
    const NaviRegistryConfigMap &_configMap;
    ResourceMap _resourceMap;
    ResourceManager *_oldResourceManager;
};

template <typename T>
bool ResourceManager::bindResource(
    const std::string &name, T &obj,
    const ResourceMap &baseResourceMap,
    const ResourceBindInfos &binderInfos)
{
    NAVI_KERNEL_LOG(SCHEDULE1, "start bind resource for [%s] at [%p], baseResourceMap: %s",
                    name.c_str(), &obj,
                    autil::StringUtil::toString(baseResourceMap).c_str());
    std::set<Resource *> staticMatchSet;
    for (const auto &info : binderInfos.staticBinderVec) {
        const auto &dependName = info.name;
        auto dependResource = baseResourceMap.getResource(dependName).get();
        NAVI_KERNEL_LOG(SCHEDULE1,
                        "bind depend resource for [%s] at [%p], "
                        "dependName [%s] dependResource [%p] require [%d]",
                        name.c_str(), &obj, dependName.c_str(), dependResource,
                        info.require);
        if (!dependResource && info.require) {
            NAVI_KERNEL_LOG(
                ERROR, "init [%s] failed, lack required depend resource [%s]",
                name.c_str(), dependName.c_str());
            return false;
        }
        staticMatchSet.insert(dependResource);
        if (info.binder && !info.binder(&obj, dependResource)) {
            NAVI_KERNEL_LOG(ERROR,
                            "init [%s] failed, bind depend resource [%s] "
                            "failed, type cast failed",
                            name.c_str(), dependName.c_str());
            return false;
        }
    }
    const auto &dynamicBinderVec = binderInfos.dynamicBinderVec;
    const auto &map = baseResourceMap.getMap();
    for (const auto &pair : map) {
        const auto &resource = pair.second;
        if (staticMatchSet.end() != staticMatchSet.find(resource.get())) {
            NAVI_KERNEL_LOG(
                SCHEDULE2,
                "init [%s] ignore static bind resource [%s] in dynamic binder",
                name.c_str(), pair.first.c_str());
            continue;
        }
        bool match = false;
        for (const auto &info : dynamicBinderVec) {
            if (!info.binder) {
                NAVI_KERNEL_LOG(
                    ERROR,
                    "init [%s] failed, null dynamic resource binder in builder",
                    name.c_str());
                return false;
            }
            if (info.binder(&obj, resource.get(), nullptr)) {
                NAVI_KERNEL_LOG(SCHEDULE1,
                                "init [%s] dynamic bind resource [%s] success",
                                name.c_str(), pair.first.c_str());
                match = true;
                break;
            }
        }
        if (!match) {
            NAVI_KERNEL_LOG(ERROR,
                            "init [%s] failed, resource [%s] not required "
                            "by def and can't bind to any dynamic binder",
                            name.c_str(), pair.first.c_str());
            return false;
        }
    }
    return true;
}

NAVI_TYPEDEF_PTR(ResourceManager);

}

#endif //NAVI_RESOURCEMANAGER_H
