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

#include "autil/Lock.h"
#include "navi/config/NaviConfig.h"
#include "navi/engine/CreatorManager.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/MultiLayerResourceMap.h"
#include "navi/engine/NamedData.h"
#include "navi/engine/Resource.h"
#include <unordered_set>

namespace multi_call {
class GigRpcServer;
}

namespace navi {

class ResourceCreator;
class Biz;
struct PartSummary;
class ProduceInfo;
class NaviRpcServerR;

typedef std::unordered_map<ResourceStage, std::map<std::string, bool> >
    ResourceStageMap;

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
    bool postInit();
    bool saveResource(const ResourceMap &resourceMap);
    bool collectPublish(std::vector<multi_call::ServerBizTopoInfo> &infoVec);
    void startPublish(multi_call::GigRpcServer *gigRpcServer);
    bool publishResource(ResourceManager *buildinResourceManager,
                         ResourceManager *bizResourceManager,
                         const ResourceStageMap &resourceStageMap);
    bool validateResource(const ResourceStageMap &resourceStageMap) const;
    void collectResourceMap(MultiLayerResourceMap &multiLayerResourceMap) const;
    ErrorCode createResource(const std::string &name,
                             ResourceStage minStage,
                             const MultiLayerResourceMap &multiLayerResourceMap,
                             const SubNamedDataMap *namedDataMap,
                             const ProduceInfo &produceInfo,
                             NaviConfigContext *ctx,
                             bool checkReuse,
                             Node *requireKernelNode,
                             ResourcePtr &resource);
    ResourcePtr buildKernelResourceRecur(const std::string &name,
                                         KernelConfigContext *ctx,
                                         const SubNamedDataMap *namedDataMap,
                                         const ResourceMap &nodeResourceMap,
                                         Node *requireKernelNode,
                                         ResourceMap &inputResourceMap) const;
    NaviPartId getPartCount() const;
    bool hasConfig(const std::string &name) const;
    void fillSummary(PartSummary &partSummary) const;
private:
    bool validateStage(const ResourceStageMap &resourceStageMap) const;
    ResourcePtr getExistResource(const std::string &name,
                                 const MultiLayerResourceMap &multiLayerResourceMap);
    bool validateDepend(const ResourcePtr &resource,
                        const MultiLayerResourceMap &multiLayerResourceMap) const;
    ErrorCode collectDependMap(const ResourceCreator *creator,
                               const MultiLayerResourceMap &multiLayerResourceMap,
                               ResourceMap &dependResourceMap);
    ResourcePtr getResourceFromOld(const std::string &name) const;
    ResourcePtr newAndConfigResource(const std::string &name,
                                     const ResourceCreator *creator,
                                     NaviConfigContext *ctx,
                                     void *&baseAddr) const;
    ErrorCode initResource(const ResourcePtr &resourcePtr,
                           void *baseAddr,
                           const SubNamedDataMap *namedDataMap,
                           const ProduceInfo *produceInfo,
                           const ResourceCreator *creator,
                           NaviConfigContext *configCtx,
                           Node *requireKernelNode) const;
    bool collectStaticGraphInfo();
    bool updatePublishMeta();
    bool getPublishMetaMap(std::map<std::string, std::string> &metaMap) const;
    bool addStaticGraphMeta(std::map<std::string, std::string> &metaMap) const;
private:
    static DataPtr getNamedData(const SubNamedDataMap *namedDataMap,
                                const std::string &name);
public:
    static inline bool bindResource(const std::string &name, void *obj,
                             const ResourceMap &baseResourceMap,
                             const DynamicResourceInfoMap &dynamicGroupMap,
                             const BindInfos &bindInfos,
                             const std::vector<int32_t> &selectorResult,
                             const char *bindType);
    template <typename T>
    static bool bindNamedData(const std::string &name, T &obj,
                              const SubNamedDataMap *namedDataMap,
                              const BindInfos &bindInfos,
                              const char *bindType);

public:
    TestMode getTestMode() const;

private:
    const Biz *_biz;
    std::string _bizName;
    std::string _configPath;
    NaviPartId _partCount;
    NaviPartId _partId;
    const CreatorManager &_creatorManager;
    const NaviRegistryConfigMap &_configMap;
    ResourceMap _resourceMap;
    ResourceMap _mergedResourceMap;
    ResourceManager *_oldResourceManager;
    mutable autil::ThreadMutex _publishLock;
    multi_call::GigRpcServer *_gigRpcServer;
    std::shared_ptr<std::unordered_map<std::string, StaticGraphInfoPtr>> _staticGraphInfoMap;
};

inline bool ResourceManager::bindResource(
    const std::string &name, void *obj,
    const ResourceMap &baseResourceMap,
    const DynamicResourceInfoMap &dynamicGroupMap,
    const BindInfos &bindInfos,
    const std::vector<int32_t> &selectorResult,
    const char *bindType)
{
    NAVI_KERNEL_LOG(SCHEDULE1, "start bind resource for %s [%s] at [%p], baseResourceMap: %s",
                    bindType, name.c_str(), obj,
                    autil::StringUtil::toString(baseResourceMap).c_str());
    for (const auto &info : bindInfos.staticBindVec) {
        const auto &dependName = info.name;
        auto dependResource = baseResourceMap.getResource(dependName).get();
        NAVI_KERNEL_LOG(SCHEDULE1,
                        "bind depend resource for %s [%s] at [%p], "
                        "dependName [%s] dependResource [%p] require [%d]",
                        bindType, name.c_str(), obj, dependName.c_str(),
                        dependResource, info.require);
        if (!dependResource && info.require) {
            NAVI_KERNEL_LOG(
                WARN,
                "init %s [%s] failed, lack required depend resource [%s]",
                bindType, name.c_str(), dependName.c_str());
            return false;
        }
        if (info.binder && !info.binder(obj, dependResource)) {
            NAVI_KERNEL_LOG(WARN,
                            "init %s [%s] failed, bind depend resource [%s] "
                            "failed, type cast failed, actual type [%s]",
                            bindType,
                            name.c_str(),
                            dependName.c_str(),
                            dependResource ? typeid(*dependResource).name() : "");
            return false;
        }
    }
    const auto &dynamicBindVec = bindInfos.dynamicBindVec;
    for (const auto &info : dynamicBindVec) {
        const auto &group = info.group;
        auto it = dynamicGroupMap.find(group);
        if (dynamicGroupMap.end() == it) {
            NAVI_KERNEL_LOG(WARN,
                            "dynamic group [%s] not exist in def of %s [%s]",
                            group.c_str(), bindType, name.c_str());
            return false;
        }
        auto dynamicGroupDef = it->second;
        auto count = dynamicGroupDef->depend_resources_size();
        for (int i = 0; i < count; i++) {
            const auto &dependDef = dynamicGroupDef->depend_resources(i);
            const auto &dependName = dependDef.name();
            auto require = dependDef.require();
            auto dependResource = baseResourceMap.getResource(dependName).get();
            NAVI_KERNEL_LOG(SCHEDULE1,
                            "bind depend resource for %s [%s] at [%p], "
                            "dependName [%s] dependResource [%p] require [%d]",
                            bindType, name.c_str(), obj, dependName.c_str(),
                            dependResource, require);
            if (!dependResource && require) {
                NAVI_KERNEL_LOG(
                    WARN,
                    "init %s [%s] failed, lack required depend resource [%s]",
                    bindType, name.c_str(), dependName.c_str());
                return false;
            }
            if (info.binder && !info.binder(obj, dependResource, nullptr)) {
                NAVI_KERNEL_LOG(
                    WARN,
                    "init %s [%s] failed, bind depend resource [%s] "
                    "failed, type cast failed",
                    bindType, name.c_str(), dependName.c_str());
                return false;
            }
        }
    }
    for (int32_t i = 0; i < bindInfos.selectBindVec.size(); i++) {
        const auto &info = bindInfos.selectBindVec[i];
        auto selectIndex = selectorResult[i];
        Resource *dependResource = nullptr;
        if (selectIndex < 0) {
            if (info.binder && !info.binder(obj, dependResource)) {
                NAVI_KERNEL_LOG(WARN,
                                "init %s [%s] failed, bind failed, select negative index [%d] on required depend "
                                "resource, selector [%d]",
                                bindType,
                                name.c_str(),
                                selectIndex,
                                i);
                return false;
            }
            continue;
        }
        const auto &dependName = info.candidates[selectIndex];
        dependResource = baseResourceMap.getResource(dependName).get();
        NAVI_KERNEL_LOG(SCHEDULE1,
                        "bind depend resource for %s [%s] at [%p], "
                        "dependName [%s] dependResource [%p] require [%d]",
                        bindType, name.c_str(), obj, dependName.c_str(),
                        dependResource, info.require);
        if (!dependResource && info.require) {
            NAVI_KERNEL_LOG(WARN,
                            "init %s [%s] failed, lack selected required depend resource [%s], selector [%d]",
                            bindType,
                            name.c_str(),
                            dependName.c_str(),
                            i);
            return false;
        }
        if (info.binder && !info.binder(obj, dependResource)) {
            NAVI_KERNEL_LOG(WARN,
                            "init %s [%s] failed, bind selected depend resource [%s] "
                            "failed, type cast failed, selector [%d]",
                            bindType, name.c_str(), dependName.c_str(), i);
            return false;
        }
    }
    return true;
}

template <typename T>
bool ResourceManager::bindNamedData(
    const std::string &name, T &obj,
    const SubNamedDataMap *namedDataMap,
    const BindInfos &bindInfos,
    const char *bindType)
{
    for (const auto &info : bindInfos.namedDataBindVec) {
        const auto &dataName = info.name;
        auto data = getNamedData(namedDataMap, dataName);
        if (!data && info.require) {
            NAVI_KERNEL_LOG(WARN,
                            "init %s [%s] failed, lack required named data [%s]",
                            bindType, name.c_str(), dataName.c_str());
            return false;
        }
        if (info.binder && !info.binder(&obj, data.get())) {
            NAVI_KERNEL_LOG(WARN,
                            "init %s [%s] failed, bind named data [%s] failed, "
                            "type cast failed",
                            bindType, name.c_str(), dataName.c_str());
            return false;
        }
    }
    return true;
}

NAVI_TYPEDEF_PTR(ResourceManager);

}

#endif //NAVI_RESOURCEMANAGER_H
