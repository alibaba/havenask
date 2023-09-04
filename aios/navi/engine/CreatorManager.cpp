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
#include "navi/engine/CreatorManager.h"
#include "navi/engine/NaviSnapshotSummary.h"

namespace navi {

CreatorManager::CreatorManager(const std::string &bizName,
                               const std::shared_ptr<CreatorManager> &parentManager)
    : _bizName(bizName)
    , _parentManager(parentManager)
{
}

CreatorManager::~CreatorManager() {
}

bool CreatorManager::init(const std::string &configPath,
                          const std::vector<std::string> &modules,
                          const ModuleManagerPtr &moduleManager)
{
    if (!initModules(configPath, modules, moduleManager)) {
        return false;
    }
    return initRegistrys();
}

const KernelCreator *CreatorManager::getKernelCreator(
        const std::string &kernelName) const
{
    auto it = _mergedKernelCreatorMap.find(kernelName);
    if (_mergedKernelCreatorMap.end() == it) {
        NAVI_KERNEL_LOG(ERROR, "kernel [%s] not registered",
                        kernelName.c_str());
        return nullptr;
    }
    return it->second;
}

const ResourceCreator *CreatorManager::getResourceCreator(
        const std::string &resourceName) const
{
    auto it = _mergedResourceCreatorMap.find(resourceName);
    if (_mergedResourceCreatorMap.end() == it) {
        return nullptr;
    }
    return it->second;
}

const std::map<std::string, bool> *CreatorManager::getResourceDependResourceMap(const std::string &name) const {
    auto creator = getResourceCreator(name);
    if (!creator) {
        return nullptr;
    }
    return &creator->getDependResources();
}

const std::unordered_set<std::string> *CreatorManager::getResourceReplacerSet(const std::string &name) const {
    auto creator = getResourceCreator(name);
    if (!creator) {
        return nullptr;
    }
    return &creator->getReplaceBySet();
}

bool CreatorManager::supportReplace(const std::string &from, const std::string &to) {
    auto resourceCreator = getResourceCreator(from);
    if (!resourceCreator) {
        NAVI_KERNEL_LOG(
            ERROR, "can't replace [%s] by [%s], resource [%s] not registerd", from.c_str(), to.c_str(), from.c_str());
        return false;
    }
    if (!getResourceCreator(to)) {
        NAVI_KERNEL_LOG(
            ERROR, "can't replace [%s] by [%s], resource [%s] not registerd", from.c_str(), to.c_str(), to.c_str());
        return false;
    }
    return resourceCreator->supportReplace(to);
}

ResourceStage CreatorManager::getResourceStage(const std::string &name) const {
    auto resourceCreator = getResourceCreator(name);
    if (!resourceCreator) {
        return RS_UNKNOWN;
    }
    return resourceCreator->getStage();
}

const Type *CreatorManager::getType(const std::string &typeId) const {
    auto it = _mergedTypeMap.find(typeId);
    if (_mergedTypeMap.end() == it) {
        return nullptr;
    }
    return it->second;
}

bool CreatorManager::initModules(const std::string &configPath,
                                 const std::vector<std::string> &modules,
                                 const ModuleManagerPtr &moduleManager)
{
    for (const auto &moduleInfo : modules) {
        auto module = moduleManager->loadModule(configPath, moduleInfo);
        if (!module) {
            NAVI_KERNEL_LOG(ERROR, "load module [%s] failed, configPath [%s]",
                            moduleInfo.c_str(), configPath.c_str());
            return false;
        }
        _moduleMap.emplace(module->getPath(), module);
    }
    return true;
}

bool CreatorManager::initBuildinModule() {
    auto &buildinResource = *CreatorRegistry::buildin(RT_RESOURCE);
    auto &buildinKernel = *CreatorRegistry::buildin(RT_KERNEL);
    auto &buildinType = *CreatorRegistry::buildin(RT_TYPE);
    if (!buildinResource.initModule(nullptr)) {
        NAVI_KERNEL_LOG(ERROR, "init buildin resource registry module failed");
        return false;
    }
    if (!buildinKernel.initModule(nullptr)) {
        NAVI_KERNEL_LOG(ERROR, "init buildin kernel registry module failed");
        return false;
    }
    if (!buildinType.initModule(nullptr)) {
        NAVI_KERNEL_LOG(ERROR, "init buildin type registry module failed");
        return false;
    }
    buildinResource.setInited();
    buildinKernel.setInited();
    buildinType.setInited();
    return true;
}

bool CreatorManager::initRegistrys() {
    for (const auto &pair : _moduleMap) {
        _resourceRegistry.addSubRegistry(pair.second->getResourceRegistry());
        _kernelRegistry.addSubRegistry(pair.second->getKernelRegistry());
        _typeRegistry.addSubRegistry(pair.second->getTypeRegistry());
    }
    auto isBuildin = (!_parentManager);
    if (isBuildin) {
        if (!initBuildinModule()) {
            return false;
        }
        _resourceRegistry.addSubRegistry(
            *CreatorRegistry::buildin(RT_RESOURCE));
        _kernelRegistry.addSubRegistry(*CreatorRegistry::buildin(RT_KERNEL));
        _typeRegistry.addSubRegistry(*CreatorRegistry::buildin(RT_TYPE));
    }
    bool ret = true;
    if (!_resourceRegistry.initCreators(isBuildin)) {
        NAVI_KERNEL_LOG(ERROR, "init resource creators failed");
        ret = false;
    }
    if (!_kernelRegistry.initCreators(isBuildin)) {
        NAVI_KERNEL_LOG(ERROR, "init kernel creators failed");
        ret = false;
    }
    if (!_typeRegistry.initCreators(isBuildin)) {
        NAVI_KERNEL_LOG(ERROR, "init type creators failed");
        ret = false;
    }
    if (!validateRegistry()) {
        return false;
    }
    if (!initDependBy()) {
        return false;
    }
    if (!initProduce()) {
        return false;
    }
    if (!initReplace()) {
        return false;
    }
    if (!initMergedMaps()) {
        return false;
    }
    if (!initEnableBuilds()) {
        return false;
    }
    if (!initResourceDependant() || !initKernelDependant()) {
        return false;
    }
    initJsonizeInfo();
    return ret;
}

bool CreatorManager::validateRegistry() const {
    auto kernel = validateKernelRegistry();
    auto resource = validateResourceRegistry();
    auto type = validateTypeRegistry();
    return kernel && resource && type;
}

bool CreatorManager::validateKernelRegistry() const {
    const auto &creatorMap = _kernelRegistry.getCreatorMap();
    for (const auto &pair : creatorMap) {
        const auto &kernel = pair.first;
        auto creator = dynamic_cast<KernelCreator *>(pair.second.get());
        if (!creator) {
            NAVI_KERNEL_LOG(ERROR, "invalid kernel registry [%s]",
                            kernel.c_str());
            return false;
        }
        if (_parentManager) {
            auto parentCreator =
                _parentManager->_kernelRegistry.getCreator(kernel);
            if (parentCreator) {
                NAVI_KERNEL_LOG(
                    ERROR,
                    "kernel [%s] double register, module 1 [%s] source 1 "
                    "[%s], module 2 [%s] source 2 [%s]",
                    kernel.c_str(), parentCreator->getModulePath().c_str(),
                    parentCreator->getSourceFile(),
                    creator->getModulePath().c_str(), creator->getSourceFile());
                return false;
            }
        }
    }
    return true;
}

bool CreatorManager::validateResourceRegistry() const {
    bool isBuildin = (!_parentManager);
    const auto &creatorMap = _resourceRegistry.getCreatorMap();
    for (const auto &pair : creatorMap) {
        const auto &resource = pair.first;
        auto creator = dynamic_cast<ResourceCreator *>(pair.second.get());
        if (!creator) {
            NAVI_KERNEL_LOG(ERROR, "invalid resource registry [%s]",
                            resource.c_str());
            return false;
        }
        auto stage = creator->getStage();
        if (stage < RS_EXTERNAL || stage >= RS_UNKNOWN) {
            NAVI_KERNEL_LOG(
                ERROR, "invalid stage [%s], resource [%s]", ResourceStage_Name(stage).c_str(), resource.c_str());
            return false;
        }
        if (!isBuildin && stage < RS_BIZ) {
            NAVI_KERNEL_LOG(
                ERROR,
                "register resource [%s] failed, can't register "
                "stage [%s] resource in biz module [%s], defined in [%s]",
                resource.c_str(), ResourceStage_Name(stage).c_str(),
                creator->getModulePath().c_str(), creator->getSourceFile());
            return false;
        }
        if (stage < RS_GRAPH) {
            const auto &bindInfos = creator->getBindInfos();
            if (!bindInfos.namedDataBindVec.empty()) {
                NAVI_KERNEL_LOG(
                    ERROR,
                    "register resource [%s] stage [%s] failed, biz module "
                    "[%s], defined in [%s], only stage >= RS_GRAPH resource can "
                    "depend on named data",
                    resource.c_str(), ResourceStage_Name(stage).c_str(),
                    creator->getModulePath().c_str(), creator->getSourceFile());
                return false;
            }
        }
        if (_parentManager) {
            auto parentCreator =
                _parentManager->_resourceRegistry.getCreator(resource);
            if (parentCreator) {
                NAVI_KERNEL_LOG(
                    ERROR,
                    "resource [%s] double register, module 1 [%s] source 1 "
                    "[%s], module 2 [%s] source 2 [%s]",
                    resource.c_str(), parentCreator->getModulePath().c_str(),
                    parentCreator->getSourceFile(),
                    creator->getModulePath().c_str(), creator->getSourceFile());
                return false;
            }
        }
    }
    return true;
}

bool CreatorManager::validateTypeRegistry() const {
    const auto &creatorMap = _typeRegistry.getCreatorMap();
    if (_parentManager) {
        if (!creatorMap.empty()) {
            NAVI_KERNEL_LOG(ERROR,
                            "type registry in biz module is not supported, "
                            "move to buildin module list, biz [%s]",
                            _bizName.c_str());
            for (const auto &pair : creatorMap) {
                const auto &creator = pair.second;
                NAVI_KERNEL_LOG(ERROR,
                                "error type [%s] source [%s] module [%s]",
                                pair.first.c_str(), creator->getSourceFile(),
                                creator->getModulePath().c_str());
            }
            return false;
        }
    }
    for (const auto &pair : creatorMap) {
        const auto &type = pair.first;
        auto creator = dynamic_cast<Type *>(pair.second.get());
        if (!creator) {
            NAVI_KERNEL_LOG(ERROR, "invalid type registry [%s]", type.c_str());
            return false;
        }
        if (creator->getName().empty()) {
            NAVI_KERNEL_LOG(ERROR,
                            "type name can't be empty, typeid [%s], source file [%s]",
                            typeid(*creator).name(),
                            creator->getSourceFile());
            return false;
        }
    }
    return true;
}

bool CreatorManager::initMergedMaps() {
    if (!initKernelMergedMap()) {
        return false;
    }
    if (!initResourceMergedMap()) {
        return false;
    }
    if (!initTypeMergedMap()) {
        return false;
    }
    return true;
}

bool CreatorManager::initKernelMergedMap() {
    addToKernelMergedMap(_kernelRegistry.getCreatorMap());
    addToKernelMergedMap(_cloneKernelCreatorMap);
    if (_parentManager) {
        addToKernelMergedMap(_parentManager->_kernelRegistry.getCreatorMap());
    }
    return true;
}

void CreatorManager::addToKernelMergedMap(const CreatorMap &creatorMap) {
    for (const auto &pair : creatorMap) {
        const auto &kernel = pair.first;
        auto creator = static_cast<KernelCreator *>(pair.second.get());
        _mergedKernelCreatorMap.emplace(kernel, creator);
    }
}

bool CreatorManager::initResourceMergedMap() {
    addToResourceMergedMap(_resourceRegistry.getCreatorMap());
    addToResourceMergedMap(_cloneResourceCreatorMap);
    if (_parentManager) {
        addToResourceMergedMap(
            _parentManager->_resourceRegistry.getCreatorMap());
    }
    return true;
}

void CreatorManager::addToResourceMergedMap(const CreatorMap &creatorMap) {
    for (const auto &pair : creatorMap) {
        const auto &resource = pair.first;
        auto creator = static_cast<ResourceCreator *>(pair.second.get());
        _mergedResourceCreatorMap.emplace(resource, creator);
    }
}

bool CreatorManager::initTypeMergedMap() {
    // type can't register in biz level
    const auto &creatorMap = _typeRegistry.getCreatorMap();
    for (const auto &pair : creatorMap) {
        const auto &type = pair.first;
        auto creator = static_cast<Type *>(pair.second.get());
        _mergedTypeMap.emplace(type, creator);
    }
    return true;
}

bool CreatorManager::initDependBy() {
    const auto &resourceCreatorMap = _resourceRegistry.getCreatorMap();
    for (const auto &pair : resourceCreatorMap) {
        const auto &name = pair.first;
        auto resourceCreator = static_cast<ResourceCreator *>(pair.second.get());
        assert(resourceCreator);
        if (!addResourceDependBy(name, resourceCreator)) {
            return false;
        }
        if (!addKernelDependBy(name, resourceCreator)) {
            return false;
        }
    }
    return true;
}

bool CreatorManager::addResourceDependBy(const std::string &name,
                                         ResourceCreator *resourceCreator)
{
    const auto &dependByResourceMap = resourceCreator->getDependByResourceMap();
    for (const auto &pair : dependByResourceMap) {
        const auto &dependBy = pair.first;
        auto dependByDef = pair.second;
        bool isClone = false;
        auto destCreator = getDependByResource(dependBy, isClone);
        if (!destCreator) {
            NAVI_KERNEL_LOG(ERROR,
                            "add depend by failed, dependBy resource [%s] not "
                            "registered, resource [%s]",
                            dependBy.c_str(), name.c_str());
            return false;
        }
        auto destResourceCreator = static_cast<ResourceCreator *>(destCreator);
        if (isClone) {
            auto destStage = destResourceCreator->getStage();
            if (destStage < RS_BIZ) {
                NAVI_KERNEL_LOG(ERROR,
                                "add depend by failed, stage [%s] resource "
                                "[%s] can't depend on biz level resource [%s]",
                                ResourceStage_Name(destStage).c_str(),
                                dependBy.c_str(), name.c_str());
                return false;
            }
        }
        if (!destResourceCreator->addDependBy(name, dependByDef)) {
            return false;
        }
    }
    return true;
}

bool CreatorManager::addKernelDependBy(const std::string &name,
                                       ResourceCreator *resourceCreator)
{
    const auto &dependByKernelMap = resourceCreator->getDependByKernelMap();
    for (const auto &pair : dependByKernelMap) {
        const auto &dependBy = pair.first;
        auto dependByDef = pair.second;
        auto destCreator = getDependByKernel(dependBy);
        if (!destCreator) {
            NAVI_KERNEL_LOG(ERROR,
                            "add depend by failed, dependBy kernel [%s] not "
                            "registered, resource [%s]",
                            dependBy.c_str(), name.c_str());
            return false;
        }
        auto destKernelCreator = static_cast<KernelCreator *>(destCreator);
        if (!destKernelCreator->addDependBy(name, dependByDef)) {
            return false;
        }
    }
    return true;
}

Creator *CreatorManager::getDependByResource(const std::string &name, bool &isClone) {
    isClone = false;
    auto destCreator = _resourceRegistry.getCreator(name);
    if (destCreator) {
        return destCreator;
    }
    auto it = _cloneResourceCreatorMap.find(name);
    if (_cloneResourceCreatorMap.end() != it) {
        return it->second.get();
    }
    if (!_parentManager) {
        return nullptr;
    }
    auto parentCreator = _parentManager->_resourceRegistry.getCreator(name);
    if (!parentCreator) {
        return nullptr;
    }
    auto parentResourceCreator = static_cast<ResourceCreator *>(parentCreator);
    auto clone = parentResourceCreator->clone();
    _cloneResourceCreatorMap.emplace(name, clone);
    isClone = true;
    return clone.get();
}

Creator *CreatorManager::getDependByKernel(const std::string &name) {
    auto destCreator = _kernelRegistry.getCreator(name);
    if (destCreator) {
        return destCreator;
    }
    auto it = _cloneKernelCreatorMap.find(name);
    if (_cloneKernelCreatorMap.end() != it) {
        return it->second.get();
    }
    if (!_parentManager) {
        return nullptr;
    }
    auto parentCreator = _parentManager->_kernelRegistry.getCreator(name);
    if (!parentCreator) {
        return nullptr;
    }
    auto parentKernelCreator = static_cast<KernelCreator *>(parentCreator);
    auto clone = parentKernelCreator->clone();
    _cloneKernelCreatorMap.emplace(name, clone);
    return clone.get();
}

bool CreatorManager::initProduce() {
    const auto &resourceCreatorMap = _resourceRegistry.getCreatorMap();
    for (const auto &pair : resourceCreatorMap) {
        const auto &resourceName = pair.first;
        auto resourceCreator = static_cast<ResourceCreator *>(pair.second.get());
        assert(resourceCreator);
        auto producInfo = resourceCreator->getProduceInfo();
        if (!producInfo) {
            continue;
        }
        const auto &produceName = producInfo->name();
        if (resourceName == produceName) {
            NAVI_KERNEL_LOG(ERROR, "resource [%s] produce itself", resourceName.c_str());
            return false;
        }
        auto creator = _resourceRegistry.getCreator(produceName);
        if (!creator) {
            NAVI_KERNEL_LOG(ERROR,
                            "produced resource [%s] not registered, produce by [%s]",
                            produceName.c_str(),
                            resourceName.c_str());
            return false;
        }
        auto produceCreator = dynamic_cast<ResourceCreator *>(creator);
        if (!produceCreator) {
            NAVI_KERNEL_LOG(ERROR, "invalid resource registry [%s]", produceName.c_str());
            return false;
        }
        auto stage1 = resourceCreator->getStage();
        auto stage2 = produceCreator->getStage();
        if (stage1 != stage2) {
            NAVI_KERNEL_LOG(
                ERROR,
                "stage not match, produce resource [%s] stage [%s] not equal produced resource [%s] stage [%s]",
                resourceName.c_str(),
                ResourceStage_Name(stage1).c_str(),
                produceName.c_str(),
                ResourceStage_Name(stage2).c_str());
            return false;
        }
        if (stage1 >= RS_RUN_GRAPH_EXTERNAL) {
            NAVI_KERNEL_LOG(ERROR,
                            "invalid produce stage, resource [%s] stage [%s] can't produce other resource [%s]",
                            resourceName.c_str(),
                            ResourceStage_Name(stage1).c_str(),
                            produceName.c_str());
            return false;
        }
        if (!produceCreator->setProducer(resourceName)) {
            NAVI_KERNEL_LOG(ERROR,
                            "resource [%s] has multiple producers: [%s] and [%s]",
                            produceName.c_str(),
                            resourceName.c_str(),
                            produceCreator->getProducer().c_str());
            return false;
        }
    }
    return true;
}

bool CreatorManager::initReplace() {
    bool ret = true;
    const auto &resourceCreatorMap = _resourceRegistry.getCreatorMap();
    for (const auto &pair : resourceCreatorMap) {
        const auto &name = pair.first;
        auto resourceCreator = static_cast<ResourceCreator *>(pair.second.get());
        assert(resourceCreator);
        const auto &replaceSet = resourceCreator->getEnableReplaceSet();
        for (const auto &replace : replaceSet) {
            bool isClone = false;
            auto destCreator = getDependByResource(replace, isClone);
            if (!destCreator) {
                NAVI_KERNEL_LOG(ERROR,
                                "init replace failed, dest resource [%s] not "
                                "registered, resource [%s]",
                                replace.c_str(),
                                name.c_str());
                ret = false;
                continue;
            }
            auto destResourceCreator = static_cast<ResourceCreator *>(destCreator);
            if (isClone) {
                auto destStage = destResourceCreator->getStage();
                if (destStage < RS_BIZ) {
                    NAVI_KERNEL_LOG(
                        ERROR,
                        "add replacer failed, can't replace stage [%s] resource [%s] in biz registry, replacer [%s]",
                        ResourceStage_Name(destStage).c_str(),
                        replace.c_str(),
                        name.c_str());
                    return false;
                }
            }
            if (!destResourceCreator->addReplacer(resourceCreator)) {
                ret = false;
            }
        }
    }
    return ret;
}

bool CreatorManager::initResourceDependant() {
    bool ret = true;
    const auto &creatorMap = _resourceRegistry.getCreatorMap();
    for (const auto &pair : creatorMap) {
        const auto &resource = pair.first;
        auto creator = dynamic_cast<ResourceCreator *>(pair.second.get());
        assert(creator);
        auto stage = creator->getStage();
        const auto &dependMap = creator->getDependResources();
        if (RS_RUN_GRAPH_EXTERNAL == stage || RS_EXTERNAL == stage) {
            if (!dependMap.empty()) {
                NAVI_KERNEL_LOG(ERROR,
                                "resource [%s] stage [%s] can't have depends, dependMap size: %lu",
                                resource.c_str(),
                                ResourceStage_Name(stage).c_str(),
                                dependMap.size());
                ret = false;
            }
        }
        for (const auto &depend : dependMap) {
            const auto &dependResource = depend.first;
            auto require = depend.second;
            auto resourceCreator = const_cast<ResourceCreator *>(
                getResourceCreator(dependResource));
            if (!resourceCreator) {
                NAVI_KERNEL_LOG(ERROR,
                                "resource [%s] depend on not registered "
                                "resource [%s] require [%d]",
                                resource.c_str(),
                                dependResource.c_str(),
                                require);
                ret = false;
                continue;
            }
            auto dependOnStage = resourceCreator->getStage();
            if (stage == RS_GRAPH) {
                if (RS_GRAPH != dependOnStage && RS_RUN_GRAPH_EXTERNAL != dependOnStage &&
                    RS_SNAPSHOT < dependOnStage)
                {
                    ret = false;
                    NAVI_KERNEL_LOG(ERROR,
                                    "resource [%s] stage [%s] can't depend on resource [%s] stage [%s]",
                                    resource.c_str(),
                                    ResourceStage_Name(stage).c_str(),
                                    dependResource.c_str(),
                                    ResourceStage_Name(dependOnStage).c_str());
                }
            } else if (stage < dependOnStage) {
                bool valid = true;
                if (stage < RS_RUN_GRAPH_EXTERNAL) {
                    valid = false;
                } else if (RS_RUN_GRAPH_EXTERNAL != dependOnStage) {
                    valid = false;
                }
                if (!valid) {
                    ret = false;
                    NAVI_KERNEL_LOG(ERROR,
                                    "resource [%s] stage [%s] can't depend on resource [%s] stage [%s]",
                                    resource.c_str(),
                                    ResourceStage_Name(stage).c_str(),
                                    dependResource.c_str(),
                                    ResourceStage_Name(dependOnStage).c_str());
                }
            }
            resourceCreator->addResourceDependant(resource);
        }
    }
    return ret;
}

bool CreatorManager::initJsonizeInfo() {
    const auto &jsonizeInfoMap = _kernelRegistry.getJsonizeInfoMap();
    _kernelRegistry.fillJsonizeInfo(jsonizeInfoMap);
    _resourceRegistry.fillJsonizeInfo(jsonizeInfoMap);
    return true;
}

bool CreatorManager::initEnableBuilds() {
    {
        const auto &creatorMap = _resourceRegistry.getCreatorMap();
        for (const auto &pair : creatorMap) {
            const auto &resource = pair.first;
            auto creator = dynamic_cast<ResourceCreator *>(pair.second.get());
            assert(creator);
            if (!creator->postInit(_mergedResourceCreatorMap)) {
                NAVI_KERNEL_LOG(ERROR, "init resource [%s] enable build failed", resource.c_str());
                return false;
            }
        }
    }
    {
        const auto &creatorMap = _kernelRegistry.getCreatorMap();
        for (const auto &pair : creatorMap) {
            const auto &kernel = pair.first;
            auto creator = dynamic_cast<KernelCreator *>(pair.second.get());
            assert(creator);
            if (!creator->postInit(_mergedResourceCreatorMap)) {
                NAVI_KERNEL_LOG(ERROR, "init kernel [%s] enable build failed", kernel.c_str());
                return false;
            }
        }
    }
    return true;
}

bool CreatorManager::initKernelDependant() {
    const auto &creatorMap = _kernelRegistry.getCreatorMap();
    for (const auto &pair : creatorMap) {
        const auto &kernel = pair.first;
        auto creator = dynamic_cast<KernelCreator *>(pair.second.get());
        assert(creator);
        const auto &dependMap = creator->getDependResources();
        for (const auto &depend : dependMap) {
            const auto &dependResource = depend.first;
            bool require = depend.second;
            auto resourceCreator = const_cast<ResourceCreator *>(
                getResourceCreator(dependResource));
            if (!resourceCreator) {
                NAVI_KERNEL_LOG(ERROR,
                                    "kernel [%s] depend on not registered "
                                    "resource [%s], require = %d",
                                    kernel.c_str(), dependResource.c_str(),
                                    require);
                return false;
            }
            resourceCreator->addKernelDependant(kernel);
        }
    }
    return true;
}

bool CreatorManager::collectMatchedKernels(
    const std::string &kernelRegex,
    bool &emptyMatch,
    std::set<std::string> &kernelSet) const
{
    try {
        bool ret = true;
        std::regex regex(kernelRegex, std::regex_constants::ECMAScript);
        const auto &kernelMap = _mergedKernelCreatorMap;
        for (const auto &pair : kernelMap) {
            const auto &name = pair.first;
            if (std::regex_search(name, regex)) {
                NAVI_KERNEL_LOG(DEBUG, "biz: %s, regex: %s, match kernel [%s]",
                                _bizName.c_str(), kernelRegex.c_str(),
                                name.c_str());
                kernelSet.insert(name);
                ret = false;
            }
        }
        emptyMatch = ret;
        return true;
    } catch (std::regex_error &e) {
        NAVI_KERNEL_LOG(
            ERROR,
            "invalid kernel regex [%s], msg [%s %s], check ECMAScript "
            "syntax[https://en.cppreference.com/w/cpp/regex/ecmascript]",
            kernelRegex.c_str(), e.what(), getRegexErrorStr(e.code()));
        return false;
    }
}

const char *CreatorManager::getRegexErrorStr(std::regex_constants::error_type code) {
    switch (code) {
    case std::regex_constants::error_collate:
        return "error_collate";
    case std::regex_constants::error_ctype:
        return "error_ctype";
    case std::regex_constants::error_escape:
        return "error_escape";
    case std::regex_constants::error_backref:
        return "error_backref";
    case std::regex_constants::error_brack:
        return "error_brack";
    case std::regex_constants::error_paren:
        return "error_paren";
    case std::regex_constants::error_brace:
        return "error_brace";
    case std::regex_constants::error_badbrace:
        return "error_badbrace";
    case std::regex_constants::error_range:
        return "error_range";
    case std::regex_constants::error_space:
        return "error_space";
    case std::regex_constants::error_badrepeat:
        return "error_badrepeat";
    case std::regex_constants::error_complexity:
        return "error_complexity";
    case std::regex_constants::error_stack:
        return "error_stack";
    default:
        return "error_unknown";
    }
}

void CreatorManager::fillModuleSummary(BizSummary &bizSummary) const {
    {
        const auto &resourceCreatorMap = _resourceRegistry.getCreatorMap();
        for (const auto &pair : resourceCreatorMap) {
            const auto &creator = pair.second;
            const auto &modulePath = creator->getModulePath();
            RegistryInfo info;
            info.creator = creator.get();
            bizSummary.modules[modulePath].resource.infos.emplace_back(
                std::move(info));
        }
    }
    {
        const auto &kernelCreatorMap = _kernelRegistry.getCreatorMap();
        for (const auto &pair : kernelCreatorMap) {
            const auto &creator = pair.second;
            const auto &modulePath = creator->getModulePath();
            RegistryInfo info;
            info.creator = creator.get();
            bizSummary.modules[modulePath].kernel.infos.emplace_back(
                std::move(info));
        }
    }
    {
        const auto &typeCreatorMap = _typeRegistry.getCreatorMap();
        for (const auto &pair : typeCreatorMap) {
            const auto &creator = pair.second;
            const auto &modulePath = creator->getModulePath();
            RegistryInfo info;
            info.creator = creator.get();
            bizSummary.modules[modulePath].type.infos.emplace_back(
                std::move(info));
        }
    }
}

std::set<std::string> CreatorManager::getKernelSet() const {
    std::set<std::string> kernelSet;
    const auto &creatorMap = _kernelRegistry.getCreatorMap();
    for (const auto &pair : creatorMap) {
        kernelSet.insert(pair.first);
    }
    return kernelSet;
}

void CreatorManager::setTypeMemoryPoolR(const MemoryPoolRPtr &memoryPoolR) {
    for (const auto &pair : _mergedTypeMap) {
        pair.second->setMemoryPoolR(memoryPoolR);
    }
}
}
