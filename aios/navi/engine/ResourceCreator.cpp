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
#include "navi/engine/ResourceCreator.h"
#include "navi/engine/Resource.h"
#include "navi/perf/NaviSymbolTable.h"

namespace navi {

ResourceCreator::ResourceCreator(const char *sourceFile,
                                 const ResourceDefBuilder &builder,
                                 const ResourceCreateFunc &func,
                                 const ResourceCastFunc &castFunc)
    : Creator(sourceFile)
    , _def(builder.def())
    , _func(func)
    , _castFunc(castFunc)
    , _bindInfos(builder.getBindInfos())
{
}

ResourceCreator::ResourceCreator(const char *sourceFile,
                                 ResourceDef *def,
                                 const ResourceCreateFunc &func,
                                 const BindInfos &bindInfos)
    : Creator(sourceFile)
    , _def(def)
    , _func(func)
    , _bindInfos(bindInfos)
{
}

ResourceCreator::~ResourceCreator() {
}

CreatorPtr ResourceCreator::clone() const {
    auto def = new ResourceDef();
    def->CopyFrom(*_def);
    CreatorPtr creator(
        new ResourceCreator(getSourceFile(), def, _func, _bindInfos));
    if (creator->init(isBuildin(), getModule())) {
        return creator;
    } else {
        NAVI_KERNEL_LOG(ERROR, "clone resource creator [%s] failed",
                        getName().c_str());
        return nullptr;
    }
}

bool ResourceCreator::init(bool isBuildin, Module *module) {
    setBuildin(isBuildin);
    setModule(module);
    if (!validateDef()) {
        return false;
    }
    if (!initDependResources()) {
        return false;
    }
    if (!initDynamicGroup()) {
        return false;
    }
    if (!initDependBy()) {
        return false;
    }
    if (!initReplace()) {
        return false;
    }
    return true;
}

bool ResourceCreator::validateDef() const {
    if (getName().empty()) {
        NAVI_KERNEL_LOG(ERROR, "resource name is empty, source file: %s", _sourceFile ? _sourceFile : "");
        return false;
    }
    return true;
}

bool ResourceCreator::initDependResources() {
    const auto &dependInfo = _def->resource_depend_info();
    auto resourceCount = dependInfo.depend_resources_size();
    NAVI_KERNEL_LOG(TRACE3, "resource def %s", _def->ShortDebugString().c_str());
    for (int i = 0; i < resourceCount; i++) {
        if (!addToDependResourceMap(dependInfo.depend_resources(i))) {
            return false;
        }
    }
    return true;
}

bool ResourceCreator::postInit(const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap) {
    if (!initEnableBuilds(resourceCreatorMap)) {
        return false;
    }
    return true;
}

bool ResourceCreator::initEnableBuilds(const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap) {
    if (_enableBuildInited) {
        return true;
    }
    _enableBuildInited = true;
    const auto &dependInfo = _def->resource_depend_info();
    const auto &enableDef = dependInfo.enable_builds();
    auto enableCount = enableDef.candidates_size();
    if (enableCount > 0) {
        auto stage = getStage();
        if (RS_KERNEL != stage) {
            NAVI_KERNEL_LOG(ERROR,
                            "can't enable buildR, stage [%s] is not RS_KERNEL, resource [%s]",
                            ResourceStage_Name(stage).c_str(),
                            getName().c_str());
            return false;
        }
    }
    for (int32_t i = 0; i < enableCount; i++) {
        const auto &candidate = enableDef.candidates(i);
        _enableBuildResourceSet.insert(candidate);
        if (!addResourceDependRecur(candidate, false, resourceCreatorMap)) {
            NAVI_KERNEL_LOG(
                ERROR, "add enable build candidate [%s] failed, kernel [%s]", candidate.c_str(), getName().c_str());
            return false;
        }
    }
    return true;
}

bool ResourceCreator::addResourceDependRecur(
    const std::string &name,
    bool require,
    const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap)
{
    auto creatorIt = resourceCreatorMap.find(name);
    if (resourceCreatorMap.end() == creatorIt) {
        NAVI_KERNEL_LOG(ERROR, "depend on not registered resource [%s], resource [%s]", name.c_str(), getName().c_str());
        return false;
    }
    auto resourceCreator = creatorIt->second;
    if (!resourceCreator->initEnableBuilds(resourceCreatorMap)) {
        return false;
    }
    if (RS_KERNEL <= resourceCreator->getStage()) {
        const auto &dependMap = resourceCreator->getDependResources();
        for (const auto &pair : dependMap) {
            if (!addResourceDependRecur(pair.first, pair.second && require, resourceCreatorMap)) {
                NAVI_KERNEL_LOG(
                    ERROR, "add depend resource [%s] failed, resource [%s]", pair.first.c_str(), getName().c_str());
                return false;
            }
        }
    } else {
        auto dependIt = _dependResources.find(name);
        if (_dependResources.end() == dependIt) {
            _dependResources[name] = require;
        } else if (require) {
            _dependResources[name] = require;
        }
        NAVI_KERNEL_LOG(SCHEDULE3, "add depend resource success [%s], resource [%s]", name.c_str(), getName().c_str());
    }
    return true;
}

bool ResourceCreator::initDependBy() {
    auto dependByCount = _def->depend_bys_size();
    for (int i = 0; i < dependByCount; i++) {
        const auto &dependBy = _def->depend_bys(i);
        auto type = dependBy.type();
        const auto &name = dependBy.name();
        if (DBT_RESOURCE == type) {
            if (_dependByResource.end() != _dependByResource.find(name)) {
                NAVI_KERNEL_LOG(
                    ERROR,
                    "invalid resource def [%s], duplicate dependBy "
                    "resource [%s]",
                    getName().c_str(), name.c_str());
                return false;
            }
            _dependByResource.emplace(name, &dependBy);
        } else {
            if (_dependByKernel.end() != _dependByKernel.find(name)) {
                NAVI_KERNEL_LOG(
                    ERROR,
                    "invalid resource def [%s], duplicate dependBy "
                    "kernel [%s]",
                    getName().c_str(), name.c_str());
                return false;
            }
            _dependByKernel.emplace(name, &dependBy);
        }
    }
    return true;
}

bool ResourceCreator::initReplace() {
    auto replaceCount = _def->replace_enables_size();
    for (int i = 0; i < replaceCount; i++) {
        const auto &replaceInfo = _def->replace_enables(i);
        _enableReplaceSet.insert(replaceInfo.name());
    }
    return true;
}

bool ResourceCreator::addReplacer(ResourceCreator *replacerCreator) {
    const auto &name = getName();
    const auto &replacerName = replacerCreator->getName();
    if (_replaceBySet.end() != _replaceBySet.find(replacerName)) {
        return true;
    }
    auto thisStage = getStage();
    auto replacerStage = replacerCreator->getStage();
    if (thisStage != replacerStage) {
        NAVI_KERNEL_LOG(ERROR,
                        "can't replace [%s] [%s] with [%s] [%s], stage not equal",
                        name.c_str(),
                        ResourceStage_Name(thisStage).c_str(),
                        replacerName.c_str(),
                        ResourceStage_Name(replacerStage).c_str());
        return false;
    }
    void *baseAddr= nullptr;
    auto replacer = replacerCreator->create(baseAddr);
    if (!_castFunc((navi::Resource *)baseAddr)) {
        NAVI_KERNEL_LOG(ERROR, "add replacer failed, [%s] is not subclass of [%s]",
                        replacerName.c_str(), name.c_str());
        return false;
    }
    _replaceBySet.insert(replacerName);
    return true;
}

bool ResourceCreator::initDynamicGroup() {
    auto dependInfo = _def->mutable_resource_depend_info();
    auto groupCount = dependInfo->dynamic_resource_groups_size();
    for (int i = 0; i < groupCount; i++) {
        auto group = dependInfo->mutable_dynamic_resource_groups(i);
        const auto &name = group->group_name();
        if (_dynamicGroupMap.end() != _dynamicGroupMap.find(name)) {
            NAVI_KERNEL_LOG(
                ERROR,
                "invalid resource def [%s], duplicate dynamic group [%s]",
                getName().c_str(), name.c_str());
            return false;
        }
        auto dependCount = group->depend_resources_size();
        for (int j = 0; j < dependCount; j++) {
            const auto &dependDef = group->depend_resources(j);
            if (!addToDependResourceMap(dependDef)) {
                NAVI_KERNEL_LOG(ERROR,
                                "add dependBy resource [%s] to resource [%s] "
                                "group [%s] failed",
                                dependDef.name().c_str(), getName().c_str(),
                                name.c_str());
                return false;
            }
        }
        _dynamicGroupMap.emplace(name, group);
    }
    return true;
}

bool ResourceCreator::addDependBy(const std::string &resource,
                                  const DependByDef *def)
{
    const auto &groupName = def->dynamic_group();
    auto groupIt = _dynamicGroupMap.find(groupName);
    if (_dynamicGroupMap.end() == groupIt) {
        NAVI_KERNEL_LOG(ERROR,
                        "add resource [%s] to dynamic depend of resource [%s] "
                        "failed, group [%s] not exist",
                        resource.c_str(), getName().c_str(), groupName.c_str());
        return false;
    }
    auto groupDef = groupIt->second;
    auto dependDef = groupDef->add_depend_resources();
    dependDef->set_name(resource);
    dependDef->set_require(true);
    if (!addToDependResourceMap(*dependDef)) {
        NAVI_KERNEL_LOG(
            ERROR,
            "add dependBy resource [%s] to resource [%s] group [%s] failed",
            resource.c_str(), getName().c_str(), groupName.c_str());
        return false;
    }
    return true;
}

const std::map<std::string, bool> &ResourceCreator::getDependResources() const {
    return _dependResources;
}

bool ResourceCreator::addToDependResourceMap(
    const DependResourceDef &resourceDef)
{
    const auto &name = resourceDef.name();
    auto require = resourceDef.require();
    if (_dependResources.end() != _dependResources.find(name)) {
        NAVI_KERNEL_LOG(
            ERROR, "invalid resource def [%s], duplicate depend resource [%s]",
            getName().c_str(), name.c_str());
        return false;
    }
    _dependResources.emplace(name, require);
    return true;
}

const DependByMap &ResourceCreator::getDependByResourceMap() const {
    return _dependByResource;
}

const DependByMap &ResourceCreator::getDependByKernelMap() const {
    return _dependByKernel;
}

const ProduceInfoDef *ResourceCreator::getProduceInfo() const {
    if (_def->has_produce_info()) {
        return &_def->produce_info();
    } else {
        return nullptr;
    }
}

const std::string &ResourceCreator::getName() const {
    return _def->name();
}

ResourceStage ResourceCreator::getStage() const {
    return _def->stage();
}

const std::shared_ptr<ResourceDef> &ResourceCreator::def() const {
    return _def;
}

std::string ResourceCreator::getClassName() const {
    void *baseAddr;
    auto resource = create(baseAddr);
    auto ptr = resource.get();
    auto className = NaviSymbolTable::parseSymbol(typeid(*ptr).name());
    return className;
}

ResourcePtr ResourceCreator::create(void *&baseAddr) const {
    auto resource = _func(baseAddr);
    assert(resource);
    resource->setResourceDef(_def);
    return resource;
}

const BindInfos &ResourceCreator::getBindInfos() const {
    return _bindInfos;
}

const DynamicResourceInfoMap &ResourceCreator::getDynamicGroupInfoMap() const {
    return _dynamicGroupMap;
}

void ResourceCreator::addResourceDependant(const std::string &resource) {
    _resourceDependant.emplace(resource);
}

void ResourceCreator::addKernelDependant(const std::string &kernel) {
    _kernelDependant.emplace(kernel);
}

void ResourceCreator::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    Creator::Jsonize(json);
    json.Jsonize("stage", ResourceStage_Name(getStage()));
    json.Jsonize("depend_on", _dependResources);
    json.Jsonize("enable_build", _enableBuildResourceSet);
    if (_def->has_produce_info()) {
        json.Jsonize("produce", _def->produce_info().name());
    }
    if (!_producer.empty()) {
        json.Jsonize("produced_by", _producer);
    }
    json.Jsonize("resource_dependant", _resourceDependant);
    json.Jsonize("kernel_dependant", _kernelDependant);

    std::map<std::string, bool> namedDataMap;
    for (const auto &namedData : _bindInfos.namedDataBindVec) {
        namedDataMap.emplace(namedData.name, namedData.require);
    }
    json.Jsonize("named_data", namedDataMap);
    std::set<std::string> replaceBySet;
    replaceBySet.insert(_replaceBySet.begin(), _replaceBySet.end());
    json.Jsonize("replace_by", replaceBySet);
}

}
