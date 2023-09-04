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
#include "navi/engine/KernelCreator.h"
#include "navi/engine/ResourceCreator.h"
#include "navi/engine/Kernel.h"
#include "navi/perf/NaviSymbolTable.h"
#include "navi/config/NaviConfig.h"
#include "navi/log/NaviLogger.h"
#include "navi/util/CommonUtil.h"

namespace navi {

KernelCreator::KernelCreator(const char *sourceFile,
                             const KernelDefBuilder &builder,
                             const KernelCreateFunc &func)

    : Creator(sourceFile)
    , _def(builder.def())
    , _func(func)
    , _bindInfos(builder.getBindInfos())
{}

KernelCreator::KernelCreator(const char *sourceFile,
                             KernelDef *def,
                             const KernelCreateFunc &func,
                             const BindInfos &bindInfos)
    : Creator(sourceFile)
    , _def(def)
    , _func(func)
    , _bindInfos(bindInfos)
{
}

KernelCreator::~KernelCreator() {
    DELETE_AND_SET_NULL(_def);
    DELETE_AND_SET_NULL(_jsonAttrsDocument);
}

CreatorPtr KernelCreator::clone() const {
    auto def = new KernelDef();
    def->CopyFrom(*_def);
    CreatorPtr creator(
        new KernelCreator(getSourceFile(), def, _func, _bindInfos));
    if (creator->init(isBuildin(), getModule())) {
        return creator;
    } else {
        NAVI_KERNEL_LOG(ERROR, "clone kernel creator [%s] failed",
                        getName().c_str());
        return nullptr;
    }
}

bool KernelCreator::init(bool isBuildin, Module *module) {
    setBuildin(isBuildin);
    setModule(module);
    if (!validateDef()) {
        return false;
    }
    return initInputIndex()
        && initOutputIndex()
        && initInputGroupIndex()
        && initOutputGroupIndex()
        && initDependResources()
        && initDynamicGroup()
        && initAttrs();
}

bool KernelCreator::validateDef() const {
    if (getName().empty()) {
        NAVI_KERNEL_LOG(ERROR, "kernel name is empty, source file: %s", _sourceFile ? _sourceFile : "");
        return false;
    }
    return true;
}

bool KernelCreator::initInputIndex() {
    for (int32_t i = 0; i < _def->inputs().size(); i++) {
        const auto &inputDef = _def->inputs(i);
        auto it = _inputMap.find(inputDef.name());
        if (_inputMap.end() != it) {
            NAVI_KERNEL_LOG(ERROR,
                            "invalid kernel def [%s], input port [%s] defined "
                            "more than once",
                            _def->kernel_name().c_str(),
                            inputDef.name().c_str());
            return false;
        }
        _inputMap.emplace(inputDef.name(),
                          PortInfo(PortIndex(i, INVALID_INDEX), &inputDef));
    }
    return true;
}

bool KernelCreator::initOutputIndex() {
    auto outputSize = _def->outputs().size();
    for (int32_t i = 0; i < outputSize; i++) {
        const auto &outputDef = _def->outputs(i);
        auto it = _outputMap.find(outputDef.name());
        if (_outputMap.end() != it) {
            NAVI_KERNEL_LOG(ERROR,
                            "invalid kernel def [%s], output port [%s] defined "
                            "more than once",
                            _def->kernel_name().c_str(),
                            outputDef.name().c_str());
            return false;
        }
        _outputMap.insert(std::make_pair(outputDef.name(),
                        PortInfo(PortIndex(i, INVALID_INDEX), &outputDef)));
    }
    _controlOutputMap.emplace(NODE_FINISH_PORT,
                              PortInfo(PortIndex(CONTROL_PORT_FINISH_INDEX, INVALID_INDEX, true), &_finishPortDef));
    return true;
}

bool KernelCreator::initInputGroupIndex() {
    for (int32_t i = 0; i < _def->input_groups().size(); i++) {
        const auto &inputGroupDef = _def->input_groups(i);
        auto it = _inputGroupMap.find(inputGroupDef.name());
        if (_inputGroupMap.end() != it) {
            NAVI_KERNEL_LOG(ERROR,
                            "invalid kernel def [%s], input group port [%s] "
                            "defined more than once",
                            _def->kernel_name().c_str(),
                            inputGroupDef.name().c_str());
            return false;
        }
        _inputGroupMap.emplace(
            inputGroupDef.name(),
            PortInfo(PortIndex(INVALID_INDEX, i), &inputGroupDef));
    }
    return true;
}

bool KernelCreator::initOutputGroupIndex() {
    for (int32_t i = 0; i < _def->output_groups().size(); i++) {
        const auto &outputGroupDef = _def->output_groups(i);
        auto it = _outputGroupMap.find(outputGroupDef.name());
        if (_outputGroupMap.end() != it) {
            NAVI_KERNEL_LOG(ERROR,
                            "invalid kernel def [%s], output group port [%s] "
                            "defined more than once",
                            _def->kernel_name().c_str(),
                            outputGroupDef.name().c_str());
            return false;
        }
        _outputGroupMap.emplace(
            outputGroupDef.name(),
            PortInfo(PortIndex(INVALID_INDEX, i), &outputGroupDef));
    }
    return true;
}

bool KernelCreator::initDependResources() {
    const auto &dependInfo = _def->resource_depend_info();
    auto resourceCount = dependInfo.depend_resources_size();
    for (int i = 0; i < resourceCount; i++) {
        if (!addToDependResourceMap(dependInfo.depend_resources(i))) {
            return false;
        }
    }
    return true;
}

bool KernelCreator::postInit(const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap) {
    if (!initEnableBuilds(resourceCreatorMap)) {
        return false;
    }
    _dependResourcesWithoutSelect = _dependResources;
    if (!initSelectors()) {
        return false;
    }
    if (!initConfigKey(resourceCreatorMap)) {
        return false;
    }
    return true;
}


bool KernelCreator::initEnableBuilds(const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap) {
    const auto &dependInfo = _def->resource_depend_info();
    const auto &enableDef = dependInfo.enable_builds();
    auto enableCount = enableDef.candidates_size();
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

bool KernelCreator::addResourceDependRecur(
    const std::string &name,
    bool require,
    const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap)
{
    auto creatorIt = resourceCreatorMap.find(name);
    if (resourceCreatorMap.end() == creatorIt) {
        NAVI_KERNEL_LOG(ERROR, "depend on not registered resource [%s], kernel [%s]", name.c_str(), getName().c_str());
        return false;
    }
    auto resourceCreator = creatorIt->second;
    if (RS_KERNEL <= resourceCreator->getStage()) {
        const auto &dependMap = resourceCreator->getDependResources();
        for (const auto &pair : dependMap) {
            if (!addResourceDependRecur(pair.first, pair.second && require, resourceCreatorMap)) {
                NAVI_KERNEL_LOG(
                    ERROR, "add depend resource [%s] failed, kernel [%s]", pair.first.c_str(), getName().c_str());
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
        NAVI_KERNEL_LOG(SCHEDULE3, "add depend resource success [%s], kernel [%s]", name.c_str(), getName().c_str());
    }
    return true;
}

bool KernelCreator::initSelectors() {
    const auto &dependInfo = _def->resource_depend_info();
    const auto &selectorCount = dependInfo.selectors_size();
    for (int i = 0; i < selectorCount; i++) {
        const auto &selectorDef = dependInfo.selectors(i);
        auto condidateCount = selectorDef.candidates_size();
        bool require = selectorDef.require();
        for (int j = 0; j < condidateCount; j++) {
            const auto &candidate = selectorDef.candidates(j);
            DependResourceDef def;
            def.set_name(candidate);
            def.set_require(require);
            if (!addToDependResourceMap(def)) {
                NAVI_KERNEL_LOG(ERROR,
                                "invalid selector [%d], candidate resource [%s] index [%d], kernel [%s]",
                                i,
                                candidate.c_str(),
                                j,
                                getName().c_str());
                return false;
            }
        }
    }
    return true;
}

bool KernelCreator::initConfigKey(const std::unordered_map<std::string, ResourceCreator *> &resourceCreatorMap) {
    auto keyCount = _def->config_keys_size();
    for (int32_t i = 0; i < keyCount; i++) {
        const auto &keyDef = _def->config_keys(i);
        const auto &name = keyDef.name();
        const auto &configKey = keyDef.config_key();
        auto it = resourceCreatorMap.find(name);
        if (resourceCreatorMap.end() == it) {
            NAVI_KERNEL_LOG(ERROR, "can't set config key for not registerd resource [%s]", name.c_str());
            return false;
        }
        auto stage = it->second->getStage();
        if (RS_KERNEL != stage) {
            NAVI_KERNEL_LOG(ERROR,
                            "only RS_KERNEL depend can have config key, kernel [%s], depend [%s] stage [%s]",
                            getName().c_str(),
                            name.c_str(),
                            ResourceStage_Name(stage).c_str());
            return false;
        }
        if (!configKey.empty()) {
            _configKeyMap[name] = configKey;
        }
    }
    return true;
}

bool KernelCreator::initDynamicGroup() {
    auto dependInfo = _def->mutable_resource_depend_info();
    auto groupCount = dependInfo->dynamic_resource_groups_size();
    for (int i = 0; i < groupCount; i++) {
        auto group = dependInfo->mutable_dynamic_resource_groups(i);
        const auto &name = group->group_name();
        if (_dynamicGroupMap.end() != _dynamicGroupMap.find(name)) {
            NAVI_KERNEL_LOG(
                ERROR, "invalid kernel def [%s], duplicate dynamic group [%s]",
                getName().c_str(), name.c_str());
            return false;
        }
        auto dependCount = group->depend_resources_size();
        for (int j = 0; j < dependCount; j++) {
            const auto &dependDef = group->depend_resources(j);
            if (!addToDependResourceMap(dependDef)) {
                NAVI_KERNEL_LOG(ERROR,
                                "add dependBy resource [%s] to kernel [%s] "
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

bool KernelCreator::addDependBy(const std::string &resource,
                                const DependByDef *def)
{
    const auto &groupName = def->dynamic_group();
    auto groupIt = _dynamicGroupMap.find(groupName);
    if (_dynamicGroupMap.end() == groupIt) {
        NAVI_KERNEL_LOG(ERROR,
                        "add resource [%s] to dynamic depend of kernel [%s] "
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
            "add dependBy resource [%s] to kernel [%s] group [%s] failed",
            resource.c_str(), getName().c_str(), groupName.c_str());
        return false;
    }
    return true;
}

bool KernelCreator::initAttrs() {
    const auto &jsonAttrs = _def->json_attrs();
    if (jsonAttrs.empty()) {
        return true;
    }
    _jsonAttrsDocument = new autil::legacy::RapidDocument();
    if (!NaviConfig::parseToDocument(jsonAttrs, *_jsonAttrsDocument)) {
        NAVI_KERNEL_LOG(ERROR, "invalid kernel [%s] attr [%s], invalid json", getName().c_str(), jsonAttrs.c_str());
        return false;
    }
    if (!_jsonAttrsDocument->IsObject()) {
        NAVI_KERNEL_LOG(ERROR, "invalid kernel [%s] attr [%s], not object", getName().c_str(), jsonAttrs.c_str());
        return false;
    }
    return true;
}
const std::map<std::string, bool> &KernelCreator::getDependResources() const {
    return _dependResources;
}

const std::map<std::string, bool> &KernelCreator::getDependResourcesWithoutSelect() const {
    return _dependResourcesWithoutSelect;
}

bool KernelCreator::addToDependResourceMap(
    const DependResourceDef &resourceDef)
{
    const auto &name = resourceDef.name();
    auto require = resourceDef.require();
    if (name.empty()) {
        NAVI_KERNEL_LOG(ERROR, "empty depend resource name, add depend of resource [%s] failed", getName().c_str());
        return false;
    }
    if (_dependResources.end() == _dependResources.find(name)) {
        _dependResources.emplace(name, require);
    } else if (require) {
        _dependResources[name] = require;
    }
    return true;
}

std::string KernelCreator::getClassName() const {
    void *baseAddr;
    auto kernel = create(baseAddr);
    auto className = NaviSymbolTable::parseSymbol(typeid(*kernel).name());
    delete kernel;
    return className;
}

Kernel *KernelCreator::create(void *&baseAddr) const {
    return _func(baseAddr);
}

size_t KernelCreator::inputSize() const {
    return _inputMap.size();
}

size_t KernelCreator::outputSize() const {
    return _outputMap.size();
}

size_t KernelCreator::controlOutputSize() const {
    return _controlOutputMap.size();
}

size_t KernelCreator::inputGroupSize() const {
    return _inputGroupMap.size();
}

size_t KernelCreator::outputGroupSize() const {
    return _outputGroupMap.size();
}

PortInfo KernelCreator::getInputPortInfo(const std::string &name) const {
    std::string port;
    IndexType index = 0;
    if (!CommonUtil::parseGroupPort(name, port, index)) {
        return PortInfo();
    }
    return getInputPortInfo(port, index);
}

PortInfo KernelCreator::getInputPortInfo(const std::string &port, IndexType index) const {
    if (0 == index) {
        auto inputIt = _inputMap.find(port);
        if (_inputMap.end() != inputIt) {
            return inputIt->second;
        }
    }
    auto inputGroupIt = _inputGroupMap.find(port);
    if (_inputGroupMap.end() != inputGroupIt) {
        auto ret = inputGroupIt->second;
        ret.index.index = index;
        return ret;
    }
    return PortInfo(PortIndex(), nullptr);
}

PortInfo KernelCreator::getOutputPortInfo(const std::string &name) const {
    std::string port;
    IndexType index = 0;
    if (!CommonUtil::parseGroupPort(name, port, index)) {
        return PortInfo();
    }
    if (0 == index) {
        auto outputIt = _outputMap.find(port);
        if (_outputMap.end() != outputIt) {
            return outputIt->second;
        }
    }
    auto outputGroupIt = _outputGroupMap.find(port);
    if (_outputGroupMap.end() != outputGroupIt) {
        auto ret = outputGroupIt->second;
        ret.index.index = index;
        return ret;
    }
    auto controlIt = _controlOutputMap.find(port);
    if (_controlOutputMap.end() != controlIt) {
        return controlIt->second;
    }
    return PortInfo(PortIndex(), nullptr);
}

std::string KernelCreator::getInputPortName(PortIndex portIndex) const {
    if (!portIndex.isGroup()) {
        auto index = portIndex.index;
        if (index >= _def->inputs().size()) {
            return std::string();
        }
        return _def->inputs(index).name();
    } else {
        auto group = portIndex.group;
        if (group >= _def->input_groups().size()) {
            return std::string();
        }
        return _def->input_groups(group).name();
    }
}

std::string KernelCreator::getOutputPortName(PortIndex portIndex) const {
    if (!portIndex.isGroup()) {
        auto index = portIndex.index;
        if (index >= _def->outputs().size()) {
            return std::string();
        }
        return _def->outputs(index).name();
    } else {
        auto group = portIndex.group;
        if (group >= _def->output_groups().size()) {
            return std::string();
        }
        return _def->output_groups(group).name();
    }
}

bool KernelCreator::getPortTypeStr(const std::string &portName,
                                   IoType ioType,
                                   std::string &typeStr) const
{
    if (0 == portName.find(NODE_RESOURCE_INPUT_PORT)) {
        typeStr = RESOURCE_DATA_TYPE_ID;
        return true;
    }
    if (IOT_INPUT == ioType) {
        auto portInfo = getInputPortInfo(portName);
        if (!portInfo.isValid()) {
            NAVI_KERNEL_LOG(ERROR, "kernel [%s] has no input port [%s]",
                            getName().c_str(), portName.c_str());
            return false;
        }
        auto portDef = static_cast<const InputDef *>(portInfo.def);
        assert(portDef);
        typeStr = portDef->data_type().name();
    } else {
        const auto &portInfo = getOutputPortInfo(portName);
        if (!portInfo.isValid()) {
            NAVI_KERNEL_LOG(ERROR, "kernel [%s] has no output port [%s]",
                            getName().c_str(), portName.c_str());
            return false;
        }
        auto portDef = static_cast<const OutputDef *>(portInfo.def);
        assert(portDef);
        typeStr = portDef->data_type().name();
    }
    return true;
}

const KernelDef *KernelCreator::def() const {
    return _def;
}

const BindInfos &KernelCreator::getBindInfos() const {
    return _bindInfos;
}

const DynamicResourceInfoMap &KernelCreator::getDynamicGroupInfoMap() const {
    return _dynamicGroupMap;
}

void KernelCreator::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    Creator::Jsonize(json);
    json.Jsonize("depend_on", _dependResources);
    if (_dependResourcesWithoutSelect != _dependResources) {
        json.Jsonize("depend_on_without_select", _dependResourcesWithoutSelect);
    }
    json.Jsonize("enable_build", _enableBuildResourceSet);
    std::map<std::string, bool> namedDataMap;
    for (const auto &namedData : _bindInfos.namedDataBindVec) {
        namedDataMap.emplace(namedData.name, namedData.require);
    }
    json.Jsonize("named_data", namedDataMap);
    if (_jsonAttrsDocument) {
        autil::legacy::RapidValue *attrs = _jsonAttrsDocument;
        json.Jsonize("attrs", attrs);
    }
    std::map<std::string, std::string> configKeyMap(_configKeyMap.begin(), _configKeyMap.end());
    json.Jsonize("config_key_map", configKeyMap);
}

}
