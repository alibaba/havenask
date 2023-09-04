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
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/NaviConfigContext.h"
#include "navi/engine/CreatorRegistryImpl.h"
#include "navi/engine/ModuleManager.h"
#include "navi/log/NaviLogger.h"

namespace navi {

CreatorRegistry *CreatorRegistry::CURRENT[] = {};
CreatorRegistry *CreatorRegistry::NAVI_BUILDIN[] = {};

__attribute__((destructor)) void destructBuildinRegistry() {
    for (int i = 0; i < RT_MAX; i++) {
        delete CreatorRegistry::buildin(RegistryType(i));
    }
}

Creator::Creator(const char *sourceFile)
    : _isBuildin(false)
    , _sourceFile(sourceFile)
    , _module(nullptr)
{
}

Creator::~Creator() {}

const std::string &Creator::getModulePath() const {
    return Module::getModulePath(_module);
}

void Creator::initJsonizeInfo(const NaviJsonizeInfoMap &jsonizeInfoMap) {
    auto className = getClassName();
    auto it = jsonizeInfoMap.find(className);
    if (jsonizeInfoMap.end() != it) {
        _jsonizeInfoVec = it->second;
    }
}

void Creator::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("name", getName());
    json.Jsonize("source", getSourceFile());
    json.Jsonize("buildin", isBuildin());
    json.Jsonize("jsonize_infos", _jsonizeInfoVec);
}

CreatorRegistry::CreatorRegistry()
    : _impl(new CreatorRegistryImpl())
{
}

CreatorRegistry::~CreatorRegistry() {
    delete _impl;
}

CreatorRegistry *CreatorRegistry::current(RegistryType type) {
    if (!CURRENT[type]) {
        CURRENT[type] = buildin(type);
    }
    return CURRENT[type];
}

void CreatorRegistry::setCurrent(RegistryType type, CreatorRegistry *current) {
    CURRENT[type] = current;
}

CreatorRegistry *CreatorRegistry::buildin(RegistryType type) {
    if (!NAVI_BUILDIN[type]) {
        NAVI_BUILDIN[type] = new CreatorRegistry();
    }
    return NAVI_BUILDIN[type];
}

bool CreatorRegistry::addModuleInitFunc(const NaviModuleInitFunc &func) {
    return _impl->addModuleInitFunc(func);
}

bool CreatorRegistry::addCreatorFunc(const NaviCreatorFunc &func) {
    return _impl->addCreatorFunc(func);
}

void CreatorRegistry::addSubRegistry(const CreatorRegistry &registry) {
    _impl->addSubRegistry(registry._impl);
}

void CreatorRegistry::addJsonizeFunc(NaviJsonizeInfoFunc func) {
    _impl->addJsonizeFunc(func);
}

bool CreatorRegistry::initModule(Module *module) {
    return _impl->initModule(module);
}

bool CreatorRegistry::initCreators(bool isBuildin) {
    return _impl->initCreators(isBuildin);
}

Creator *CreatorRegistry::getCreator(const std::string &name) const {
    return _impl->getCreator(name);
}

const CreatorMap &CreatorRegistry::getCreatorMap() const {
    return _impl->getCreatorMap();
}

void CreatorRegistry::clear() {
    _impl->clear();
}

void CreatorRegistry::setInited(bool inited) {
    _impl->setInited(inited);
}

const NaviJsonizeInfoMap &CreatorRegistry::getJsonizeInfoMap() const {
    return _impl->getJsonizeInfoMap();
}

void CreatorRegistry::fillJsonizeInfo(const NaviJsonizeInfoMap &jsonizeInfoMap) {
    _impl->fillJsonizeInfo(jsonizeInfoMap);
}

}
