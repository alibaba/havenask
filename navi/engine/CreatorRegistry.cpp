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
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/CreatorRegistryImpl.h"

namespace navi {

CreatorRegistry *CreatorRegistry::CURRENT[] = {};
CreatorRegistry *CreatorRegistry::NAVI_BUILDIN[] = {};

__attribute__((destructor)) void destructBuildinRegistry() {
    for (int i = 0; i < RT_MAX; i++) {
        delete CreatorRegistry::buildin(RegistryType(i));
    }
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

void CreatorRegistry::addModuleInitFunc(const NaviModuleInitFunc &func) {
    _impl->addModuleInitFunc(func);
}

void CreatorRegistry::addCreatorFunc(const NaviCreatorFunc &func) {
    _impl->addCreatorFunc(func);
}

void CreatorRegistry::merge(const CreatorRegistry &registry) {
    _impl->merge(registry._impl);
}

bool CreatorRegistry::initModule() {
    return _impl->initModule();
}

bool CreatorRegistry::initCreators() {
    return _impl->initCreators();
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

}

