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
#include "navi/engine/CreatorRegistryImpl.h"

namespace navi {

CreatorRegistryImpl::CreatorRegistryImpl() {
}

CreatorRegistryImpl::~CreatorRegistryImpl() {
    clear();
}

void CreatorRegistryImpl::addModuleInitFunc(const NaviModuleInitFunc &func) {
    _moduleInitFuncList.push_back(func);
}

void CreatorRegistryImpl::addCreatorFunc(const NaviCreatorFunc &func) {
    _funcList.push_back(func);
}

void CreatorRegistryImpl::merge(const CreatorRegistryImpl *impl) {
    _moduleInitFuncList.insert(_moduleInitFuncList.end(),
                               impl->_moduleInitFuncList.begin(),
                               impl->_moduleInitFuncList.end());
    _funcList.insert(_funcList.end(), impl->_funcList.begin(),
                     impl->_funcList.end());
}

bool CreatorRegistryImpl::initModule() {
    for (const auto &func : _moduleInitFuncList) {
        if (!func()) {
            NAVI_KERNEL_LOG(ERROR, "call module init func failed");
            return false;
        }
    }
    return true;
}

bool CreatorRegistryImpl::initCreators() {
    for (const auto &func : _funcList) {
        CreatorPtr creator(func());
        assert(creator);
        if (!creator->init()) {
            return false;
        }
        auto ret = _creatorMap.emplace(std::make_pair(creator->getName(), creator));
        if (!ret.second) {
            NAVI_KERNEL_LOG(ERROR, "found multiple registry for [%s]",
                            creator->getName().c_str());
            return false;
        }
    }
    return true;
}

Creator *CreatorRegistryImpl::getCreator(const std::string &name) const
{
    auto it = _creatorMap.find(name);
    if (_creatorMap.end() == it) {
        return nullptr;
    }
    return it->second.get();
}

const CreatorMap &CreatorRegistryImpl::getCreatorMap() const {
    return _creatorMap;
}

void CreatorRegistryImpl::clear() {
    _funcList.clear();
    _creatorMap.clear();
}

}

