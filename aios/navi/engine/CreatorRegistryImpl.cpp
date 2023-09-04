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
#include "navi/engine/CreatorRegistryImpl.h"
#include "navi/engine/NaviConfigContext.h"
#include "navi/engine/ModuleManager.h"
#include "navi/engine/NaviSnapshotSummary.h"
#include "navi/log/NaviLogger.h"

namespace navi {

CreatorRegistryImpl::CreatorRegistryImpl()
    : _inited(false)
{
}

CreatorRegistryImpl::~CreatorRegistryImpl() {
    clear();
}

bool CreatorRegistryImpl::addModuleInitFunc(const NaviModuleInitFunc &func) {
    if (_inited) {
        NAVI_KERNEL_LOG(ERROR, "register ModuleInitFunc after init");
        return false;
    }
    _moduleInitFuncList.push_back(func);
    return true;
}

bool CreatorRegistryImpl::addCreatorFunc(const NaviCreatorFunc &func) {
    if (_inited) {
        NAVI_KERNEL_LOG(ERROR, "register CreatorFunc after init");
        return false;
    }
    _funcList.push_back(func);
    return true;
}

void CreatorRegistryImpl::addJsonizeFunc(const NaviJsonizeInfoFunc &func) {
    _jsonizeFuncSet.insert(func);
}

void CreatorRegistryImpl::addJsonizeInfo(const NaviJsonizeInfoFunc &func) {
    NaviJsonizeInfo info;
    func(info);
    // void abc::def::config()::NaviJsonizeHelper0::json_log_0()
    const auto &functionName = info._functionName;
    auto pos = functionName.find("(");
    std::string className;
    if (pos != std::string::npos) {
        pos = functionName.rfind("::", pos - 1);
        if (pos != std::string::npos) {
            auto begin = functionName.rfind(" ", pos - 1);
            if (begin != std::string::npos) {
                begin++;
                className = functionName.substr(begin, pos - begin);
            }
        }
    } else {
        className = functionName;
    }
    auto &vec = _jsonizeInfoMap[className];
    vec.push_back(info);
}

bool CreatorRegistryImpl::initModule(Module *module) {
    if (_inited) {
        return true;
    }
    for (const auto &func : _moduleInitFuncList) {
        if (!func()) {
            NAVI_KERNEL_LOG(ERROR, "call module init func failed");
            return false;
        }
    }
    for (const auto &func : _jsonizeFuncSet) {
        addJsonizeInfo(func);
    }
    _module = module;
    return true;
}

void CreatorRegistryImpl::addSubRegistry(const CreatorRegistryImpl *impl) {
    _jsonizeInfoMap.insert(impl->_jsonizeInfoMap.begin(), impl->_jsonizeInfoMap.end());
    _subRegistrys.push_back(impl);
}

bool CreatorRegistryImpl::initCreators(bool isBuildin) {
    for (auto sub : _subRegistrys) {
        if (!sub->getCreators(isBuildin, _creatorMap)) {
            return false;
        }
    }
    return true;
}

bool CreatorRegistryImpl::getCreators(bool isBuildin,
                                      CreatorMap &creatorMap) const
{
    bool ok = true;
    for (const auto &func : _funcList) {
        CreatorPtr creator(func());
        assert(creator);
        if (!creator->init(isBuildin, _module)) {
            ok = false;
            continue;
        }
        const auto &name = creator->getName();
        auto ret = creatorMap.emplace(std::make_pair(name, creator));
        if (!ret.second) {
            auto it = creatorMap.find(name);
            if (it != creatorMap.end()) {
                const auto &prev = it->second;
                const auto &current = creator;
                NAVI_KERNEL_LOG(
                    ERROR,
                    "found multiple registry for [%s], registry 1: file [%s] "
                    "module [%s], registry 2: file [%s] module [%s]",
                    name.c_str(), prev->getSourceFile(),
                    Module::getModulePath(prev->getModule()).c_str(),
                    current->getSourceFile(),
                    Module::getModulePath(current->getModule()).c_str());
            }
            ok = false;
            continue;
        }
    }
    return ok;
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
    _moduleInitFuncList.clear();
    _funcList.clear();
    _creatorMap.clear();
    _jsonizeInfoMap.clear();
}

void CreatorRegistryImpl::setInited(bool inited) {
    _inited = inited;
}

void CreatorRegistryImpl::fillJsonizeInfo(const NaviJsonizeInfoMap &jsonizeInfoMap) {
    for (const auto &pair : _creatorMap) {
        pair.second->initJsonizeInfo(jsonizeInfoMap);
    }
}

}
