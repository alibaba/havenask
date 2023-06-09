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
#include "autil/plugin/InterfaceManager.h"

#include <utility>

#include "autil/plugin/BaseInterface.h"

namespace autil {

AUTIL_LOG_SETUP(plugin, InterfaceManager);

InterfaceManager::~InterfaceManager() {
    clear();
}

void InterfaceManager::clear() {
    for (auto it : _interfaceMap) {
        delete it.second;
    }
    _interfaceMap.clear();
}

bool InterfaceManager::addInterface(const std::string &name, BaseInterface* interface) {
    if (name.empty() || interface == nullptr) {
        AUTIL_LOG(ERROR, "register interface failed, empty interface name or pointer ");
        return false;
    }
    auto iter = _interfaceMap.find(name);
    if (iter == _interfaceMap.end()) {
        _interfaceMap[name] = interface;
        AUTIL_LOG(INFO, "register interface %s successfully", name.c_str());
    } else {
        AUTIL_LOG(ERROR, "register interface %s failed, already existed", name.c_str());
        delete interface;
        return false;
    }
    return true;
}

BaseInterface *InterfaceManager::find(const std::string &name) const {
    auto iter = _interfaceMap.find(name);
    if (iter == _interfaceMap.end()) {
        return nullptr;
    }
    return iter->second;
}


} // namespace autil
