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
#include "aios/network/gig/multi_call/service/LoadBalancerContext.h"

namespace multi_call {

VersionSelectorPtr
LoadBalancerContext::getVersionSelector(const std::string &bizName) {
    autil::ScopedLock lock(_mutex);
    auto iter = _versionSelectorMap.find(bizName);
    if (iter != _versionSelectorMap.end()) {
        return iter->second;
    }
    // create
    VersionSelectorPtr versionSelector =
        _versionSelectorFactory->create(bizName);
    if (versionSelector) {
        _versionSelectorMap[bizName] = versionSelector;
    }
    return versionSelector;
}

ProviderSelectorPtr
LoadBalancerContext::getProviderSelector(const std::string &bizName) {
    autil::ScopedLock lock(_mutex);
    auto iter = _providerSelectorMap.find(bizName);
    if (iter != _providerSelectorMap.end()) {
        return iter->second;
    }
    // create
    ProviderSelectorPtr providerSelector =
        _providerSelectorFactory->create(bizName);
    if (providerSelector) {
        _providerSelectorMap[bizName] = providerSelector;
    }
    return providerSelector;
}

} // namespace multi_call
