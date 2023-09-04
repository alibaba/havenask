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
#include "navi/engine/MultiLayerResourceMap.h"

namespace navi {

void MultiLayerResourceMap::addResourceMap(const ResourceMap *resourceMap) {
    if (resourceMap) {
        _resourceMapVec.push_back(resourceMap);
    }
}

ResourcePtr MultiLayerResourceMap::getResource(const std::string &name) const {
    for (auto resourceMap : _resourceMapVec) {
        auto resource = resourceMap->getResource(name);
        if (resource) {
            return resource;
        }
    }
    return nullptr;
}

bool MultiLayerResourceMap::hasResource(const std::string &name) const {
    for (auto resourceMap : _resourceMapVec) {
        if (resourceMap->hasResource(name)) {
            return true;
        }
    }
    return false;
}

bool MultiLayerResourceMap::hasResource(const ResourcePtr &resource) const {
    if (!resource) {
        return false;
    }
    const auto &name = resource->getName();
    for (auto resourceMap : _resourceMapVec) {
        auto depend = resourceMap->getResource(name);
        if (!depend) {
            continue;
        }
        return (resource == depend);
    }
    return false;
}

void MultiLayerResourceMap::append(const MultiLayerResourceMap &other) {
    const auto &otherMap = other._resourceMapVec;
    _resourceMapVec.insert(_resourceMapVec.end(), otherMap.begin(),
                           otherMap.end());
}

std::ostream &operator<<(std::ostream &os, const MultiLayerResourceMap &multiLayerResourceMap) {
    for (auto *resourceMap : multiLayerResourceMap._resourceMapVec) {
        os << autil::StringUtil::toString(*resourceMap) << ",";
    }
    return os;
}

}

