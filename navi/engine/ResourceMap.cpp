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
#include "navi/engine/ResourceMap.h"
#include "navi/engine/Resource.h"

namespace navi {

ResourceMap::ResourceMap() {
}

ResourceMap::~ResourceMap() {
}

bool ResourceMap::addResource(const ResourcePtr &resource) {
    if (!resource) {
        return false;
    }
    const auto &name = resource->getName();
    autil::ScopedWriteLock scope(_lock);
    auto it = _map.find(name);
    if (it == _map.end()) {
        _map.emplace(std::make_pair(name, resource));
        NAVI_KERNEL_LOG(SCHEDULE1, "[%p] add resource [%s] [%p]", this,
                        name.c_str(), resource.get());
        return true;
    } else if (resource != it->second) {
        NAVI_KERNEL_LOG(DEBUG,
                        "resource [%s] already exists, old [%p] new [%p]",
                        name.c_str(), it->second.get(), resource.get());
        return false;
    }
    return true;
}

bool ResourceMap::addResource(const ResourceMap &resourceMap) {
    for (const auto &pair : resourceMap._map) {
        if (!addResource(pair.second)) {
            NAVI_KERNEL_LOG(DEBUG, "add resource failed [%s]",
                            pair.first.c_str());
            return false;
        }
    }
    return true;
}

ResourcePtr ResourceMap::getResource(const std::string &name) const {
    autil::ScopedReadLock scope(_lock);
    auto it = _map.find(name);
    if (it != _map.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

bool ResourceMap::hasResource(const std::string &name) const {
    autil::ScopedReadLock scope(_lock);
    return _map.end() != _map.find(name);
}

void ResourceMap::clear() {
    autil::ScopedWriteLock scope(_lock);
    _map.clear();
}

const std::unordered_map<std::string, ResourcePtr> &ResourceMap::getMap() const {
    return _map;
}

std::ostream &operator<<(std::ostream &os, const ResourceMap &resourceMap) {
    os << "{";
    for (auto &pair : resourceMap._map) {
        os << "(" << pair.first << ", " << pair.second.get() << ")";
    }
    return os << "}";
}

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

std::ostream &operator<<(std::ostream &os, const MultiLayerResourceMap &multiLayerResourceMap) {
    for (auto *resourceMap : multiLayerResourceMap._resourceMapVec) {
        autil::StringUtil::toStream(os, *resourceMap);
        os << ",";
    }
    return os;
}

}
