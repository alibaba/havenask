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

ResourceMap::ResourceMap(const std::unordered_map<std::string, ResourcePtr> &map)
    : _map(map)
{
}

ResourceMap::~ResourceMap() {
}

bool ResourceMap::addResource(const ResourcePtr &resource) {
    if (!resource) {
        return false;
    }
    return addResource(resource->getName(), resource);
}

bool ResourceMap::addResource(const std::string &name, const std::shared_ptr<Resource> &resource) {
    if (!resource) {
        return false;
    }
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
    auto map = resourceMap.getMap();
    bool ret = true;
    for (const auto &pair : map) {
        if (!addResource(pair.second)) {
            NAVI_KERNEL_LOG(DEBUG, "add resource failed [%s]",
                            pair.first.c_str());
            ret = false;
        }
    }
    return ret;
}

bool ResourceMap::overrideResource(const std::shared_ptr<Resource> &resource) {
    if (!resource) {
        return false;
    }
    return overrideResource(resource->getName(), resource);
}

bool ResourceMap::overrideResource(const std::string &name, const std::shared_ptr<Resource> &resource) {
    autil::ScopedWriteLock scope(_lock);
    _map[name] = resource;
    return true;
}

void ResourceMap::setResource(const ResourceMap &resourceMap) {
    auto map = resourceMap.getMap();
    autil::ScopedWriteLock scope(_lock);
    _map = std::move(map);
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

size_t ResourceMap::size() const {
    autil::ScopedReadLock scope(_lock);
    return _map.size();
}

std::unordered_map<std::string, ResourcePtr> ResourceMap::getMap() const {
    autil::ScopedReadLock scope(_lock);
    return _map;
}

std::ostream &operator<<(std::ostream &os, const ResourceMap &resourceMap) {
    auto map = resourceMap.getMap();
    os << "count: " << map.size() << ": ";
    os << "{";
    for (const auto &pair : map) {
        os << "(" << pair.first << ", " << pair.second.get() << ")";
    }
    return os << "}";
}

}
