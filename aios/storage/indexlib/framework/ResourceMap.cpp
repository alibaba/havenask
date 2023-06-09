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
#include "indexlib/framework/ResourceMap.h"

#include <cassert>

#include "indexlib/base/Constant.h"
#include "indexlib/framework/IResource.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, ResourceMap);

std::shared_ptr<IResource> ResourceMap::GetResource(const std::string& name) const
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    auto iter = _resource.find(name);
    if (iter != _resource.end()) {
        return iter->second.second;
    }
    return nullptr;
}

Status ResourceMap::AddSegmentResource(const std::string& name, segmentid_t segmentId,
                                       const std::shared_ptr<IResource>& resource)
{
    if (segmentId == INVALID_SEGMENTID) {
        AUTIL_LOG(ERROR, "can not add segment resource with invalid segmentId");
        return Status::InvalidArgs("segment id invalid");
    }
    std::lock_guard<std::mutex> guard(_resourceMutex);
    auto iter = _resource.find(name);
    if (iter != _resource.end()) {
        AUTIL_LOG(ERROR, "resource [%s] already exist", name.c_str());
        return Status::Exist();
    }
    _resource[name] = std::make_pair(segmentId, resource);
    return Status::OK();
}

Status ResourceMap::AddVersionResource(const std::string& name, const std::shared_ptr<IResource>& resource)
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    auto iter = _resource.find(name);
    if (iter != _resource.end()) {
        AUTIL_LOG(ERROR, "resource [%s] already exist", name.c_str());
        return Status::Exist("resource already exist");
    }
    _resource[name] = std::make_pair(INVALID_SEGMENTID, resource);
    return Status::OK();
}

size_t ResourceMap::CurrentMemmoryUse() const
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    size_t memUse = 0;
    for (const auto& [_, resourcePair] : _resource) {
        memUse += resourcePair.second->CurrentMemmoryUse();
    }
    return memUse;
}

void ResourceMap::ReclaimSegmentResource(const std::set<segmentid_t>& reservedSegments)
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    std::map<std::string, std::pair<segmentid_t, std::shared_ptr<IResource>>> notReclaimedResource;
    for (const auto& iter : _resource) {
        if (reservedSegments.find(iter.second.first) != reservedSegments.end() ||
            iter.second.first == INVALID_SEGMENTID) {
            notReclaimedResource.insert(iter);
        }
    }

    _resource.swap(notReclaimedResource);
}

std::shared_ptr<ResourceMap> ResourceMap::Clone() const
{
    std::map<std::string, std::pair<segmentid_t, std::shared_ptr<IResource>>> clonedMap;
    {
        std::lock_guard<std::mutex> guard(_resourceMutex);
        clonedMap = _resource;
    }
    auto manager = std::make_shared<ResourceMap>();
    for (const auto& [name, resourcePair] : clonedMap) {
        if (resourcePair.first != INVALID_SEGMENTID) {
            manager->_resource[name] = std::make_pair(resourcePair.first, resourcePair.second->Clone());
        }
    }
    return manager;
}

void ResourceMap::TEST_Clear()
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    _resource.clear();
}

void ResourceMap::TEST_RemoveResource(const std::string& name)
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    _resource.erase(name);
}

} // namespace indexlibv2::framework
