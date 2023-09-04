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

std::shared_ptr<IResource> ResourceMap::GetResourceWithoutLock(const std::string& name) const
{
    auto iterV = _versionResource.find(name);
    if (iterV != _versionResource.end()) {
        return iterV->second;
    }

    auto iterI = _inheritedResource.find(name);
    if (iterI != _inheritedResource.end()) {
        return iterI->second;
    }

    auto iterS = _segmentResource.find(name);
    if (iterS != _segmentResource.end()) {
        return iterS->second.second;
    }
    return nullptr;
}

std::shared_ptr<IResource> ResourceMap::GetResource(const std::string& name) const
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    return GetResourceWithoutLock(name);
}
Status ResourceMap::AddSegmentResource(const std::string& name, segmentid_t segmentId,
                                       const std::shared_ptr<IResource>& resource)
{
    if (!resource) {
        AUTIL_LOG(ERROR, "can not add segment resource with empty resource");
        return Status::InvalidArgs("resource is empty");
    }
    if (segmentId == INVALID_SEGMENTID) {
        AUTIL_LOG(ERROR, "can not add segment resource with invalid segmentId");
        return Status::InvalidArgs("segment id invalid");
    }
    std::lock_guard<std::mutex> guard(_resourceMutex);
    if (GetResourceWithoutLock(name) != nullptr) {
        AUTIL_LOG(ERROR, "resource [%s] already exist", name.c_str());
        return Status::Exist();
    }
    _segmentResource[name] = std::make_pair(segmentId, resource);
    return Status::OK();
}

Status ResourceMap::AddVersionResource(const std::string& name, const std::shared_ptr<IResource>& resource)
{
    if (!resource) {
        AUTIL_LOG(ERROR, "can not add segment resource with empty resource");
        return Status::InvalidArgs("resource is empty");
    }
    std::lock_guard<std::mutex> guard(_resourceMutex);
    if (GetResourceWithoutLock(name) != nullptr) {
        AUTIL_LOG(ERROR, "resource [%s] already exist", name.c_str());
        return Status::Exist();
    }
    _versionResource[name] = resource;
    return Status::OK();
}

Status ResourceMap::AddInheritedResource(const std::string& name, const std::shared_ptr<IResource>& resource)
{
    if (!resource) {
        AUTIL_LOG(ERROR, "can not add segment resource with empty resource");
        return Status::InvalidArgs("resource is empty");
    }
    std::lock_guard<std::mutex> guard(_resourceMutex);
    if (GetResourceWithoutLock(name) != nullptr) {
        AUTIL_LOG(ERROR, "resource [%s] already exist", name.c_str());
        return Status::Exist();
    }
    _inheritedResource[name] = resource;
    return Status::OK();
}

size_t ResourceMap::CurrentMemmoryUse() const
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    size_t memUse = 0;
    for (const auto& [_, resource] : _versionResource) {
        memUse += resource->CurrentMemmoryUse();
    }
    for (const auto& [_, resource] : _inheritedResource) {
        memUse += resource->CurrentMemmoryUse();
    }
    for (const auto& [_, resourcePair] : _segmentResource) {
        memUse += resourcePair.second->CurrentMemmoryUse();
    }
    return memUse;
}

void ResourceMap::ReclaimSegmentResource(const std::set<segmentid_t>& reservedSegments)
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    std::map<std::string, std::pair<segmentid_t, std::shared_ptr<IResource>>> notReclaimedResource;
    for (const auto& iter : _segmentResource) {
        if (reservedSegments.find(iter.second.first) != reservedSegments.end()) {
            notReclaimedResource.insert(iter);
        }
    }
    _segmentResource.swap(notReclaimedResource);
}

std::shared_ptr<ResourceMap> ResourceMap::Clone() const
{
    auto cloned = std::make_shared<ResourceMap>();
    std::lock_guard<std::mutex> guard(_resourceMutex);
    for (const auto& [name, resourcePair] : _segmentResource) {
        cloned->_segmentResource[name] = std::make_pair(resourcePair.first, resourcePair.second->Clone());
    }

    for (const auto& [name, resource] : _inheritedResource) {
        cloned->_inheritedResource[name] = resource->Clone();
    }
    return cloned;
}

void ResourceMap::TEST_Clear()
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    _versionResource.clear();
    _segmentResource.clear();
    _inheritedResource.clear();
}

void ResourceMap::TEST_RemoveResource(const std::string& name)
{
    std::lock_guard<std::mutex> guard(_resourceMutex);
    _versionResource.erase(name);
    _inheritedResource.erase(name);
    _segmentResource.erase(name);
}

} // namespace indexlibv2::framework
