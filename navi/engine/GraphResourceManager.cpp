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
#include "navi/engine/GraphResourceManager.h"

namespace navi {

GraphResourceManager::GraphResourceManager() {
}

GraphResourceManager::~GraphResourceManager() {
}

ResourceMapPtr GraphResourceManager::getBizResourceMap(
        const LocationDef &location)
{
    const auto &bizName = location.biz_name();
    auto partId = location.this_part_id();
    if (INVALID_NAVI_PART_ID == partId) {
        return nullptr;
    }
    {
        autil::ScopedReadLock lock(_mapLock);
        auto ret = doGetMap(bizName, partId);
        if (ret) {
            return ret;
        }
    }
    {
        autil::ScopedWriteLock lock(_mapLock);
        auto ret = doGetMap(bizName, partId);
        if (ret) {
            return ret;
        }
        ResourceMapPtr resourceMap(new ResourceMap());
        _bizResourceMap[bizName][partId] = resourceMap;
        return resourceMap;
    }
}

ResourceMapPtr GraphResourceManager::doGetMap(const std::string &bizName,
                                              NaviPartId partId) const
{
    auto itBiz = _bizResourceMap.find(bizName);
    if (_bizResourceMap.end() == itBiz) {
        return nullptr;
    }
    const auto &partMap = itBiz->second;
    auto itPart = partMap.find(partId);
    if (partMap.end() != itPart) {
        return itPart->second;
    }
    return nullptr;
}

}

