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
#include "navi/engine/ResourceInitContext.h"

namespace navi {

ResourceInitContext::ResourceInitContext(
        const std::string &configPath,
        const std::string &bizName,
        NaviPartId partCount,
        NaviPartId partId,
        const ResourceMap &resourceMap)
    : _configPath(configPath)
    , _bizName(bizName)
    , _partCount(partCount)
    , _partId(partId)
    , _resourceMap(resourceMap)
{
}

ResourceInitContext::~ResourceInitContext() {
}

Resource *ResourceInitContext::getDependResource(const std::string &name) const {
    return _resourceMap.getResource(name).get();
}

const ResourceMap &ResourceInitContext::getDependResourceMap() const {
    return _resourceMap;
}

const std::string &ResourceInitContext::getConfigPath() const {
    return _configPath;
}

}
