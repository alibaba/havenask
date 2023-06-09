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
#include "build_service/common/ResourceKeeperGuard.h"

#include "autil/legacy/jsonizable.h"
#include "build_service/common/ResourceKeeperCreator.h"

using namespace std;
using autil::legacy::Any;
using autil::legacy::Jsonizable;

namespace build_service { namespace common {
BS_LOG_SETUP(common, ResourceKeeperGuard);

ResourceKeeperGuard::ResourceKeeperGuard() {}

ResourceKeeperGuard::~ResourceKeeperGuard()
{
    if (_resourceKeeper) {
        _resourceKeeper->deleteApplyer(_roleName);
    }
}

bool ResourceKeeperGuard::init(const std::string& roleName, const config::ResourceReaderPtr& configReader,
                               const ResourceKeeperPtr& resourceKeeper)
{
    if (!resourceKeeper->prepareResource(roleName, configReader)) {
        return false;
    }
    _roleName = roleName;
    _resourceKeeper = resourceKeeper;
    return true;
}

void ResourceKeeperGuard::fillResourceKeeper(const proto::WorkerNodes& workerNodes)
{
    _resourceKeeper->syncResources(_roleName, workerNodes);
}

}} // namespace build_service::common
