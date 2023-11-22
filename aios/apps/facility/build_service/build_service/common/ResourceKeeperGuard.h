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
#pragma once

#include <assert.h>
#include <memory>
#include <string>

#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/WorkerNode.h"

namespace build_service { namespace common {

class ResourceKeeperGuard;

BS_TYPEDEF_PTR(ResourceKeeperGuard);

class ResourceKeeperGuard
{
public:
    ResourceKeeperGuard();
    ~ResourceKeeperGuard();

private:
    ResourceKeeperGuard(const ResourceKeeperGuard&);
    ResourceKeeperGuard& operator=(const ResourceKeeperGuard&);

public:
    bool init(const std::string& roleName, const config::ResourceReaderPtr& configReader,
              const ResourceKeeperPtr& resourceKeeper);
    const std::string& getResourceName() const
    {
        assert(_resourceKeeper);
        return _resourceKeeper->getResourceName();
    }

    void fillResourceKeeper(const proto::WorkerNodes& workerNode);
    ResourceKeeperPtr getResourceKeeper() { return _resourceKeeper; }

private:
    std::string _roleName;
    ResourceKeeperPtr _resourceKeeper;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::common
