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
#include "build_service/common/ResourceKeeperCreator.h"

#include <assert.h>
#include <iosfwd>

#include "alog/Logger.h"
#include "build_service/common/CheckpointResourceKeeper.h"
#include "build_service/common/IndexCheckpointResourceKeeper.h"
#include "build_service/common/SwiftResourceKeeper.h"

using namespace std;

namespace build_service { namespace common {
BS_LOG_SETUP(controlflow, ResourceKeeperCreator);

ResourceKeeperCreator::ResourceKeeperCreator() {}

ResourceKeeperCreator::~ResourceKeeperCreator() {}

ResourceKeeperPtr ResourceKeeperCreator::create(const std::string& resourceName, const std::string& type,
                                                const ResourceContainerPtr& resourceContainer)
{
    if (type == "swift") {
        return ResourceKeeperPtr(new SwiftResourceKeeper(resourceName, type, resourceContainer));
    }
    if (type == "index_checkpoint") {
        return ResourceKeeperPtr(new IndexCheckpointResourceKeeper(resourceName, type, resourceContainer));
    }
    if (type == "checkpoint") {
        return ResourceKeeperPtr(new CheckpointResourceKeeper(resourceName, type, resourceContainer));
    }
    assert(false);
    BS_LOG(ERROR, "unknown resourceKeeper type[%s]!", type.c_str());
    return ResourceKeeperPtr();
}

}} // namespace build_service::common
