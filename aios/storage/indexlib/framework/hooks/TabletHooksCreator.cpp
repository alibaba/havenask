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
#include "indexlib/framework/hooks/TabletHooksCreator.h"

#include "indexlib/framework/hooks/ITabletDeployerHook.h"

namespace indexlib::framework {

TabletHooksCreator::TabletHooksCreator() {}

TabletHooksCreator::~TabletHooksCreator() {}

std::unique_ptr<ITabletDeployerHook> TabletHooksCreator::CreateTabletDeployerHook(std::string tableType) const
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _tabletDeployerHooks.find(tableType);
    if (it == _tabletDeployerHooks.end() || !it->second) {
        return nullptr;
    }
    return it->second->Create();
}

void TabletHooksCreator::RegisterTabletDeployerHook(const std::string& tableType,
                                                    std::unique_ptr<ITabletDeployerHook> hook)
{
    std::lock_guard<std::mutex> lock(_mutex);
    if (_tabletDeployerHooks.count(tableType)) {
        std::abort();
    }
    _tabletDeployerHooks[tableType] = std::move(hook);
}

} // namespace indexlib::framework
