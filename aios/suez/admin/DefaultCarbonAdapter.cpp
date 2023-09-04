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
#include "suez/admin/DefaultCarbonAdapter.h"

#include "carbon/RoleManagerWrapper.h"
#include "suez/admin/GroupDescGenerator.h"

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, DefaultCarbonAdapter);

DefaultCarbonAdapter::DefaultCarbonAdapter(const std::shared_ptr<carbon::RoleManagerWrapper> &roleManager)
    : _roleManager(roleManager) {}

DefaultCarbonAdapter::~DefaultCarbonAdapter() = default;

bool DefaultCarbonAdapter::commitToCarbon(const AdminTarget &target, const JsonNodeRef::JsonMap *statusInfo) {
    std::map<std::string, carbon::GroupDesc> groups;
    try {
        GroupDescGenerator::generateGroupDescs(target.toJsonMap(), statusInfo, groups);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "generate group desc failed, error: %s", e.what());
        return false;
    }
    return _roleManager->setGroups(groups);
}

} // namespace suez
