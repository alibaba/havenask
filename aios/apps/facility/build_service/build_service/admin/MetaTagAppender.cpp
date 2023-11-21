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
#include "build_service/admin/MetaTagAppender.h"

#include <assert.h>
#include <cstdint>
#include <iosfwd>
#include <utility>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace hippo;
using namespace autil;
using namespace build_service::proto;
namespace build_service { namespace admin {
BS_LOG_SETUP(admin, MetaTagAppender);

GroupRole::GroupRole() : type(ROLE_UNKNOWN), isHighQuality(false) {}

GroupRole::GroupRole(const string& role, const string& group)
    : roleName(role)
    , groupName(group)
    , type(ROLE_UNKNOWN)
    , isHighQuality(false)
{
}

GroupRole::GroupRole(const string& role, const string& group, RoleType roleType)
    : roleName(role)
    , groupName(group)
    , type(roleType)
    , isHighQuality(false)
{
}

GroupRole::GroupRole(const string& role, const string& group, RoleType roleType, const string& gangId)
    : roleName(role)
    , groupName(group)
    , type(roleType)
    , gangRegionId(gangId)
    , isHighQuality(false)
{
}

bool GroupRole::operator==(const GroupRole& other)
{
    return roleName == other.roleName && groupName == other.groupName && type == other.type &&
           gangRegionId == other.gangRegionId && isHighQuality == other.isHighQuality;
}

bool GroupRole::operator!=(const GroupRole& other) { return !(operator==(other)); }

void MetaTagAppender::init(const GroupRolePairs& groupRoles)
{
    // fill _regionIdToCount
    assert(_regionIdToCount.empty());
    if (!_enableGangRegion) {
        return;
    }
    for (auto iter = groupRoles.begin(); iter != groupRoles.end(); ++iter) {
        const GroupRole& groupRole = iter->second;
        if (groupRole.gangRegionId.empty()) {
            continue;
        }
        _regionIdToCount[groupRole.gangRegionId]++;
    }
}

void MetaTagAppender::append(RolePlan& rolePlan, const GroupRole& groupRole)
{
    MetaTagMap& metaTags = rolePlan.metaTags;
    metaTags.erase("app.hippo.io/gangRegion");
    const string& gangRegionId = groupRole.gangRegionId;

    fillIfEmpty(metaTags, "app.hippo.io/appShortName", _appName);
    fillIfEmpty(metaTags, "app.hippo.io/roleShortName", groupRole.groupName);
    if (_enableGangRegion && !gangRegionId.empty()) {
        auto iter = _regionIdToCount.find(gangRegionId);
        if (iter == _regionIdToCount.end()) {
            BS_LOG(ERROR, "invaild gangRegionId %s", gangRegionId.c_str());
        } else {
            fillIfEmpty(metaTags, "app.hippo.io/gangRegion", gangRegionId + "_" + StringUtil::toString(iter->second));
        }
    }
    fillIfEmpty(metaTags, "app.hippo.io/appType", groupRole.type == RoleType::ROLE_PROCESSOR ? "service" : "job");
}

void MetaTagAppender::fillIfEmpty(MetaTagMap& metaTags, const string& key, const string& value)
{
    auto iter = metaTags.find(key);
    if (iter != metaTags.end() && !iter->second.empty()) {
        return;
    }
    assert(!value.empty());
    metaTags[key] = value;
}

}} // namespace build_service::admin
