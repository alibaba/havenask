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
#ifndef ISEARCH_BS_METATAGAPPENDER_H
#define ISEARCH_BS_METATAGAPPENDER_H

#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "master_framework/AppPlan.h"

namespace build_service { namespace admin {

struct GroupRole {
public:
    std::string roleName;
    std::string groupName;
    proto::RoleType type;
    std::string gangRegionId;
    bool isHighQuality;

public:
    GroupRole();
    GroupRole(const std::string& roleName, const std::string& groupName);
    GroupRole(const std::string& roleName, const std::string& groupName, proto::RoleType type);

private:
    // for test
    GroupRole(const std::string& roleName, const std::string& groupName, proto::RoleType type,
              const std::string& gangRegionId);

public:
    bool operator==(const GroupRole& other);
    bool operator!=(const GroupRole& other);
};

typedef std::map<std::string, GroupRole> GroupRolePairs;

typedef MF_NS(master_base)::RolePlan RolePlan;

class MetaTagAppender
{
public:
    MetaTagAppender(const std::string& appName, bool enableGangRegion)
        : _appName(appName)
        , _enableGangRegion(enableGangRegion)
    {
    }
    ~MetaTagAppender() {}

private:
    MetaTagAppender(const MetaTagAppender&);
    MetaTagAppender& operator=(const MetaTagAppender&);

public:
    void init(const GroupRolePairs& groupRoles);

    void append(RolePlan& rolePlan, const GroupRole& groupRole);

private:
    void fillIfEmpty(hippo::MetaTagMap& metaTags, const std::string& key, const std::string& value);

private:
    std::string _appName;
    bool _enableGangRegion;
    std::map<std::string, uint32_t> _regionIdToCount;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MetaTagAppender);

}} // namespace build_service::admin

#endif // ISEARCH_BS_METATAGAPPENDER_H
