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

#include <map>
#include <set>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace config {
class ConfigReader;
} // namespace config
} // namespace swift

namespace swift {
namespace config {

class ConfigVersionManager : public autil::legacy::Jsonizable {
public:
    typedef std::map<std::string, std::string> Key2ValMap;

public:
    ConfigVersionManager();
    ~ConfigVersionManager();

public:
    protocol::ErrorCode validateUpdateVersion(protocol::RoleType roleType, const std::string &updateVersionPath) const;
    protocol::ErrorCode validateRollback(protocol::RoleType roleType) const;
    bool needUpgradeRoleVersion(protocol::RoleType roleType, const std::string &updateVersionPath) const;
    bool isUpgrading(protocol::RoleType roleType = protocol::RoleType::ROLE_TYPE_ALL) const;
    bool isRollback(protocol::RoleType roleType) const;
    bool upgradeTargetRoleVersion(protocol::RoleType roleType);

    const std::string &getRoleCurrentVersion(protocol::RoleType roleType) const;
    const std::string &getRoleTargetVersion(protocol::RoleType roleType) const;
    const std::string &getRoleCurrentPath(protocol::RoleType roleType) const;
    const std::string &getRoleTargetPath(protocol::RoleType roleType) const;

public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

private:
    void updateWhiteList(config::ConfigReader &configReader,
                         std::map<std::string, std::set<std::string>> &sectionWhiteListMap) const;

    bool hasDiff(const std::map<std::string, std::string> &tarMap, const std::set<std::string> &whiteSet) const;
    bool hasDiff(const std::map<std::string, std::string> &curMap,
                 const std::map<std::string, std::string> &tarMap,
                 const std::set<std::string> &whiteSet) const;
    bool hasDiffInHippoFile(const std::string currentConfigPath, const std::string &updateVersionPath) const;

public:
    std::string currentBrokerConfigPath;
    std::string targetBrokerConfigPath;
    std::string currentAdminConfigPath;
    std::string currentBrokerRoleVersion;
    std::string targetBrokerRoleVersion;
    bool brokerRollback;

private:
    static const std::string EmptyString;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ConfigVersionManager);

} // namespace config
} // namespace swift
