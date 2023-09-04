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
#include "swift/config/ConfigVersionManager.h"

#include <cstddef>
#include <stdint.h>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/config/AdminConfig.h"
#include "swift/config/ConfigDefine.h"
#include "swift/config/ConfigReader.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace config {
AUTIL_LOG_SETUP(swift, ConfigVersionManager);

using namespace swift::protocol;
using namespace swift::config;

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib::fs;

const std::string ConfigVersionManager::EmptyString;

ConfigVersionManager::ConfigVersionManager()
    : currentBrokerRoleVersion("0"), targetBrokerRoleVersion("0"), brokerRollback(false) {}

ConfigVersionManager::~ConfigVersionManager() {}

ErrorCode ConfigVersionManager::validateUpdateVersion(protocol::RoleType roleType,
                                                      const string &updateVersionPath) const {
    if (isUpgrading(roleType) || isRollback(roleType)) {
        return ERROR_BROKER_IS_UPGRADING;
    }
    const auto &currentConfigPath = getRoleCurrentPath(roleType);
    if (currentConfigPath == updateVersionPath) {
        return ERROR_UPGRADE_VERSION_SAME_AS_CURRENT;
    }
    AdminConfig *adminUpdateConfig = AdminConfig::loadConfig(updateVersionPath);
    AdminConfig *adminCurrentConfig = AdminConfig::loadConfig(currentConfigPath);
    ErrorCode retError = ERROR_NONE;
    if (adminUpdateConfig == NULL || adminCurrentConfig == NULL ||
        adminUpdateConfig->getApplicationId() != adminCurrentConfig->getApplicationId()) {
        retError = ERROR_ADMIN_APPLICATION_ID_CHANGED;
    }

    DELETE_AND_SET_NULL(adminUpdateConfig);
    DELETE_AND_SET_NULL(adminCurrentConfig);
    return retError;
}

protocol::ErrorCode ConfigVersionManager::validateRollback(protocol::RoleType roleType) const {
    if (isRollback(roleType)) {
        return ERROR_CONFIG_IS_ROLLBACKING;
    }
    if (roleType == ROLE_TYPE_BROKER) {
        if (currentBrokerConfigPath == targetBrokerConfigPath && currentBrokerRoleVersion == targetBrokerRoleVersion) {
            return ERROR_NOT_NEED_ROLLBACK;
        }
    }
    return ERROR_NONE;
}

bool ConfigVersionManager::isUpgrading(protocol::RoleType roleType) const {
    if (roleType == ROLE_TYPE_BROKER) {
        return currentBrokerConfigPath != targetBrokerConfigPath || currentBrokerRoleVersion != targetBrokerRoleVersion;
    } else if (roleType == ROLE_TYPE_ALL) {
        return currentBrokerConfigPath != targetBrokerConfigPath || currentBrokerRoleVersion != targetBrokerRoleVersion;
    }
    return false;
}

bool ConfigVersionManager::isRollback(protocol::RoleType roleType) const {
    if (roleType == ROLE_TYPE_BROKER) {
        return brokerRollback;
    }
    return false;
}

void ConfigVersionManager::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("broker_current", currentBrokerConfigPath, currentBrokerConfigPath);
    json.Jsonize("broker_target", targetBrokerConfigPath, targetBrokerConfigPath);
    json.Jsonize("admin_current", currentAdminConfigPath, currentAdminConfigPath);
    json.Jsonize("broker_current_role_version", currentBrokerRoleVersion, currentBrokerRoleVersion);
    json.Jsonize("broker_target_role_version", targetBrokerRoleVersion, targetBrokerRoleVersion);
    json.Jsonize("rollback", brokerRollback, brokerRollback);
}

bool ConfigVersionManager::upgradeTargetRoleVersion(protocol::RoleType roleType) {
    uint32_t roleVersion;
    if (roleType == ROLE_TYPE_BROKER) {
        if (!StringUtil::fromString(targetBrokerRoleVersion, roleVersion)) {
            AUTIL_LOG(WARN, "parse target role version[%s] failed", targetBrokerRoleVersion.c_str());
            return false;
        }
        roleVersion++;
        targetBrokerRoleVersion = StringUtil::toString(roleVersion);
    }
    return true;
}

const std::string &ConfigVersionManager::getRoleCurrentVersion(protocol::RoleType roleType) const {
    if (roleType == ROLE_TYPE_BROKER) {
        return currentBrokerRoleVersion;
    } else {
        return EmptyString;
    }
}

const std::string &ConfigVersionManager::getRoleTargetVersion(protocol::RoleType roleType) const {
    if (roleType == ROLE_TYPE_BROKER) {
        return targetBrokerRoleVersion;
    } else {
        return EmptyString;
    }
}

const std::string &ConfigVersionManager::getRoleCurrentPath(protocol::RoleType roleType) const {
    if (roleType == ROLE_TYPE_ADMIN) {
        return currentAdminConfigPath;
    } else if (roleType == ROLE_TYPE_BROKER) {
        return currentBrokerConfigPath;
    } else {
        return EmptyString;
    }
}

const std::string &ConfigVersionManager::getRoleTargetPath(protocol::RoleType roleType) const {
    if (roleType == ROLE_TYPE_BROKER) {
        return targetBrokerConfigPath;
    } else {
        return EmptyString;
    }
}

bool ConfigVersionManager::needUpgradeRoleVersion(protocol::RoleType roleType,
                                                  const std::string &updateVersionPath) const {
    const auto &currentConfigPath = getRoleCurrentPath(roleType);
    if (hasDiffInHippoFile(currentConfigPath, updateVersionPath)) {
        return true;
    }
    string curFileName = FileSystem::joinFilePath(currentConfigPath, SWIFT_CONF_FILE_NAME);
    string tarFileName = FileSystem::joinFilePath(updateVersionPath, SWIFT_CONF_FILE_NAME);
    map<string, set<string>> whiteListMap;
    string DEFAULT_COMMON_SECTION_WHITE_LIST[] = {"broker_count", "admin_count", "group_broker_count"};
    whiteListMap["common"] = set<string>(DEFAULT_COMMON_SECTION_WHITE_LIST, DEFAULT_COMMON_SECTION_WHITE_LIST + 3);
    whiteListMap["admin"] = set<string>();
    whiteListMap["broker"] = set<string>();
    ConfigReader currentReader;
    ConfigReader targetReader;
    if (!currentReader.read(curFileName)) {
        AUTIL_LOG(WARN, "read swift config failed, path[%s]", curFileName.c_str());
        return true;
    }
    if (!targetReader.read(tarFileName)) {
        AUTIL_LOG(WARN, "read swift config failed, path[%s]", tarFileName.c_str());
        return true;
    }
    updateWhiteList(targetReader, whiteListMap);
    map<string, set<string>>::iterator iter;
    for (iter = whiteListMap.begin(); iter != whiteListMap.end(); iter++) {
        string key = iter->first;
        bool curHas = currentReader.hasSection(key);
        bool tarHas = targetReader.hasSection(key);
        set<string> confList = iter->second;
        if (curHas && tarHas) {
            Key2ValMap curMap = currentReader.items(key);
            Key2ValMap tarMap = targetReader.items(key);
            if (hasDiff(curMap, tarMap, confList)) {
                return true;
            }
        } else if (curHas) {
            Key2ValMap curMap = currentReader.items(key);
            if (hasDiff(curMap, confList)) {
                return true;
            }
        } else if (tarHas) {
            Key2ValMap tarMap = targetReader.items(key);
            if (hasDiff(tarMap, confList)) {
                return true;
            }
        }
    }
    return false;
}

bool ConfigVersionManager::hasDiff(const Key2ValMap &tarMap, const set<string> &whiteSet) const {
    Key2ValMap::const_iterator iter = tarMap.begin();
    for (; iter != tarMap.end(); iter++) {
        string key = iter->first;
        if (whiteSet.count(key) == 0) {
            return true;
        }
    }
    return false;
}

bool ConfigVersionManager::hasDiff(const Key2ValMap &curMap,
                                   const Key2ValMap &tarMap,
                                   const set<string> &whiteSet) const {
    Key2ValMap::const_iterator curIter = curMap.begin();
    Key2ValMap::const_iterator tarIter = tarMap.begin();
    while (curIter != curMap.end() && tarIter != tarMap.end()) {
        if (curIter->first == tarIter->first) {
            if (curIter->second == tarIter->second) {
                curIter++;
                tarIter++;
            } else {
                if (whiteSet.count(curIter->first) == 0) {
                    return true;
                } else {
                    curIter++;
                    tarIter++;
                }
            }
        } else if (curIter->first > tarIter->first) {
            if (whiteSet.count(tarIter->first) == 0) {
                return true;
            } else {
                tarIter++;
            }
        } else {
            if (whiteSet.count(curIter->first) == 0) {
                return true;
            } else {
                curIter++;
            }
        }
    }
    if (curIter == curMap.end() && tarIter == tarMap.end()) {
        return false;
    } else if (curIter != curMap.end()) {
        for (; curIter != curMap.end(); curIter++) {
            if (whiteSet.count(curIter->first) == 0) {
                return true;
            }
        }
        return false;
    } else if (tarIter != tarMap.end()) {
        for (; tarIter != tarMap.end(); tarIter++) {
            if (whiteSet.count(tarIter->first) == 0) {
                return true;
            }
        }
        return false;
    }
    return false;
}

void ConfigVersionManager::updateWhiteList(ConfigReader &configReader,
                                           std::map<std::string, std::set<std::string>> &sectionWhiteListMap) const {
    if (!configReader.hasSection(config::SECTION_WHITE_LIST)) {
        return;
    }
    Key2ValMap kvMap = configReader.items(config::SECTION_WHITE_LIST);
    Key2ValMap::iterator iter;
    for (iter = kvMap.begin(); iter != kvMap.end(); iter++) {
        vector<string> nameVec = autil::StringUtil::split(iter->second, ";");
        if (sectionWhiteListMap.find(iter->first) != sectionWhiteListMap.end()) {
            sectionWhiteListMap[iter->first].insert(nameVec.begin(), nameVec.end());
        } else {
            set<string> nameSet(nameVec.begin(), nameVec.end());
            sectionWhiteListMap[iter->first] = nameSet;
        }
    }
}

bool ConfigVersionManager::hasDiffInHippoFile(const std::string currentConfigPath,
                                              const std::string &updateVersionPath) const {
    string curStr;
    string curFileName = FileSystem::joinFilePath(currentConfigPath, HIPPO_FILE_NAME);
    if (fslib::EC_OK != FileSystem::readFile(curFileName, curStr)) {
        AUTIL_LOG(WARN, "Read file [%s] failed!", curFileName.c_str());
        return true;
    }
    string tarStr;
    string tarFileName = FileSystem::joinFilePath(updateVersionPath, HIPPO_FILE_NAME);
    if (fslib::EC_OK != FileSystem::readFile(tarFileName, tarStr)) {
        AUTIL_LOG(WARN, "Read file [%s] failed!", tarFileName.c_str());
        return true;
    }
    JsonMap curJsonMap;
    JsonMap tarJsonMap;
    try {
        FromJsonString(curJsonMap, curStr);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "from json failed, content[%s], exception[%s]", curStr.c_str(), e.what());
        return true;
    }
    try {
        FromJsonString(tarJsonMap, tarStr);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "from json failed, content[%s], exception[%s]", tarStr.c_str(), e.what());
        return true;
    }
    return !autil::legacy::json::Equal(curJsonMap, tarJsonMap);
}

} // namespace config
} // namespace swift
