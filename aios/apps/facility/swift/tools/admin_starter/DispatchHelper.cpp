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
#include "swift/tools/admin_starter/DispatchHelper.h"

#include <iostream>
#include <stddef.h>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/config/AdminConfig.h"
#include "swift/config/ConfigVersionManager.h"

using namespace std;
using namespace swift::config;
using namespace fslib::fs;
using namespace autil::legacy;

namespace swift {
namespace tools {

AUTIL_LOG_SETUP(swift, DispatchHelper);

DispatchHelper::DispatchHelper() : _config(NULL) {}

DispatchHelper::~DispatchHelper() { DELETE_AND_SET_NULL(_config); }

bool DispatchHelper::init(AdminConfig *config) {
    _config = config;
    return true;
}

bool DispatchHelper::writeVersionFile() {
    string versionFile = FileSystem::joinFilePath(_config->getZkRoot(), "config/version");
    if (fslib::EC_TRUE == FileSystem::isExist(versionFile)) {
        if (fslib::EC_OK != FileSystem::remove(versionFile)) {
            AUTIL_LOG(ERROR, "remove dir for version path [%s] failed", versionFile.c_str());
            return false;
        }
    }
    ConfigVersionManager versionManager;
    versionManager.currentBrokerConfigPath = _config->getConfigPath();
    versionManager.targetBrokerConfigPath = _config->getConfigPath();
    versionManager.currentAdminConfigPath = _config->getConfigPath();
    versionManager.brokerRollback = false;

    string versionStr = ToJsonString(versionManager);

    if (fslib::EC_OK != FileSystem::writeFile(versionFile, versionStr)) {
        AUTIL_LOG(ERROR, "write version file [%s] failed, content [%s]", versionFile.c_str(), versionStr.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "write version file success, content [%s] ", versionStr.c_str());
    return true;
}

bool DispatchHelper::updateAdminVersion() {
    string versionFile = FileSystem::joinFilePath(_config->getZkRoot(), "config/version");
    string versionStr;
    if (FileSystem::readFile(versionFile, versionStr) != fslib::EC_OK) {
        return false;
    }
    ConfigVersionManager versionManager;
    try {
        FromJsonString(versionManager, versionStr);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR,
                  "Invalid json file[%s], content[%s], exception[%s]",
                  versionFile.c_str(),
                  versionStr.c_str(),
                  e.what());
        return false;
    }
    versionManager.currentAdminConfigPath = _config->getConfigPath();
    versionStr = autil::legacy::ToJsonString(versionManager);
    if (fslib::EC_OK != FileSystem::writeFile(versionFile, versionStr)) {
        AUTIL_LOG(ERROR, "write version file [%s] failed, content [%s]", versionFile.c_str(), versionStr.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "update admin version success, content [%s] ", versionStr.c_str());
    return true;
}

} // namespace tools
} // namespace swift
