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
#include "swift/tools/config_validator/ConfigValidator.h"

#include <cstddef>
#include <memory>

#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "fslib/fs/FileSystem.h"
#include "swift/admin/AppPlanMaker.h"
#include "swift/config/AdminConfig.h"
#include "swift/config/BrokerConfig.h"
#include "swift/config/BrokerConfigParser.h"
#include "swift/config/ConfigDefine.h"
#include "swift/config/ResourceReader.h"

using namespace std;
using namespace autil;
using namespace swift::config;
using namespace swift::admin;

namespace swift {
namespace tools {

AUTIL_LOG_SETUP(swift, ConfigValidator);

ConfigValidator::ConfigValidator() {}

ConfigValidator::~ConfigValidator() {}

bool ConfigValidator::validate(const string &configPath) {
    if (!validateSwiftConfig(configPath)) {
        return false;
    }

    if (!validateAppPlanMaker(configPath)) {
        return false;
    }
    return true;
}

bool ConfigValidator::validateAppPlanMaker(const string &configPath) {
    ResourceReader reader(configPath);
    AppPlanMaker appPlanMaker;
    if (!reader.getConfigWithJsonPath(HIPPO_FILE_NAME, "", appPlanMaker)) {
        AUTIL_LOG(ERROR, "parse hippo config failed");
        return false;
    }

    if (!appPlanMaker.validate()) {
        AUTIL_LOG(ERROR, "validate hippo config failed");
        return false;
    }

    return true;
}

bool ConfigValidator::validateSwiftConfig(const string &configPath) {
    unique_ptr<AdminConfig> adminConfig(AdminConfig::loadConfig(configPath));
    if (!adminConfig.get()) {
        AUTIL_LOG(WARN, "swift admin config failed!");
        return false;
    }

    string configFile = fslib::fs::FileSystem::joinFilePath(configPath, SWIFT_CONF_FILE_NAME);
    BrokerConfigParser brokerParser;
    BrokerConfigPtr brokerConfig(brokerParser.parse(configFile));
    if (brokerConfig == NULL || !brokerConfig->validate()) {
        AUTIL_LOG(WARN, "swift broker config failed!");
        return false;
    }
    return true;
}
}; // namespace tools
}; // namespace swift
