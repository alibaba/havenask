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

#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/config/ConfigReader.h"

namespace swift {
namespace config {

class ConfigChecker {
public:
    ConfigChecker();
    ~ConfigChecker();

public:
    bool check(const std::string &sysRoot);

private:
    bool checkNecessaryConf(const std::string &confPath);
    bool checkIpList(const std::string &ipListPath);

private:
    ConfigReader _confReader;

private:
    friend class ConfigCheckerTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ConfigChecker);

} // namespace config
} // namespace swift
