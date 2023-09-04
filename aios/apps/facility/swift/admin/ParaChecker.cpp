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
#include "swift/admin/ParaChecker.h"

#include <ctype.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace autil;

namespace swift {
namespace admin {

AUTIL_LOG_SETUP(swift, ParaChecker);

ParaChecker::ParaChecker() {}

ParaChecker::~ParaChecker() {}

bool ParaChecker::checkValidTopicName(const std::string &name) {
    if (name.size() <= 0 || name.size() >= 1024) {
        AUTIL_LOG(ERROR, "Topic name [%s] is NULL or is too long.", name.c_str());
        return false;
    }

    if (!isalnum(name[0])) {
        AUTIL_LOG(ERROR, "Topic name [%s] not start with alphabet or digit.", name.c_str());
        return false;
    }

    for (size_t i = 1; i < name.size(); ++i) {
        if (!isalnum(name[i]) && !IsConnector(name[i])) {
            AUTIL_LOG(ERROR, "Topic name [%s] has invalid char [%c].", name.c_str(), name[i]);
            return false;
        }
    }

    return true;
}

bool ParaChecker::checkValidIpAddress(const std::string &ipAddress) {
    std::vector<std::string> strList = StringUtil::split(ipAddress, ".");
    if (strList.size() != 4) {
        return false;
    }
    uint8_t tmp;
    for (size_t i = 0; i < strList.size(); ++i) {
        if (strList[i].size() >= 4) {
            return false;
        }
        if (!StringUtil::strToUInt8(strList[i].c_str(), tmp)) {
            return false;
        }
    }

    return true;
}

bool ParaChecker::IsConnector(const char c) {
    if (c == '.' || c == '-' || c == '_') {
        return true;
    }
    return false;
}

} // namespace admin
} // namespace swift
