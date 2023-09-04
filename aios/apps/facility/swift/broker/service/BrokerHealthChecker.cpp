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
#include "swift/broker/service/BrokerHealthChecker.h"

#include <iosfwd>
#include <string.h>

#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/common/PathDefine.h"

using namespace std;
using namespace fslib::fs;
using namespace autil;
using namespace swift::common;

namespace swift {
namespace service {
AUTIL_LOG_SETUP(swift, BrokerHealthChecker);

BrokerHealthChecker::BrokerHealthChecker() {}

BrokerHealthChecker::~BrokerHealthChecker() {}

bool BrokerHealthChecker::checkAllStatus(const string &content) {

    return checkDeleteBeforeWrite(_dfsPath, content) && checkWrite(_zkPath, content) &&
           checkWrite(_localPath, content) && checkMemoryAllocate();
}

bool BrokerHealthChecker::init(const string &dfsPath, const string &zkPath, const string &roleName) {
    size_t pos = roleName.rfind("_");
    if (string::npos == pos) {
        AUTIL_LOG(WARN, "parse rolename fail[%s]", roleName.c_str());
        return false;
    }
    string brokerName = roleName.substr(0, pos);
    string version = roleName.substr(pos + 1);
    if (version.empty() || brokerName.empty()) {
        AUTIL_LOG(WARN, "parse rolename fail[%s]", roleName.c_str());
        return false;
    }
    if (!dfsPath.empty()) {
        _dfsPath = PathDefine::getHealthCheckFile(dfsPath, version, brokerName);
    }
    if (!zkPath.empty()) {
        _zkPath = PathDefine::getHealthCheckFile(zkPath, version, brokerName);
    }
    _localPath = PathDefine::getHealthCheckFile("./", version, brokerName);
    return true;
}

bool BrokerHealthChecker::checkWrite(const string &fileName, const string &content) {
    if (fileName.empty()) {
        return true;
    }
    fslib::ErrorCode ec = FileSystem::writeFile(fileName, content);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(WARN, "write[%s] failed, error[%d %s]", fileName.c_str(), ec, FileSystem::getErrorString(ec).c_str());
        return false;
    }
    return true;
}

bool BrokerHealthChecker::checkDeleteBeforeWrite(const string &fileName, const string &content) {
    if (fileName.empty()) {
        return true;
    }
    fslib::ErrorCode ec = FileSystem::isExist(fileName);
    if (fslib::EC_TRUE == ec) {
        ec = FileSystem::remove(fileName);
        if (fslib::EC_OK != ec) {
            AUTIL_LOG(
                WARN, "remove[%s] failed, error[%d %s]", fileName.c_str(), ec, FileSystem::getErrorString(ec).c_str());
            return false;
        }
        return checkWrite(fileName, content);
    } else if (fslib::EC_FALSE == ec) {
        return checkWrite(fileName, content);
    } else {
        AUTIL_LOG(
            WARN, "check exist[%s] failed, error[%d %s]", fileName.c_str(), ec, FileSystem::getErrorString(ec).c_str());
        return false;
    }
}

bool BrokerHealthChecker::checkMemoryAllocate() {
    char *memoryStatus = new char[1024];
    memset(memoryStatus, 0, 1024);
    delete[] memoryStatus;
    return true;
}

} // namespace service
} // namespace swift
