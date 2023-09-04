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
#include "swift/util/ZkDataAccessor.h"

#include <sstream>
#include <unistd.h>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/TargetRecorder.h"

using namespace swift::protocol;
using namespace swift::common;
using namespace swift::util;

using namespace fslib::util;
using namespace http_arpc;
using namespace std;
using namespace autil;

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, ZkDataAccessor);

ZkDataAccessor::ZkDataAccessor() {}

ZkDataAccessor::~ZkDataAccessor() {
    if (_ownZkWrapper) {
        DELETE_AND_SET_NULL(_zkWrapper);
    }
}

bool ZkDataAccessor::init(cm_basic::ZkWrapper *zkWrapper, const string &sysRoot) {
    int64_t maxWaitZkOpen = 3 * 1000 * 1000;
    int64_t leftTime = maxWaitZkOpen;
    if (!zkWrapper->isConnected()) {
        if (!zkWrapper->open()) {
            AUTIL_LOG(ERROR, "connect to zookeeper failed[%s]", sysRoot.c_str());
            return false;
        }
    }
    while (!zkWrapper->isConnected() && leftTime >= 0) {
        usleep(100 * 1000);
        leftTime -= 100 * 1000;
    }
    _zkWrapper = zkWrapper;
    return leftTime >= 0;
}

bool ZkDataAccessor::init(const string &sysRoot) {
    string zkHost = FileUtil::getHostFromZkPath(sysRoot);
    if (zkHost.empty()) {
        AUTIL_LOG(ERROR, "invalid zkRoot:[%s]", sysRoot.c_str());
        return false;
    }
    _zkWrapper = new cm_basic::ZkWrapper(zkHost);
    _ownZkWrapper = true;
    if (!_zkWrapper->open()) {
        AUTIL_LOG(ERROR, "open zkRoot:[%s] failed", sysRoot.c_str());
        return false;
    }
    int64_t maxWaitZkOpen = 3 * 1000 * 1000;
    int64_t leftTime = maxWaitZkOpen;
    while (!_zkWrapper->isConnected() && leftTime >= 0) {
        usleep(100 * 1000);
        leftTime -= 100 * 1000;
    }
    return leftTime >= 0;
}

bool ZkDataAccessor::remove(const string &path) {
    bool exist = false;
    if (!_zkWrapper->check(path, exist)) {
        AUTIL_LOG(ERROR, "fail to get data %s", path.c_str());
        return false;
    }
    if (!exist) {
        AUTIL_LOG(INFO, "remove path %s not exist", path.c_str());
        return true;
    }
    return _zkWrapper->remove(path);
}

// for zk wrapper touch delete node
bool ZkDataAccessor::writeFile(const string &path, const string &content) {
    bool isExist = false;
    if (!_zkWrapper->check(path, isExist, false)) {
        return false;
    }
    if (!isExist) {
        return _zkWrapper->createNode(path, content, true);
    }
    return _zkWrapper->set(path, content);
}

bool ZkDataAccessor::createPath(const string &path) {
    bool isExist = false;
    if (!_zkWrapper->check(path, isExist, false)) {
        AUTIL_LOG(ERROR, "check path [%s] exist failed.", path.c_str());
        return false;
    }
    if (!isExist) {
        return _zkWrapper->createPath(path);
    }
    return true;
}

cm_basic::ZkWrapper *ZkDataAccessor::getZkWrapper() { return _zkWrapper; }

bool ZkDataAccessor::getData(const std::string &path, std::string &content) {
    bool exist = false;
    if (!_zkWrapper->check(path, exist)) {
        AUTIL_LOG(ERROR, "check[%s] exist error", path.c_str());
        return false;
    }
    if (exist) {
        ZOO_ERRORS ec = _zkWrapper->getData(path, content);
        if (ZOK != ec) {
            AUTIL_LOG(ERROR, "fail to get data[%s], zk error[%d]", path.c_str(), ec);
            return false;
        }
    }
    return true;
}

} // namespace util
} // namespace swift
