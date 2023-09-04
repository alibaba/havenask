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
#include "common/ZkState.h"
#include "fslib/util/FileUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;


BEGIN_HIPPO_NAMESPACE(common);

HIPPO_LOG_SETUP(common, ZkState);

ZkState::ZkState(cm_basic::ZkWrapper *zkWrapper, const string &basePath) {
    assert(zkWrapper);
    _zkWrapper = zkWrapper;
    _basePath = basePath;
}

ZkState::~ZkState() {
}

bool ZkState::read(const string &fileName, string &content) const {
    string zkNodePath = fslib::util::FileUtil::joinPath(_basePath, fileName);
    ZOO_ERRORS ret = _zkWrapper->getData(zkNodePath, content);
    if (ZOK != ret) {
        HIPPO_LOG(ERROR, "ZkState read data failed with code %d. zkNodePath: %s",
                  ret, zkNodePath.c_str());
        return false;
    }
    return true;
}

bool ZkState::write(const string &fileName, const string &content) {
    string zkNodePath = fslib::util::FileUtil::joinPath(_basePath, fileName);
    bool isExist = false;
    int64_t startTime = 0, endTime = 0, usedTime = 0;
    bool isSucc = _zkWrapper->check(zkNodePath, isExist);
    if (!isSucc) {
        HIPPO_LOG(WARN, "Check zkNode [%s] isExist failed.", zkNodePath.c_str());
        return false;
    }
    if (!isExist) {
        startTime = TimeUtility::currentTime();
        if (!_zkWrapper->createNode(zkNodePath, content, true)) {
            HIPPO_LOG(ERROR, "ZkState create node failed. nodePath: %s",
                      zkNodePath.c_str());
            return false;
        }
        endTime = TimeUtility::currentTime();
        usedTime = endTime - startTime;
        if (usedTime > 1000 * 1000) {
            HIPPO_LOG(WARN, "Touch zkNode [%s] takes too long: [%ld]ms.",
                      zkNodePath.c_str(), usedTime / 1000);
        }
    } else {
        startTime = TimeUtility::currentTime();
        if (!_zkWrapper->set(zkNodePath, content)) {
            HIPPO_LOG(ERROR, "ZkState write data failed. nodePath: %s",
                      zkNodePath.c_str());
            return false;
        }
        endTime = TimeUtility::currentTime();
        usedTime = endTime - startTime;
        if (usedTime > 1000 * 1000) {
            HIPPO_LOG(WARN, "set zkNode [%s] takes too long: [%ld]ms.",
                      zkNodePath.c_str(), usedTime / 1000);
        }
    }
    return true;
}

bool ZkState::check(const std::string &fileName, bool &bExist) {
    string zkNodePath = fslib::util::FileUtil::joinPath(_basePath, fileName);
    bool isSucc = _zkWrapper->check(zkNodePath, bExist);
    if (!isSucc) {
        HIPPO_LOG(WARN, "Check zkNode [%s] isExist failed.", zkNodePath.c_str());
        return false;
    }
    return true;
}

bool ZkState::remove(const std::string &path) {
    bool bExist = false;
    if (!check(path, bExist)) {
        return false;
    }
    if (!bExist) {
        return true;
    }
    string zkNodePath = fslib::util::FileUtil::joinPath(_basePath, path);
    bool isSucc = _zkWrapper->remove(zkNodePath);
    if (!isSucc) {
        HIPPO_LOG(WARN, "remove zkNode [%s] failed.", zkNodePath.c_str());
        return false;
    }
    return true;
}

END_HIPPO_NAMESPACE(common);
