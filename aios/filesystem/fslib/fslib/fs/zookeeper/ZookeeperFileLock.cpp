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
#include "fslib/fs/zookeeper/ZookeeperFileLock.h"

#include <errno.h>

#include "autil/TimeUtility.h"
#include "fslib/fs/zookeeper/ZookeeperFileSystem.h"
#include "fslib/util/LongIntervalLog.h"

using namespace std;
using namespace autil;
FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);
AUTIL_DECLARE_AND_SETUP_LOGGER(zookeeper, ZookeeperFileLock);

int ZookeeperFileLock::MAX_PATH_LENGTH = 2000;
string ZookeeperFileLock::FILE_LOCK_NAME = "/lock";

ZookeeperFileLock::ZookeeperFileLock(zhandle_t *zh, const string &fileName, int8_t retryCnt, const string &server)
    : _zh(zh), _server(server), _parentPath(fileName), _retryCnt(retryCnt) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _parentPath.c_str());
    _flag = true;
    int ret = ZookeeperFileSystem::zooExistWithRetry(zh, fileName.c_str(), 0, NULL, _retryCnt, _server.c_str());
    if (ret == ZNONODE) {
        ret = ZookeeperFileSystem::zooCreateWithRetry(
            zh, fileName.c_str(), NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0, _retryCnt, _server.c_str());
        if (ret != ZOK && ret != ZNODEEXISTS) {
            AUTIL_LOG(ERROR, "creat zookeeper file lock fail %s.", zerror(ret));
            _flag = false;
        }
    } else if (ret != ZOK) {
        AUTIL_LOG(ERROR, "creat zookeeper file lock fail %s.", zerror(ret));
        _flag = false;
    }
}

bool ZookeeperFileLock::createNode() {
    if (!_flag || !_ownPath.empty()) {
        return false;
    }

    char tmpPath[MAX_PATH_LENGTH];
    int ret = ZookeeperFileSystem::zooCreateWithRetry(_zh,
                                                      (_parentPath + FILE_LOCK_NAME).c_str(),
                                                      NULL,
                                                      0,
                                                      &ZOO_OPEN_ACL_UNSAFE,
                                                      3,
                                                      tmpPath,
                                                      MAX_PATH_LENGTH,
                                                      _retryCnt,
                                                      _server.c_str());
    if (ret != ZOK) {
        AUTIL_LOG(ERROR, "creat zookeeper file lock fail %s.", zerror(ret));
        _flag = false;
        return false;
    } else {
        _flag = true;
        _ownPath.assign(tmpPath);
        return true;
    }
}

bool ZookeeperFileLock::deleteNode() {
    if (!_flag || _ownPath.empty()) {
        return false;
    }

    int ret = ZookeeperFileSystem::zooDeleteWithRetry(_zh, _ownPath.c_str(), -1, _retryCnt, _server.c_str());

    if (ret != ZOK) {
        AUTIL_LOG(ERROR, "deleteNode fail. %s", zerror(ret));
    }

    _ownPath.clear();

    return ret == ZOK;
}

ZookeeperFileLock::~ZookeeperFileLock() {
    if (_zh) {
        zookeeper_close(_zh);
    }
}

int ZookeeperFileLock::lock(uint32_t timeout) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _parentPath.c_str());
    if (!createNode()) {
        AUTIL_LOG(ERROR, "lock fail. create node fail");
        return -1;
    }

    string lowest;
    int64_t timeleft = timeout * 1000000;
    int64_t curTime = TimeUtility::currentTime();
    int64_t endTime = timeleft + curTime;
    while (!tryLock(lowest)) {
        if (lowest.empty()) {
            return -1;
        }

        ScopedLock lock(_cond);
        int ret = ZookeeperFileSystem::zooWExistWithRetry(
            _zh, lowest.c_str(), watcherFunc, &_cond, NULL, _retryCnt, _server.c_str());
        if (ret == ZNONODE) {
            // lowest node is already disappeared
            continue;
        } else if (ret == ZOK) {
            if (timeout == 0) {
                _cond.wait();
            } else {
                timeleft = endTime - TimeUtility::currentTime();
                if (timeleft > 0) {
                    _cond.wait(timeleft);
                }
            }
            timeleft = endTime - TimeUtility::currentTime();
            if (timeout != 0 && timeleft <= 0) {
                AUTIL_LOG(ERROR, "lock fail. timeout");
                deleteNode();
                return -1;
            }
        } else {
            AUTIL_LOG(ERROR, "lock fail. set exist watch error [%s]", zerror(ret));
            deleteNode();
            return -1;
        }
    }

    return 0;
}

void ZookeeperFileLock::watcherFunc(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    ThreadCond *cond = (ThreadCond *)watcherCtx;
    ScopedLock lock(*cond);
    cond->signal();
}

bool ZookeeperFileLock::tryLock(string &lowest) {
    struct String_vector strVec;
    strVec.data = NULL;
    strVec.count = 0;

    lowest.clear();

    int ret =
        ZookeeperFileSystem::zooGetChildrenWithRetry(_zh, _parentPath.c_str(), 0, &strVec, _retryCnt, _server.c_str());
    if (ret != ZOK) {
        AUTIL_LOG(ERROR, "try fail. %s", zerror(ret));
        return false;
    }

    if (strVec.count == 0) {
        AUTIL_LOG(ERROR, "try fail. no children node");
        return false;
    }

    qsort(strVec.data, strVec.count, sizeof(char *), &vstrcmp);
    lowest.append(_parentPath);
    lowest.append(1, '/');
    lowest.append(strVec.data[0]);

    freeStringVector(&strVec);
    ret = strcmp(_ownPath.c_str(), lowest.c_str());
    if (ret == 0) {
        return true;
    } else if (ret < 0) {
        // _ownPath is disappeared
        lowest.clear();
        return false;
    } else {
        return false;
    }
}

int ZookeeperFileLock::unlock() {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", _parentPath.c_str());
    if (!_flag) {
        AUTIL_LOG(ERROR, "lock fail. bad file lock");
        return -1;
    }

    bool ret = deleteNode();
    if (ret) {
        return 0;
    } else {
        AUTIL_LOG(ERROR, "unlock fail");
        return -1;
    }
}

int ZookeeperFileLock::vstrcmp(const void *str1, const void *str2) {
    const char **a = (const char **)str1;
    const char **b = (const char **)str2;
    return strcmp(*a, *b);
}

void ZookeeperFileLock::freeStringVector(struct String_vector *v) {
    if (v->data) {
        for (int32_t i = 0; i < v->count; i++) {
            free(v->data[i]);
        }
        free(v->data);
        v->data = NULL;
    }
}

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);
