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
#ifndef FSLIB_PLUGIN_ZOOKEEPERFILELOCK_H
#define FSLIB_PLUGIN_ZOOKEEPERFILELOCK_H

#include <zookeeper/zookeeper.h>

#include "fslib/common.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);

class ZookeeperFileLock : public FileLock {
public:
    static int MAX_PATH_LENGTH;
    static std::string FILE_LOCK_NAME;

public:
    friend class ZookeeperFileLockCreator;

public:
    ~ZookeeperFileLock();

private:
    ZookeeperFileLock(zhandle_t *zh, const std::string &fileName, int8_t retryCnt, const std::string &server);
    ZookeeperFileLock(const ZookeeperFileLock &);
    ZookeeperFileLock &operator=(const ZookeeperFileLock &);
    bool createNode();
    bool deleteNode();

public:
    /*override*/ int lock(uint32_t timeout = 0);
    /*override*/ int unlock();

private:
    static void watcherFunc(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx);
    static int vstrcmp(const void *str1, const void *str2);
    static void freeStringVector(struct String_vector *v);
    bool tryLock(std::string &lowest);

private:
    autil::ThreadCond _cond;
    zhandle_t *_zh;
    const std::string _server;
    std::string _parentPath;
    std::string _ownPath;
    int8_t _retryCnt;
    bool _flag;
};

typedef std::unique_ptr<ZookeeperFileLock> ZookeeperFileLockPtr;

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);

#endif // FSLIB_PLUGIN_ZOOKEEPERFILELOCK_H
