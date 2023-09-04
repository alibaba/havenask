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
#include "fslib/fs/zookeeper/ZookeeperFileLockCreator.h"

#include "fslib/fs/zookeeper/ZookeeperFileLock.h"
#include "fslib/fs/zookeeper/ZookeeperFileSystem.h"

using namespace std;

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);

FileReadWriteLock *ZookeeperFileLockCreator::createFileReadWriteLock(const string &fileName) { return NULL; }

FileLock *ZookeeperFileLockCreator::createFileLock(const string &fileName) {
    string server;
    string path;
    zhandle_t *zh = NULL;
    ZookeeperFsConfig fsConfig;
    ErrorCode ec = ZookeeperFileSystem::internalGetZhandleAndPath(fileName, fsConfig, zh, server, path);
    if (ec != EC_OK) {
        return NULL;
    }

    return new ZookeeperFileLock(zh, path, fsConfig._retry, server.c_str());
}

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);
