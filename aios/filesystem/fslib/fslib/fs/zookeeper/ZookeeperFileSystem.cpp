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
#include "fslib/fs/zookeeper/ZookeeperFileSystem.h"

#include <errno.h>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "fslib/fs/zookeeper/ZookeeperFile.h"
#include "fslib/util/SafeBuffer.h"

using namespace std;
using namespace autil;
using namespace fslib::util;

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);
AUTIL_DECLARE_AND_SETUP_LOGGER(zookeeper, ZookeeperFileSystem);

int ZookeeperFileSystem::RECV_TIMEOUT = 6000;
const int ZookeeperFileSystem::ERROR_INIT_HANDLE_FAIL = -200;
const string ZookeeperFileSystem::CONFIG_SEPARATOR("?");
const string ZookeeperFileSystem::ZOOKEEPER_PREFIX("zfs://");
const string ZookeeperFileSystem::KV_SEPARATOR("=");
const string ZookeeperFileSystem::KV_PAIR_SEPARATOR("&");
const string ZookeeperFileSystem::ZOOFS_RETRY("retry");

#define ZOOKEEPER_CLOSE(zkHandle)                                                                                      \
    {                                                                                                                  \
        int ret = zookeeper_close(zkHandle);                                                                           \
        if (ZOK != ret) {                                                                                              \
            AUTIL_LOG(WARN, "close zk handle [%p] failed, ret [%d].", zkHandle, ret);                                  \
        }                                                                                                              \
    }

ZookeeperFileSystem::ZookeeperFileSystem() {
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    string zk_log_str("zookeeper_log");
    string tmp_dir = autil::EnvUtil::getEnv("TEST_TMPDIR");
    if (!tmp_dir.empty()) {
        zk_log_str.insert(0, tmp_dir);
    }
    _logFile = fopen(zk_log_str.c_str(), "a");
    if (_logFile) {
        zoo_set_log_stream(_logFile);
    } else {
        cerr << "open ./zookeeper_log for logging fail, " << strerror(errno) << endl;
    }
}

ZookeeperFileSystem::~ZookeeperFileSystem() {
    if (_logFile) {
        fclose(_logFile);
    }
}

void ZookeeperFileSystem::reconnect(zhandle_t *&zh, const char *server) {
    ZOOKEEPER_CLOSE(zh);
    zh = zookeeper_init(server, NULL, RECV_TIMEOUT, 0, NULL, 0);
    if (!zh) {
        AUTIL_LOG(ERROR, "zoo init fail, %s", zerror(errno));
    }
}

int ZookeeperFileSystem::zooWExistWithRetry(zhandle_t *&zh,
                                            const char *path,
                                            watcher_fn watcher,
                                            void *watcherCtx,
                                            struct Stat *stat,
                                            int8_t retryCnt,
                                            const char *server) {
    int zrc = ZOK;
    int8_t actualRetryCnt = 0;
    for (; actualRetryCnt <= retryCnt; actualRetryCnt++) {
        zrc = zoo_wexists(zh, path, watcher, watcherCtx, stat);
        switch (zrc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            continue;
        case ZINVALIDSTATE:
        case ZSESSIONEXPIRED: {
            reconnect(zh, server);
            if (zh) {
                continue;
            } else {
                return ERROR_INIT_HANDLE_FAIL;
            }
        }
        default:
            return zrc;
        }
    }

    return zrc;
}

int ZookeeperFileSystem::zooExistWithRetry(
    zhandle_t *&zh, const char *path, int watch, struct Stat *stat, int8_t retryCnt, const char *server) {
    int zrc = ZOK;
    int8_t actualRetryCnt = 0;
    for (; actualRetryCnt <= retryCnt; actualRetryCnt++) {
        zrc = zoo_exists(zh, path, 0, stat);
        switch (zrc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            continue;
        case ZINVALIDSTATE:
        case ZSESSIONEXPIRED: {
            reconnect(zh, server);
            if (zh) {
                continue;
            } else {
                return ERROR_INIT_HANDLE_FAIL;
            }
        }
        default:
            return zrc;
        }
    }

    return zrc;
}

int ZookeeperFileSystem::zooCreateWithRetry(zhandle_t *&zh,
                                            const char *path,
                                            const char *value,
                                            int valuelen,
                                            const struct ACL_vector *acl,
                                            int flags,
                                            char *path_buffer,
                                            int path_buffer_len,
                                            int8_t retryCnt,
                                            const char *server) {
    int zrc = ZOK;
    int8_t actualRetryCnt = 0;
    for (; actualRetryCnt <= retryCnt; actualRetryCnt++) {
        zrc = zoo_create(zh, path, value, valuelen, acl, flags, path_buffer, path_buffer_len);
        switch (zrc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            continue;
        case ZINVALIDSTATE:
        case ZSESSIONEXPIRED: {
            reconnect(zh, server);
            if (zh) {
                continue;
            } else {
                return ERROR_INIT_HANDLE_FAIL;
            }
        }
        default:
            return zrc;
        }
    }

    return zrc;
}

int ZookeeperFileSystem::zooGetChildrenWithRetry(
    zhandle_t *&zh, const char *path, int watch, struct String_vector *strings, int8_t retryCnt, const char *server) {
    int zrc = ZOK;
    int8_t actualRetryCnt = 0;
    for (; actualRetryCnt <= retryCnt; actualRetryCnt++) {
        zrc = zoo_get_children(zh, path, watch, strings);
        switch (zrc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            continue;
        case ZINVALIDSTATE:
        case ZSESSIONEXPIRED: {
            reconnect(zh, server);
            if (zh) {
                continue;
            } else {
                return ERROR_INIT_HANDLE_FAIL;
            }
        }
        case ZOK: {
            if (checkReturnResults(strings)) {
                return zrc;
            } else {
                zrc = ZCONNECTIONLOSS;
                continue;
            }
        }
        default: {
            return zrc;
        }
        }
    }

    return zrc;
}

bool ZookeeperFileSystem::checkReturnResults(struct String_vector *result) {
    for (int32_t i = 0; i < result->count; i++) {
        if (result->data[i] == NULL) {
            return false;
        }
    }
    return true;
}

int ZookeeperFileSystem::zooGetWithRetry(zhandle_t *&zh,
                                         const char *path,
                                         int watch,
                                         char *buffer,
                                         int *buffer_len,
                                         struct Stat *stat,
                                         int8_t retryCnt,
                                         const char *server) {
    int zrc = ZOK;
    int8_t actualRetryCnt = 0;
    for (; actualRetryCnt <= retryCnt; actualRetryCnt++) {
        zrc = zoo_get(zh, path, watch, buffer, buffer_len, stat);
        switch (zrc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            continue;
        case ZINVALIDSTATE:
        case ZSESSIONEXPIRED: {
            reconnect(zh, server);
            if (zh) {
                continue;
            } else {
                return ERROR_INIT_HANDLE_FAIL;
            }
        }
        default:
            return zrc;
        }
    }

    return zrc;
}

int ZookeeperFileSystem::zooSetWithRetry(zhandle_t *&zh,
                                         const char *path,
                                         const char *buffer,
                                         int buflen,
                                         int version,
                                         int8_t retryCnt,
                                         const char *server) {
    int zrc = ZOK;
    int8_t actualRetryCnt = 0;
    for (; actualRetryCnt <= retryCnt; actualRetryCnt++) {
        zrc = zoo_set(zh, path, buffer, buflen, version);
        switch (zrc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            continue;
        case ZINVALIDSTATE:
        case ZSESSIONEXPIRED: {
            reconnect(zh, server);
            if (zh) {
                continue;
            } else {
                return ERROR_INIT_HANDLE_FAIL;
            }
        }
        default:
            return zrc;
        }
    }

    return zrc;
}

int ZookeeperFileSystem::zooDeleteWithRetry(
    zhandle_t *&zh, const char *path, int version, int8_t retryCnt, const char *server) {
    int zrc = ZOK;
    int8_t actualRetryCnt = 0;
    for (; actualRetryCnt <= retryCnt; actualRetryCnt++) {
        zrc = zoo_delete(zh, path, version);
        switch (zrc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            continue;
        case ZINVALIDSTATE:
        case ZSESSIONEXPIRED: {
            reconnect(zh, server);
            if (zh) {
                continue;
            } else {
                return ERROR_INIT_HANDLE_FAIL;
            }
        }
        default:
            return zrc;
        }
    }

    return zrc;
}

File *ZookeeperFileSystem::openFile(const std::string &fileName, Flag flag) {
    string server;
    string path;
    zhandle_t *zh = NULL;
    ZookeeperFsConfig fsConfig;
    ErrorCode ec = internalGetZhandleAndPath(fileName, fsConfig, zh, server, path);
    if (ec != EC_OK) {
        return new ZookeeperFile(NULL, server, path, flag, 0, fsConfig._retry, ec);
    }

    struct Stat stat;
    stat.dataLength = 0;
    int zrc = zooExistWithRetry(zh, path.c_str(), 0, &stat, fsConfig._retry, server.c_str());
    flag = (Flag)(flag & FLAG_CMD_MASK);
    switch (flag) {
    case READ:
        break;
    case WRITE:
        if (zrc == ZNONODE) {
            ec = internalCreateParentNode(zh, path, fsConfig._retry, server);
            if (ec != EC_OK) {
                ZOOKEEPER_CLOSE(zh);
                return new ZookeeperFile(NULL, server, path, flag, 0, fsConfig._retry, ec);
            }

            zrc = zooCreateWithRetry(
                zh, path.c_str(), NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0, fsConfig._retry, server.c_str());
        }

        break;
    default:
        AUTIL_LOG(ERROR, "openFile %s fail, flag %d not support", fileName.c_str(), flag);
        ZOOKEEPER_CLOSE(zh);
        return new ZookeeperFile(NULL, server, path, flag, 0, fsConfig._retry, EC_NOTSUP);
    }

    if (zrc != ZOK && zrc != ZNODEEXISTS) {
        AUTIL_LOG(ERROR, "open file %s fail! %s", fileName.c_str(), zerror(zrc));
        ZOOKEEPER_CLOSE(zh);
        return new ZookeeperFile(NULL, server, path, flag, 0, fsConfig._retry, convertErrno(zrc));
    }

    return new ZookeeperFile(zh, server, path, flag, stat.dataLength, fsConfig._retry);
}

MMapFile *ZookeeperFileSystem::mmapFile(
    const std::string &fileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset) {
    return new MMapFile(fileName, -1, NULL, -1, -1, EC_NOTSUP);
}

ErrorCode ZookeeperFileSystem::rename(const std::string &oldPath, const std::string &newPath) {
    string oldServer;
    string oldDstPath;
    zhandle_t *oldZh = NULL;
    ZookeeperFsConfig oldFsConfig;
    ErrorCode ec = internalGetZhandleAndPath(oldPath, oldFsConfig, oldZh, oldServer, oldDstPath);
    if (ec != EC_OK) {
        return ec;
    }
    int8_t oldRetryCnt = oldFsConfig._retry;
    int zrc = zooExistWithRetry(oldZh, oldDstPath.c_str(), 0, NULL, oldRetryCnt, oldServer.c_str());
    if (zrc != ZOK) {
        AUTIL_LOG(ERROR, "rename node %s to node %s fail! %s", oldPath.c_str(), newPath.c_str(), zerror(zrc));
        ZOOKEEPER_CLOSE(oldZh);
        return convertErrno(zrc);
    }

    string newServer;
    string newDstPath;
    zhandle_t *newZh = NULL;
    ZookeeperFsConfig newFsConfig;
    ec = internalGetZhandleAndPath(newPath, newFsConfig, newZh, newServer, newDstPath);
    int8_t newRetryCnt = newFsConfig._retry;
    if (ec != EC_OK) {
        return ec;
    }

    if (oldServer == newServer && oldDstPath == newDstPath) {
        return EC_OK;
    }
    zrc = zooExistWithRetry(newZh, newDstPath.c_str(), 0, NULL, newRetryCnt, newServer.c_str());
    if (zrc == ZOK) {
        AUTIL_LOG(ERROR, "rename %s to %s fail, newfile already exist", oldPath.c_str(), newPath.c_str());
        ZOOKEEPER_CLOSE(oldZh);
        ZOOKEEPER_CLOSE(newZh);
        return EC_EXIST;
    }

    ErrorCode ret = internalCreateParentNode(newZh, newDstPath, newRetryCnt, newServer);
    if (ret != EC_OK) {
        ZOOKEEPER_CLOSE(oldZh);
        ZOOKEEPER_CLOSE(newZh);
        return ret;
    }

    ret = internalCopyNode(oldZh, oldDstPath, oldRetryCnt, oldServer, newZh, newDstPath, newRetryCnt, newServer);
    if (ret != EC_OK) {
        ZOOKEEPER_CLOSE(oldZh);
        ZOOKEEPER_CLOSE(newZh);
        return ret;
    }

    ret = internalRemoveNode(oldZh, oldDstPath, oldRetryCnt, oldServer);
    if (ret == EC_NOENT) {
        ret = EC_OK;
    }

    ZOOKEEPER_CLOSE(oldZh);
    ZOOKEEPER_CLOSE(newZh);
    return ret;
}

ErrorCode ZookeeperFileSystem::link(const std::string &oldPath, const std::string &newPath) { return EC_NOTSUP; }

ErrorCode ZookeeperFileSystem::getFileMeta(const std::string &fileName, FileMeta &fileMeta) {
    string server;
    string path;
    zhandle_t *zh = NULL;
    ZookeeperFsConfig fsConfig;
    ErrorCode ret = internalGetZhandleAndPath(fileName, fsConfig, zh, server, path);
    if (ret != EC_OK) {
        return ret;
    }

    struct Stat stat;
    int zrc = zooExistWithRetry(zh, path.c_str(), 0, &stat, fsConfig._retry, server.c_str());
    ZOOKEEPER_CLOSE(zh);

    if (zrc != ZOK) {
        AUTIL_LOG(ERROR, "get file %s meta fail, %s", fileName.c_str(), zerror(zrc));
        return convertErrno(zrc);
    }

    fileMeta.fileLength = stat.dataLength;
    fileMeta.createTime = stat.ctime;
    fileMeta.lastModifyTime = stat.mtime;
    return EC_OK;
}

ErrorCode ZookeeperFileSystem::isFile(const std::string &path) {
    ErrorCode ret = isExist(path);
    if (ret == EC_TRUE) {
        return EC_TRUE;
    } else if (ret == EC_FALSE) {
        return EC_NOENT;
    } else {
        return ret;
    }
    return EC_OK;
}

FileChecksum ZookeeperFileSystem::getFileChecksum(const std::string &fileName) { return 0; }

ErrorCode ZookeeperFileSystem::mkDir(const std::string &dirName, bool recursive) {
    string server;
    string path;
    zhandle_t *zh = NULL;
    ZookeeperFsConfig fsConfig;
    ErrorCode ret = internalGetZhandleAndPath(dirName, fsConfig, zh, server, path);
    if (ret != EC_OK) {
        return ret;
    }

    size_t size = path.size();
    if (path[size - 1] == '/') {
        if (size == 1) {
            AUTIL_LOG(ERROR, "create node %s fail! dir / already exist", dirName.c_str());
            ZOOKEEPER_CLOSE(zh);
            return EC_EXIST;
        } else {
            path.resize(size - 1);
        }
    }

    if (recursive) {
        ret = internalCreateParentNode(zh, path, fsConfig._retry, server);
        if (ret != EC_OK) {
            ZOOKEEPER_CLOSE(zh);
            return ret;
        }
    }

    int zrc = zooCreateWithRetry(
        zh, path.c_str(), NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0, fsConfig._retry, server.c_str());
    if (zrc != ZOK) {
        AUTIL_LOG(ERROR, "create node %s fail! %s", dirName.c_str(), zerror(zrc));
        ZOOKEEPER_CLOSE(zh);
        return convertErrno(zrc);
    }

    ZOOKEEPER_CLOSE(zh);
    return EC_OK;
}

ErrorCode ZookeeperFileSystem::listDir(const std::string &dirName, FileList &fileList) {
    string server;
    string path;
    zhandle_t *zh = NULL;
    ZookeeperFsConfig fsConfig;
    ErrorCode ret = internalGetZhandleAndPath(dirName, fsConfig, zh, server, path);
    if (ret != EC_OK) {
        return ret;
    }

    struct String_vector strings;
    int zrc = zooGetChildrenWithRetry(zh, path.c_str(), 0, &strings, fsConfig._retry, server.c_str());
    ZOOKEEPER_CLOSE(zh);
    if (zrc == EC_OK) {
        for (int i = 0; i < strings.count; i++) {
            fileList.push_back(strings.data[i]);
        }
        freeStringVector(&strings);
        return EC_OK;
    }

    AUTIL_LOG(ERROR, "list dir of %s  fail, %s", dirName.c_str(), zerror(zrc));
    return convertErrno(zrc);
}

ErrorCode ZookeeperFileSystem::listDir(const std::string &dirName, EntryList &entryList) {
    FileList fileList;
    auto ec = listDir(dirName, fileList);
    std::for_each(fileList.begin(), fileList.end(), [&entryList](const string &file) {
        entryList.push_back({true, file});
    });
    return ec;
}

ErrorCode ZookeeperFileSystem::isDirectory(const std::string &path) {
    ErrorCode ret = isExist(path);
    if (ret == EC_TRUE) {
        return EC_TRUE;
    } else if (ret == EC_FALSE) {
        return EC_NOENT;
    } else {
        return ret;
    }
    return EC_OK;
}

ErrorCode ZookeeperFileSystem::isExist(const std::string &pathName) {
    zhandle_t *zh = NULL;
    string server;
    string path;
    ZookeeperFsConfig fsConfig;
    ErrorCode ret = internalGetZhandleAndPath(pathName, fsConfig, zh, server, path);
    if (ret != EC_OK) {
        return ret;
    }

    int zrc = zooExistWithRetry(zh, path.c_str(), 0, NULL, fsConfig._retry, server.c_str());
    ZOOKEEPER_CLOSE(zh);
    if (zrc == ZOK) {
        return EC_TRUE;
    } else if (zrc == ZNONODE) {
        return EC_FALSE;
    }

    AUTIL_LOG(ERROR, "zoo exists fail, %s", zerror(zrc));
    return convertErrno(zrc);
}

ErrorCode ZookeeperFileSystem::convertErrno(int errNum) {
    switch (errNum) {
    case ZOK:
        return EC_OK;
    case ZCONNECTIONLOSS:
        return EC_CONNECTIONLOSS;
    case ZNONODE:
        return EC_NOENT;
    case ZNODEEXISTS:
        return EC_EXIST;
    case ZBADARGUMENTS:
        return EC_BADARGS;
    case ZOPERATIONTIMEOUT:
        return EC_OPERATIONTIMEOUT;
    case ZINVALIDSTATE:
        return EC_INVALIDSTATE;
    case 2:
        return EC_BADARGS;
    case ERROR_INIT_HANDLE_FAIL:
        return EC_INIT_ZOOKEEPER_FAIL;
    case ZSESSIONEXPIRED:
        return EC_SESSIONEXPIRED;
    default:
        AUTIL_LOG(ERROR, "unknown errNum: %d", errNum);
        return EC_UNKNOWN;
    }
}

ErrorCode ZookeeperFileSystem::internalCopyNode(zhandle_t *oldZh,
                                                const string &oldNode,
                                                int8_t oldRetryCnt,
                                                const string &oldServer,
                                                zhandle_t *newZh,
                                                const string &newNode,
                                                int8_t newRetryCnt,
                                                const string &newServer) {
    string oldNodeStr(oldNode);
    string newNodeStr(newNode);
    struct Stat stat;
    int zrc = zooExistWithRetry(oldZh, oldNode.c_str(), 0, &stat, oldRetryCnt, oldServer.c_str());
    if (zrc != ZOK) {
        AUTIL_LOG(ERROR, "internal rename node %s to node %s fail! %s", oldNode.c_str(), newNode.c_str(), zerror(zrc));
        return convertErrno(zrc);
    }

    int bufferLen = stat.dataLength;
    SafeBuffer contentBuf(bufferLen);
    zrc = zooGetWithRetry(
        oldZh, oldNode.c_str(), 0, contentBuf.getBuffer(), &bufferLen, NULL, oldRetryCnt, oldServer.c_str());
    if (zrc != ZOK) {
        AUTIL_LOG(ERROR, "internal rename node %s to node %s fail! %s", oldNode.c_str(), newNode.c_str(), zerror(zrc));
        return convertErrno(zrc);
    }

    zrc = zooExistWithRetry(newZh, newNode.c_str(), 0, NULL, newRetryCnt, newServer.c_str());
    if (zrc == ZOK) {
        struct String_vector strings;
        zrc = zooGetChildrenWithRetry(newZh, newNode.c_str(), 0, &strings, newRetryCnt, newServer.c_str());
        if (zrc != ZOK) {
            AUTIL_LOG(
                ERROR, "internal rename node %s to node %s fail! %s", oldNode.c_str(), newNode.c_str(), zerror(zrc));
            return convertErrno(zrc);
        }

        if (strings.count != 0) {
            AUTIL_LOG(ERROR,
                      "internal rename node %s to node %s fail! which has "
                      "child nodes",
                      oldNode.c_str(),
                      newNode.c_str());
            freeStringVector(&strings);
            return EC_NOTEMPTY;
        }

        freeStringVector(&strings);
        zrc = zooSetWithRetry(
            newZh, newNode.c_str(), contentBuf.getBuffer(), bufferLen, -1, newRetryCnt, newServer.c_str());
        if (zrc != ZOK) {
            AUTIL_LOG(
                ERROR, "internal rename node %s to node %s fail! %s", oldNode.c_str(), newNode.c_str(), zerror(zrc));
            return convertErrno(zrc);
        }
    } else if (zrc == ZNONODE) {
        zrc = zooCreateWithRetry(newZh,
                                 newNode.c_str(),
                                 contentBuf.getBuffer(),
                                 bufferLen,
                                 &ZOO_OPEN_ACL_UNSAFE,
                                 0,
                                 NULL,
                                 0,
                                 newRetryCnt,
                                 newServer.c_str());
        if (zrc != ZOK && zrc != ZNODEEXISTS) {
            AUTIL_LOG(
                ERROR, "internal rename node %s to node %s fail! %s", oldNode.c_str(), newNode.c_str(), zerror(zrc));
            return convertErrno(zrc);
        }
    } else {
        AUTIL_LOG(ERROR, "internal rename node %s to node %s fail! %s", oldNode.c_str(), newNode.c_str(), zerror(zrc));
        return convertErrno(zrc);
    }

    struct String_vector strings;
    zrc = zooGetChildrenWithRetry(oldZh, oldNode.c_str(), 0, &strings, oldRetryCnt, oldServer.c_str());
    if (zrc != ZOK) {
        AUTIL_LOG(ERROR, "internal rename node %s to node %s fail! %s", oldNode.c_str(), newNode.c_str(), zerror(zrc));
        return convertErrno(zrc);
    }

    for (int i = 0; i < strings.count; i++) {
        string oldName = oldNode + "/" + string(strings.data[i]);
        string newName = newNode + "/" + string(strings.data[i]);

        ErrorCode ret = internalCopyNode(
            oldZh, oldName, oldRetryCnt, oldServer.c_str(), newZh, newName, newRetryCnt, newServer.c_str());
        if (ret != EC_OK) {
            freeStringVector(&strings);
            return ret;
        }
    }

    freeStringVector(&strings);
    return EC_OK;
}

void ZookeeperFileSystem::freeStringVector(struct String_vector *v) {
    if (v->data) {
        for (int32_t i = 0; i < v->count; i++) {
            free(v->data[i]);
        }
        free(v->data);
        v->data = NULL;
    }
}

ErrorCode ZookeeperFileSystem::remove(const std::string &pathName) {
    string server;
    string path;
    zhandle_t *zh = NULL;
    ZookeeperFsConfig fsConfig;
    ErrorCode ret = internalGetZhandleAndPath(pathName, fsConfig, zh, server, path);
    if (ret != EC_OK) {
        return ret;
    }

    struct String_vector strings;
    int zrc = zooGetChildrenWithRetry(zh, path.c_str(), 0, &strings, fsConfig._retry, server.c_str());
    if (zrc != ZOK) {
        AUTIL_LOG(ERROR, "get children of node %s fail! %s", path.c_str(), zerror(zrc));
        ZOOKEEPER_CLOSE(zh);
        return convertErrno(zrc);
    }

    for (int i = 0; i < strings.count; i++) {
        string childName = path + "/" + string(strings.data[i]);
        ret = internalRemoveNode(zh, childName, fsConfig._retry, server.c_str());
        if (ret != EC_OK) {
            freeStringVector(&strings);
            ZOOKEEPER_CLOSE(zh);
            return ret;
        }
    }

    freeStringVector(&strings);
    zrc = zooDeleteWithRetry(zh, path.c_str(), -1, fsConfig._retry, server.c_str());
    if (zrc != ZOK) {
        AUTIL_LOG(ERROR, "remove node %s fail! %s", path.c_str(), zerror(zrc));
        ZOOKEEPER_CLOSE(zh);
        return convertErrno(zrc);
    }

    ZOOKEEPER_CLOSE(zh);
    return EC_OK;
}

ErrorCode
ZookeeperFileSystem::internalRemoveNode(zhandle_t *zh, const string &node, int8_t retryCnt, const string &server) {
    struct String_vector strings;
    int zrc = zooGetChildrenWithRetry(zh, node.c_str(), 0, &strings, retryCnt, server.c_str());
    if (zrc == ZNONODE) {
        return EC_OK;
    } else if (zrc != ZOK) {
        AUTIL_LOG(ERROR, "internal remove node %s fail! %s", node.c_str(), zerror(zrc));
        return convertErrno(zrc);
    }

    for (int i = 0; i < strings.count; i++) {
        string childName = node + "/" + string(strings.data[i]);
        ErrorCode ret = internalRemoveNode(zh, childName, retryCnt, server.c_str());
        if (ret != EC_OK) {
            freeStringVector(&strings);
            return ret;
        }
    }

    freeStringVector(&strings);
    zrc = zooDeleteWithRetry(zh, node.c_str(), -1, retryCnt, server.c_str());
    if (zrc != ZOK && zrc != ZNONODE) {
        AUTIL_LOG(ERROR, "internal remove node %s fail! %s", node.c_str(), zerror(zrc));
        return convertErrno(zrc);
    }

    return EC_OK;
}

ErrorCode ZookeeperFileSystem::internalCreateParentNode(zhandle_t *zh,
                                                        const string &node,
                                                        int8_t retryCnt,
                                                        const string &server) {
    size_t pos = node.rfind("/");
    if (pos == 0) {
        return EC_OK;
    }

    string parentNode = node.substr(0, pos);
    int zrc = zooExistWithRetry(zh, parentNode.c_str(), 0, NULL, retryCnt, server.c_str());

    if (zrc == ZNONODE) {
        if (pos == 0) {
            return EC_OK;
        } else {
            ErrorCode ret = internalCreateParentNode(zh, parentNode, retryCnt, server);
            if (ret != EC_OK) {
                return ret;
            }

            zrc = zooCreateWithRetry(
                zh, parentNode.c_str(), NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0, retryCnt, server.c_str());
            if (zrc != ZOK && zrc != ZNODEEXISTS) {
                AUTIL_LOG(ERROR, "internal create node %s fail! %s", parentNode.c_str(), zerror(zrc));
                return convertErrno(zrc);
            }
        }
    } else if (zrc != ZOK && zrc != ZNODEEXISTS) {
        AUTIL_LOG(ERROR, "internal create node %s fail! %s", parentNode.c_str(), zerror(zrc));
        return convertErrno(zrc);
    }

    return EC_OK;
}

ErrorCode ZookeeperFileSystem::internalGetZhandleAndPath(
    const string &pathName, ZookeeperFsConfig &config, zhandle_t *&zh, string &server, string &path) {

    size_t prefixPos = pathName.find(ZookeeperFileSystem::ZOOKEEPER_PREFIX, 0);
    if (prefixPos == string::npos) {
        AUTIL_LOG(ERROR,
                  "parse zookeeper config fail for path %s which "
                  "is an invalid zfs path",
                  pathName.c_str());
        return EC_NOTSUP;
    }
    string NonConfigPath;
    if (!parsePath(pathName, NonConfigPath, config)) {
        return EC_PARSEFAIL;
    }
    if (!parsePath(NonConfigPath, server, path)) {
        return EC_PARSEFAIL;
    }
    zh = zookeeper_init(server.c_str(), NULL, RECV_TIMEOUT, 0, NULL, 0);
    if (!zh) {
        AUTIL_LOG(ERROR, "zoo init fail, %s", zerror(errno));
        return EC_BADARGS;
    }
    return EC_OK;
}

bool ZookeeperFileSystem::parsePath(const string &pathName, string &server, string &dstPath) {
    size_t pos = pathName.find("/");
    if (pos == string::npos) {
        AUTIL_LOG(ERROR, "cannot find / between servername and pathname for %s", pathName.c_str());
        return false;
    }

    server = pathName.substr(0, pos);
    dstPath = pathName.substr(pos);
    return true;
}

bool ZookeeperFileSystem::parsePath(const string &fullPath, string &filePath, ZookeeperFsConfig &config) {
    filePath.reserve(fullPath.size());
    filePath.resize(0);
    size_t prefixLen = ZookeeperFileSystem::ZOOKEEPER_PREFIX.size();
    size_t beginPosForFileName = fullPath.find('/', prefixLen);
    if (beginPosForFileName == string::npos) {
        AUTIL_LOG(ERROR,
                  "parse zookeeper config fail for path %s which "
                  "is an invalid path",
                  fullPath.c_str());
        return false;
    }

    string configUri;
    configUri.reserve(beginPosForFileName);
    configUri.append(fullPath.c_str(), beginPosForFileName);
    size_t beginPosForConfig = configUri.find(ZookeeperFileSystem::CONFIG_SEPARATOR.c_str(), prefixLen);
    configUri.resize(0);
    if (beginPosForConfig != string::npos) {
        configUri.append(fullPath.c_str() + beginPosForConfig + 1, beginPosForFileName - beginPosForConfig - 1);
        filePath.append(fullPath.c_str() + prefixLen, beginPosForConfig - prefixLen);
    } else {
        filePath.append(fullPath.c_str() + prefixLen, beginPosForFileName - prefixLen);
    }

    if (!eliminateSeqSlash(
            fullPath.c_str() + beginPosForFileName, fullPath.size() - beginPosForFileName, filePath, false)) {
        AUTIL_LOG(ERROR,
                  "parse zookeeper config fail for path %s which "
                  "is an invalid path",
                  fullPath.c_str());
        return false;
    }
    // parse config
    config._retry = 3;

    return parseConfig(configUri, config);
}

bool ZookeeperFileSystem::eliminateSeqSlash(const char *src, size_t len, string &dst, bool keepLastSlash) {
    int slashNum = 0;
    bool flag = false;
    for (size_t i = 0; i < len; i++) {
        if (src[i] == '/') {
            if (flag == false) {
                flag = true;
                dst.append(1, src[i]);
                slashNum++;
            } else {
                continue;
            }
        } else {
            flag = false;
            dst.append(1, src[i]);
        }
    }

    if (*dst.rbegin() == '/' && slashNum > 1 && !keepLastSlash) {
        dst.resize(dst.size() - 1);
    }

    return slashNum > 0;
}

bool ZookeeperFileSystem::parseConfig(const string &configUri, ZookeeperFsConfig &config) {
    vector<string> confVec = StringUtil::split(configUri, KV_PAIR_SEPARATOR);
    for (uint32_t i = 0; i < confVec.size(); i++) {
        vector<string> subConfVec = StringUtil::split(confVec[i], KV_SEPARATOR);
        if (subConfVec.size() != 2) {
            AUTIL_LOG(ERROR,
                      "parse ZooFsConfig string <%s> fail! "
                      "parse key value pair error",
                      configUri.c_str());
            return false;
        }

        switch (subConfVec[0][0]) {
        case 'r': {
            if (strcmp(subConfVec[0].c_str(), ZOOFS_RETRY.c_str()) == 0) {
                if (!StringUtil::strToInt8(subConfVec[1].c_str(), config._retry)) {
                    AUTIL_LOG(ERROR,
                              "parse ZooFsConfig string <%s> fail "
                              "while parsing retry times, convert string to "
                              "int8_t error",
                              configUri.c_str());
                    return false;
                } else {
                    continue;
                }
            }
            break;
        }
        default:
            break;
        }

        AUTIL_LOG(ERROR,
                  "parse ZooFsConfig string <%s> fail! "
                  "not support config: %s",
                  configUri.c_str(),
                  subConfVec[0].c_str());
        return false;
    }
    return true;
}

FileLock *ZookeeperFileSystem::createFileLock(const std::string &fileName) {
    ZookeeperFileLockCreator creator;
    return creator.createFileLock(fileName);
}

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);
