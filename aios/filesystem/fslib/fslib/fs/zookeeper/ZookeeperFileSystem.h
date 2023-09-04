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
#ifndef FSLIB_PLUGIN_ZOOKEEPERFILESYSTEM_H
#define FSLIB_PLUGIN_ZOOKEEPERFILESYSTEM_H

#include <zookeeper/zookeeper.h>

#include "autil/Log.h"
#include "fslib/common.h"
#include "fslib/fs/AbstractFileSystem.h"
#include "fslib/fs/zookeeper/ZookeeperFileLock.h"
#include "fslib/fs/zookeeper/ZookeeperFileLockCreator.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);
using namespace fslib;
using namespace fslib::fs;

struct ZookeeperFsConfig {
    int8_t _retry;
};

class ZookeeperFileSystem : public fslib::fs::AbstractFileSystem {
public:
    static int RECV_TIMEOUT;
    static const int ERROR_INIT_HANDLE_FAIL;
    static const std::string CONFIG_SEPARATOR;
    static const std::string ZOOKEEPER_PREFIX;
    static const std::string KV_SEPARATOR;
    static const std::string KV_PAIR_SEPARATOR;
    static const std::string ZOOFS_RETRY;

public:
    ZookeeperFileSystem();
    ~ZookeeperFileSystem();

public:
    /*override*/ File *openFile(const std::string &fileName, Flag flag);

    /*override*/ MMapFile *mmapFile(
        const std::string &fileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset);

    /*override*/ ErrorCode rename(const std::string &oldPath, const std::string &newPath);

    /*override*/ ErrorCode link(const std::string &oldPath, const std::string &newPath);

    /*override*/ ErrorCode getFileMeta(const std::string &fileName, FileMeta &fileMeta);

    /*override*/ ErrorCode isFile(const std::string &path);

    /*override*/ FileChecksum getFileChecksum(const std::string &fileName);

    /*override*/ ErrorCode mkDir(const std::string &dirName, bool recursive = false);

    /*override*/ ErrorCode listDir(const std::string &dirName, FileList &fileList);

    /*ovverride*/ ErrorCode isDirectory(const std::string &path);

    /*override*/ ErrorCode remove(const std::string &pathName);

    /*override*/ ErrorCode isExist(const std::string &pathName);

    /*override*/ ErrorCode listDir(const std::string &dirName, EntryList &entryList);

    /*override*/ FileReadWriteLock *createFileReadWriteLock(const std::string &fileName) { return NULL; }

    /*override*/ FileLock *createFileLock(const std::string &fileName);

    /*override*/ FileSystemCapability getCapability() const { return (FileSystemCapability)~FSC_DISTINCT_FILE_DIR; }

    static ErrorCode convertErrno(int errNum);

    static ErrorCode internalGetZhandleAndPath(
        const std::string &pathName, ZookeeperFsConfig &config, zhandle_t *&zh, std::string &server, std::string &path);

    static int zooWExistWithRetry(zhandle_t *&zh,
                                  const char *path,
                                  watcher_fn watcher,
                                  void *watcherCtx,
                                  struct Stat *stat,
                                  int8_t retryCnt,
                                  const char *server);
    static int zooExistWithRetry(
        zhandle_t *&zh, const char *path, int watch, struct Stat *stat, int8_t retryCnt, const char *server);
    static int zooCreateWithRetry(zhandle_t *&zh,
                                  const char *path,
                                  const char *value,
                                  int valuelen,
                                  const struct ACL_vector *acl,
                                  int flags,
                                  char *path_buffer,
                                  int path_buffer_len,
                                  int8_t retryCnt,
                                  const char *server);
    static int zooGetChildrenWithRetry(zhandle_t *&zh,
                                       const char *path,
                                       int watch,
                                       struct String_vector *strings,
                                       int8_t retryCnt,
                                       const char *server);
    static int zooGetWithRetry(zhandle_t *&zh,
                               const char *path,
                               int watch,
                               char *buffer,
                               int *buffer_len,
                               struct Stat *stat,
                               int8_t retryCnt,
                               const char *server);
    static int zooSetWithRetry(zhandle_t *&zh,
                               const char *path,
                               const char *buffer,
                               int buflen,
                               int version,
                               int8_t retryCnt,
                               const char *server);
    static int zooDeleteWithRetry(zhandle_t *&zh, const char *path, int version, int8_t retryCnt, const char *server);

private:
    static bool checkReturnResults(struct String_vector *result);
    static void freeStringVector(struct String_vector *v);
    ErrorCode internalCopyNode(zhandle_t *oldZh,
                               const std::string &oldNode,
                               int8_t oldRetryCnt,
                               const std::string &oldServer,
                               zhandle_t *newZh,
                               const std::string &newNode,
                               int8_t newRetryCnt,
                               const std::string &newServer);
    ErrorCode internalRemoveNode(zhandle_t *zh, const std::string &node, int8_t retryCnt, const std::string &server);
    ErrorCode
    internalCreateParentNode(zhandle_t *zh, const std::string &node, int8_t retryCnt, const std::string &server);
    static void reconnect(zhandle_t *&zh, const char *server);
    static bool parsePath(const std::string &pathName, std::string &server, std::string &dstPath);

public:
    static bool eliminateSeqSlash(const char *src, size_t len, std::string &dst, bool keepLastSlash = true);
    static bool parsePath(const std::string &fullPath, std::string &filePath, ZookeeperFsConfig &config);
    static bool parseConfig(const std::string &configUri, ZookeeperFsConfig &config);

private:
    FILE *_logFile;
};
typedef std::unique_ptr<ZookeeperFileSystem> ZookeeperFileSystemPtr;

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);

#endif // FSLIB_PLUGIN_ZOOKEEPERFILESYSTEM_H
