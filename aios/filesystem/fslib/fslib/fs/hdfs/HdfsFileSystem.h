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
#ifndef FSLIB_PLUGIN_HDFSFILESYSTEM_H
#define FSLIB_PLUGIN_HDFSFILESYSTEM_H

#include <map>

#include "autil/Lock.h"
#include "fslib/common.h"
#include "fslib/fs/AbstractFileSystem.h"
#include "fslib/fs/hdfs/HdfsConnection.h"
#include "fslib/fs/hdfs/PanguMonitor.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(hdfs);

using namespace fslib::fs;

struct HdfsFsConfig {
    char *_user;
    const char **_groups;
    int32_t _bufferSize;
    int32_t _blockSize;
    int16_t _replica;
    int16_t _groupNum;
    std::string _prefix;

public:
    HdfsFsConfig() {
        _user = NULL;
        _groups = NULL;
        _bufferSize = 0;
        _blockSize = 0;
        _replica = 0;
        _groupNum = 0;
    }
    ~HdfsFsConfig() {
        delete[] _user;
        if (NULL != _groups) {
            for (uint16_t i = 0; i < _groupNum; i++) {
                delete[] _groups[i];
            }
            delete[] _groups;
        }
    }
};

class HdfsFileSystem : public fslib::fs::AbstractFileSystem {
public:
    static const size_t MAX_CONNECTION;
    static const std::string HADOOP_CLASSPATH_ENV;
    static const std::string CLASSPATH_ENV;
    static const std::string HADOOP_HOME_ENV;
    static const std::string HDFS_PREFIX;
    static const std::string DFS_PREFIX;
    static const std::string JFS_PREFIX;

    static const std::string KV_SEPARATOR;
    static const std::string KV_PAIR_SEPARATOR;
    static const std::string CONFIG_SEPARATOR;
    static const std::string HDFS_BUFFER_SIZE;
    static const std::string HDFS_REPLICA;
    static const std::string HDFS_BLOCK_SIZE;
    static const std::string HDFS_USER;
    static const std::string HDFS_GROUPS;
    static const std::string HDFS_GROUP_SEPARATOR;
    static const uint32_t DEFAULT_PANGU_AUTO_COMMIT_LIMIT;

public:
    friend class HdfsFileSystemTest;

public:
    HdfsFileSystem();
    ~HdfsFileSystem();

public:
    /*override*/ File *openFile(const std::string &fileName, Flag mode);

    /*override*/ MMapFile *mmapFile(
        const std::string &fileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset);

    /*override*/ ErrorCode rename(const std::string &oldPath, const std::string &newPath);

    /*override*/ ErrorCode link(const std::string &oldPath, const std::string &newPath);

    /*override*/ ErrorCode getFileMeta(const std::string &fileName, FileMeta &fileMeta);

    /*override*/ ErrorCode getPathMeta(const std::string &path, PathMeta &pathMeta);

    /*override*/ ErrorCode isFile(const std::string &path);

    /*override*/ FileChecksum getFileChecksum(const std::string &fileName);

    /*override*/ ErrorCode mkDir(const std::string &dirName, bool recursive = false);

    /*override*/ ErrorCode listDir(const std::string &dirName, FileList &fileList);

    /*override*/ ErrorCode isDirectory(const std::string &path);

    /*override*/ ErrorCode remove(const std::string &path);

    /*override*/ ErrorCode isExist(const std::string &path);

    /*override*/ ErrorCode listDir(const std::string &dirName, EntryList &entryList);
    /*override*/ ErrorCode listDir(const std::string &dirName, RichFileList &fileList);

    /*override*/ FileReadWriteLock *createFileReadWriteLock(const std::string &fileName) { return NULL; }

    /*override*/ FileLock *createFileLock(const std::string &fileName) { return NULL; }

    /*override*/ FileSystemCapability getCapability() const { return FSC_ALL_CAPABILITY; }

    static ErrorCode convertErrno(int errNum);

public:
    static bool parsePath(const std::string &path, std::string &filePath, HdfsFsConfig &config);
    HdfsConnectionPtr getHdfsConnection(const std::string &name);

private:
    static bool eliminateSeqSlash(const char *src, size_t len, std::string &dst, bool keepLastSlash);
    static bool parseConfig(const std::string &configStr, HdfsFsConfig &config);

    ErrorCode splitServerAndPath(const std::string &srcPath,
                                 const std::string &prefix,
                                 std::string &server,
                                 std::string &dstPath);

    ErrorCode connectServer(const std::string &server, const HdfsFsConfig &fsConfig, HdfsConnectionPtr &hdfsFs);

    ErrorCode internalGetConnectionAndPath(const std::string &srcPath,
                                           HdfsFsConfig &fsConfig,
                                           HdfsConnectionPtr &hdfsConnection,
                                           std::string &dstPath);

    void getConnectUser(const HdfsFsConfig &fsConfig, std::string &username);
    ErrorCode internalIsExist(HdfsConnectionPtr hdfsConnection, const std::string &dstPath);

    void initClassPath();
    bool getHadoopClassPath(std::string &hadoopClassPath);
    void getHadoopHome(std::string &hadoopHome);
    void genHadoopClassPath(const std::string &hadoopHome, std::string &hadoopClassPath);
    void setHadoopClassPath(const std::string &hadoopClassPath);
    static std::string getHadoopClassPathPattern(const std::string &hadoopHome);
    static void extendHadoopClassPathPattern(const std::string &pattern, std::string &hadoopClassPath);
    static void readStream(FILE *stream, std::string *pattern);
    static void globPath(const char *pathPattern, std::vector<std::string> *paths);

private:
    std::map<const std::string, HdfsConnectionPtr> _connectionMap;
    autil::ReadWriteLock _rwLock;
    PanguMonitor _panguMonitor;
};

typedef std::unique_ptr<HdfsFileSystem> HdfsFileSystemPtr;

FSLIB_PLUGIN_END_NAMESPACE(hdfs);

#endif // FSLIB_PLUGIN_HDFSFILESYSTEM_H
