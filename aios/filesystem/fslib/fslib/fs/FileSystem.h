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
#ifndef FSLIB_FILESYSTEM_H
#define FSLIB_FILESYSTEM_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/MMapFile.h"
#include "fslib/fs/FileLock.h"
#include "fslib/fs/FileLockCreator.h"
#include "fslib/fs/FileReadWriteLock.h"
#include "fslib/fs/ScopedFileLock.h"
#include "fslib/fs/ScopedFileReadWriteLock.h"

FSLIB_BEGIN_NAMESPACE(config);
class FsConfig;
FSLIB_END_NAMESPACE(config);

FSLIB_BEGIN_NAMESPACE(tools);
class FsUtil;
FSLIB_END_NAMESPACE(tools);

FSLIB_BEGIN_NAMESPACE(cache);
class FSCacheModule;
FSLIB_END_NAMESPACE(cache);

FSLIB_BEGIN_NAMESPACE(util);
class MetricTagsHandler;
FSLIB_TYPEDEF_SHARED_PTR(MetricTagsHandler);
FSLIB_END_NAMESPACE(util);

FSLIB_BEGIN_NAMESPACE(fs);

class AbstractFileSystem;

struct PathInfo {
    PathInfo(AbstractFileSystem* fs, const std::string& path)
    {
        _fs = fs;
        _path = path;
    }

    AbstractFileSystem* _fs;
    std::string _path;
};

class FileSystem
{
public:
    friend class tools::FsUtil;
    friend class FileSystemGtest;
    friend class FileSystemMetricTest;
    friend class FileSystemMetricTest_testInitReporter_Test;
public:
    const static int64_t BUFFER_SIZE;
    const static std::string SUFFIX;
    const static int32_t DEFAULT_THREAD_NUM;
    const static int32_t DEFAULT_THREAD_QUEUE_SIZE;

public:
    /**
     * openFile open a file in specific mode
     * @param fileName [in] file to open
     * @param mode [in] READ/WRITE/APPEND
     * @param useDirectIO [in] true for user direct io
     * @param fileLength [in] fileLength hint to to avoid getFileMeta before open, -1 means unknown
     * @return File*
     *
     *
     * fileName example
     * LocalFile: /home/admin/example.txt
     * PanguFile: pangu://localhost:10240?min=1&max=3&app=app&part=part&commit=64&compress=lz77/test
     *            (1. the param 'commit=64' means that the pangu file writer will auto commit when
     *             the writed bytes accumulated to or more than 64M. default is auto commit with 64M
     *             when it is not specified.
     *             2. the param 'compress=lz77' means that the pangu file is writed with compress.
     *             there are 5 options: none, lz77, lz77_entropy, preprocess_lz77, preprocess_lz77_entropy.
     *             the default is none. so far, 'lz77' is the ONLY compress type supported)
     * ZookeeperFile: zfs://localhost:2181?retry=3/test
     * HadoopFile: hdfs://localhost:2181?buf=1024&rep=3&blk=1024&
     *             user=admin&groups=users,supergroup/test
     *
     */
    static File* openFile(const std::string& fileName, Flag mode,
                          bool useDirectIO = false, ssize_t fileLength = -1);

    /**
     * mmapFile mmap local file
     * @param fileName [in] file to mmap
     * @param openMode [in] READ/WRITE/APPEDN
     * @param start [in] start address of mapped memory
     * @param len [in] length of the mapped memory
     * @param prot [in] describes the desired memory protection
     * @param mapFlag [in] specifies the type of the mapped object
     * @param offset [in] offset from the file
     * @param fileLength [in] fileLength hint to to avoid getFileMeta before open, -1 means unknown
     * @return File*
     */
    static MMapFile* mmapFile(const std::string& fileName, Flag openMode, char* start,
                              size_t length, int prot, int mapFlag, off_t offset,
                              ssize_t fileLength = -1);

    /**
     * getFileMeta return the meta info of file
     * @param fileName [in] file name
     * @param fileMeta [out] meta of file
     * @return ErrorCode, EC_OK means success, otherwise means fail.
     */
    static ErrorCode getFileMeta(const std::string& fileName,
                                 FileMeta& fileMeta);

    /**
     * getFileChecksum return the checksum of a file's content
     * @param fileName [in] file name
     * @return FileChecksum, currently it always return 0.
     */
    static FileChecksum getFileChecksum(const std::string& fileName);

    /**
     * isFile check whether the path is a file
     * @param path [in] path name
     * @return ErrorCode, EC_TRUE means path is file, EC_FALSE means
     *         the path is not a file, or return other error code
     */
    static ErrorCode isFile(const std::string& path);

    /**
     * mkDir create a directory
     * @param dirName [in] directory name
     * @param recursive [in] true means recursively create dir while its
     *        parent does not exist.
     * @return ErrorCode, EC_OK means success, otherwise means fail.
     */
    static ErrorCode mkDir(const std::string& dirName, bool recursive = false);

    /**
     * listDir list files and dirs in the specific directory
     * @param dirName [in] dir name
     * @return FileList, a set of string.
     */
    static ErrorCode listDir(const std::string& dirName, FileList& fileList);

    /**
     * listDir list files and dirs in the specific directory
     * @param dirName [in] dir name
     * @return EntryList, a set of file entry, include entry name and whether
     *         this entry is dir
     */
    static ErrorCode listDir(const std::string& dirName, EntryList& entryList);

    /**
     * listDir list files and dirs in the specific directory
     * @param dirName [in] dir name
     * @return RichFileList, a list of RichFileMeta, include name, size, time, isdir
     */
    static ErrorCode listDir(const std::string& dirName, RichFileList& fileList);

    /**
     * listDir recursively list files and dirs in the specific directory
     * @param dirName [in] dir name
     * @return a map of EntryInfo, a set of file entry, include entry name
     * and length of this entry
     */
    static ErrorCode listDir(const std::string& dirName,
                             EntryInfoMap& entryInfoMap,
                             int32_t threadNum = DEFAULT_THREAD_NUM,
                             int32_t threadQueueSize = DEFAULT_THREAD_QUEUE_SIZE);

    /**
     * isDirectory check whether the path is a directory
     * @param path [in] path to check
     * @return ErrorCode, EC_TRUE if the path is directory, EC_FALSE if the path
     *         is not directory, or return other error code
     */
    static ErrorCode isDirectory(const std::string& path);

    /**
     * isLink check whether the path is a Link
     * @param path [in] path to check
     * @return ErrorCode, EC_TRUE if the path is Link, EC_FALSE if the path
     *         is not Link, or return other error code
     */
    static ErrorCode isLink(const std::string& path);

    /**
     * rename rename a file or a directory, can only be used in one FileSystem
     * @param oldPath [in] old file or directory name
     * @param newPath [in] new file or directory name
     * @return ErrorCode, EC_OK means success, otherwise means fail.
     */
    static ErrorCode rename(const std::string& oldPath,
                            const std::string& newPath);

    /**
     * copy copy the file or dir from src to dst, file system of which can be different
     *          from the src
     * @param dstPath [in] dst file or dir name
     * @param srcPath [in] src file or dir name
     * @return ErrorCode, EC_OK means success, otherwise means fail.
     */
    static ErrorCode copy(const std::string& srcPath,
                          const std::string& dstPath);

    /**
     * move move the file or dir from src to dst, file system of which can be different
     *          from the src
     * @param dstPath [in] dst file or dir name
     * @param srcPath [in] src file or dir name
     * @return ErrorCode, EC_OK means success, otherwise means fail.
     */
    static ErrorCode move(const std::string& srcPath,
                          const std::string& dstPath);

    /**
     * remove remove the path
     * @param path [in] path can either be dir or file
     * @return ErrorCode, EC_OK means success, otherwise means fail.
     *         removeFile on a dir, may return EC_ISDIR or EC_OK(compitable)
     *         removeDir on a file, may return EC_NOTDIR or EC_OK(compitable)
     */
    static ErrorCode remove(const std::string& path);
    static ErrorCode removeFile(const std::string& path);
    static ErrorCode removeDir(const std::string& path);

    /**
     * isExist check whether the path exist
     * @param path [in] path can either be dir or file
     * @return ErrorCode, EC_TRUE if the path exist, EC_FALSE if path does
     *         not exist, or return other error code.
     */
    static ErrorCode isExist(const std::string& path);

    /**
     * getErrorString return the detail error description of the error code
     * @param ec [in] error code
     * @return string, error description.
     */
    static std::string getErrorString(ErrorCode ec);

    /**
     * getFileLock return a fileLock which is only for multi processes.
     *             for multi threads please use locks in autil
     * @param fileName [in] fileName of file lock
     * @return FileLock, a corresponding FileLock
     */
    static FileLock* getFileLock(const std::string& fileName);

    /**
     * getFileReadWriteLock return a fileLock which is only for multi processes.
     *             for multi threads please use locks in autil
     * @param fileName [in] fileName of file lock
     * @return FileReadWriteLock, a corresponding FileReadWriteLock
     */
    static FileReadWriteLock* getFileReadWriteLock(const std::string& fileName);

    /**
     * getScopedFileLock return a ScopedFileLock with mode passed
     * @param fileName [in] fileName of file lock
     * @param scopedLock [out] ScopedFileReadWriteLock to use
     * @return bool, true if succeed, otherwise false
     */
    static bool getScopedFileLock(const std::string& fileName, ScopedFileLock& scopedLock);

    /**
     * getScopedFileReadWriteLock return a ScopedFileReadWriteLock with mode passed
     * @param fileName [in] fileName of file lock
     * @param mode [in] mode for lock, support r/R/w/W
     * @param scopedLock [out] ScopedFileReadWriteLock to use
     * @return bool, true if succeed, otherwise false
     */
    static bool getScopedFileReadWriteLock(const std::string& fileName,
            const char mode, ScopedFileReadWriteLock& scopedLock);

    /**
     * getFsType return file system type of path
     * @param path [in] path name
     * @return FsType, file system type of path
     */
//    static FsType getFsType(const std::string& path);
    static ErrorCode parseInternal(const std::string& srcPath,
                                   AbstractFileSystem*& fs);

    /**
     * getPathMeta return the meta info of path
     * @param path [in] path string
     * @param pathMeta [out] meta of path
     * @return ErrorCode, EC_OK means success, otherwise means fail.
     */
    static ErrorCode getPathMeta(const std::string& path,
                                 PathMeta& pathMeta);
    static std::string getPathFromZkPath(const std::string &zkPath);
    static bool isZkLikeFileSystem(const AbstractFileSystem* fileSystem);
    static bool isOssFileSystem(const AbstractFileSystem* fileSystem);
    static ErrorCode copyZKLikeFsToOtherFs(const PathInfo& srcInfo,
            const PathInfo& dstInfo, bool recursive, bool toConsole);
    static FsType getFsType(const std::string &srcPath);

    static ErrorCode readFile(const std::string &filePath, std::string &content);
    static ErrorCode writeFile(const std::string &filePath, const std::string &content);
    static std::string joinFilePath(const std::string &path, const std::string &file);
    static std::string getParentPath(const std::string &path);

    // public for test
    static bool reuseThreadPool();
    static int32_t getThreadNum();
    static ErrorCode forward(const std::string &command, const std::string &path,
                             const std::string &args, std::string &output);
    static void close();

    static fslib::cache::FSCacheModule* getCacheModule();

private:
    static bool generatePath(const std::string& srcPath,
                             const std::string& dstDir,
                             std::string& dstPath);

    static bool appendPath(const std::string& dstDir,
                           const std::string& relativeName,
                           std::string& dstPath);

    static ErrorCode copyFileInternal(const PathInfo& srcInfo,
            const PathInfo& dstInfo);

    static ErrorCode copyAll(const PathInfo& srcInfo, const PathInfo& dstInfo,
                             bool recursive, bool toConsole);

    static ErrorCode copyFile(const PathInfo& srcInfo, const PathInfo& dstInfo,
                              bool toConsole, bool& created);

    static ErrorCode copyDir(const PathInfo& srcInfo, const PathInfo& dstInfo,
                             FileList& fileList, bool toConsole,
                             bool created, bool special);

    static ErrorCode moveInternal(const std::string& srcPath,
                                  const std::string& dstPath,
                                  bool toConsole);

    static ErrorCode renameInternal(const PathInfo& oldInfo,
                                    const PathInfo& newInfo);

    static FileLockCreator* getFileLockCreatorForPath(const std::string& fileName);

    static ErrorCode parseInternal(const std::string& srcPath,
                                   AbstractFileSystem*& fs,
                                   std::string& fsType,
                                   std::string& fsName);
public:
    static void reportErrorMetric(const std::string& filePath,
                                  const std::string& opType,
                                  double value = 1.0);
    static void reportQpsMetric(const std::string& filePath,
                                const std::string& opType,
                                double value = 1.0);
    static void reportLatencyMetric(const std::string& filePath,
                                    const std::string& opType,
                                    int64_t latency);

    static void reportDNErrorQps(const std::string& filePath,
                                 const std::string& opType,
                                 double value = 1.0);
    static void reportDNReadErrorQps(const std::string& filePath,
                                     double value = 1.0);
    static void reportDNReadSpeed(const std::string& filePath,
                                  double speed);
    static void reportDNReadLatency(const std::string& filePath,
                                    int64_t latency);
    // read latency per KB
    static void reportDNReadAvgLatency(const std::string& filePath,
                                       int64_t latency);

    static void reportDNWriteErrorQps(const std::string& filePath,
                                      double value = 1.0);
    static void reportDNWriteSpeed(const std::string& filePath,
                                   double speed);
    static void reportDNWriteLatency(const std::string& filePath,
                                     int64_t latency);
    // write latency per KB
    static void reportDNWriteAvgLatency(const std::string& filePath,
                                        int64_t latency);

    static void reportMetaCachedPathCount(int64_t fileCount);
    static void reportMetaCacheImmutablePathCount(int64_t fileCount);
    static void reportMetaCacheHitQps(const std::string& filePath, double value = 1.0);
    static void reportDataCachedFileCount(int64_t fileCount);
    static void reportDataCacheMemUse(int64_t fileCount);
    static void reportDataCacheHitQps(const std::string& filePath, double value = 1.0);

    static ErrorCode GENERATE_ERROR(const std::string& operate, const std::string& filename);
    static bool _useMock;
    static bool _mmapDontDump;
public:
    static bool setMetricTagsHandler(util::MetricTagsHandlerPtr tagsHandler);
    static util::MetricTagsHandlerPtr getMetricTagsHandler();
};

FSLIB_END_NAMESPACE(fs);

#endif //FSLIB_FILESYSTEM_H
