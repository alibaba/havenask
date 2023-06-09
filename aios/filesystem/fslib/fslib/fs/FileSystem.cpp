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
#include "fslib/fs/FileSystem.h"

#include "autil/ThreadPool.h"
#include "autil/StringUtil.h"
#include "autil/EnvUtil.h"
#include "kmonitor_adapter/MonitorFactory.h"
#include "fslib/fs/FileSystemFactory.h"
#include "fslib/fs/AbstractFileSystem.h"
#include "fslib/fs/DummyFile.h"
#include "fslib/fs/ProxyFile.h"
#include "fslib/fs/FileTraverser.h"
#include "fslib/fs/MetricReporter.h"
#include "fslib/util/SafeBuffer.h"
#include "fslib/util/LongIntervalLog.h"
#include "fslib/util/MetricTagsHandler.h"
#include "fslib/config.h"
#include "fslib/fs/local/LocalFileSystem.h"
#include "fslib/fs/ProxyFileSystem.h"
#include "fslib/fs/ErrorGenerator.h"
#include "fslib/cache/FSCacheModule.h"
#include "fslib/common/common_define.h"
#include "fslib/fs/ExceptionTrigger.h"
#include "fslib/fs/MockFile.h"

using namespace std;
using namespace autil;
FSLIB_USE_NAMESPACE(util);
FSLIB_USE_NAMESPACE(config);
FSLIB_USE_NAMESPACE(cache);
FSLIB_BEGIN_NAMESPACE(fs);

AUTIL_DECLARE_AND_SETUP_LOGGER(fs, FileSystem);

#define ZFS_PREFIX "zfs://"
#define ZFS_PREFIX_LEN 6

const int64_t FileSystem::BUFFER_SIZE = 1 << 18;
const string FileSystem::SUFFIX = "__temp__";
const int32_t FileSystem::DEFAULT_THREAD_NUM = 128;
const int32_t FileSystem::DEFAULT_THREAD_QUEUE_SIZE = 1024;

#define FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, param1, param2) { \
        if (toConsole) {                                                \
            fprintf(stderr, errMsg.c_str(), param1.c_str(), param2.c_str()); \
        } else {                                                        \
            AUTIL_LOG(ERROR, errMsg.c_str(), param1.c_str(), param2.c_str()); \
        }                                                               \
    }

#define FSLIB_INTERNAL_LOG_THREE_PARAM(toConsole, errMsg, param1, param2, param3) { \
        if (toConsole) {                                                \
            fprintf(stderr, errMsg.c_str(), param1.c_str(), param2.c_str(), param3.c_str()); \
        } else {                                                        \
            AUTIL_LOG(ERROR, errMsg.c_str(), param1.c_str(), param2.c_str(), param3.c_str()); \
        }                                                               \
    }

bool FileSystem::_useMock = false;
bool FileSystem::_mmapDontDump = ("true" == EnvUtil::getEnv("MMAP_DONTDUMP"));
ErrorCode FileSystem::GENERATE_ERROR(const string& operation, const string& filename) {
    ErrorCode __ec = EC_OK;
    if (unlikely(_useMock)) {
        if (ExceptionTrigger::CanTriggerException(operation, filename, &__ec)) {
            return __ec;
        }
        __ec = ErrorGenerator::getInstance()->generateFileSystemError(operation, filename);
        if (__ec != EC_OK) {
            return __ec;
        }
    }
    return EC_OK;
}

File* FileSystem::openFile(const string& fileName, Flag flag,
                           bool useDirectIO, ssize_t fileLength)
{
    FSLIB_LONG_INTERVAL_LOG("fileName[%s], flag[%d], useDirectIO[%d], fileLength[%ld]",
                            fileName.c_str(), flag, useDirectIO, fileLength);
    ErrorCode __ec = EC_OK;
    if (unlikely(_useMock)) {
        if (ExceptionTrigger::CanTriggerException(OPERATION_OPEN_FILE, fileName, &__ec)) {
            return new DummyFile(__ec);
        }
        __ec = ErrorGenerator::getInstance()->generateFileSystemError(OPERATION_OPEN_FILE, fileName);
        if (__ec != EC_OK) {
            return new DummyFile(__ec);
        }
    }
    File* file = NULL;
    AbstractFileSystem* fs = NULL;
    string fsName, fsType;
    ErrorCode ret = parseInternal(fileName, fs);
    if (ret == EC_OK) {
        if (useDirectIO) {
            flag = (Flag)(flag | USE_DIRECTIO);
        }
        file = fs->openFile(fileName, flag, fileLength);
        if (unlikely(_useMock)) {
            AUTIL_LOG(TRACE1, "use mock file system %p", fs);
            File* mockFile = new MockFile(file);
            return mockFile;
        }
        return file;
    }

    AUTIL_LOG(ERROR, "parse path %s fail!", fileName.c_str());
    return new DummyFile(EC_NOTSUP);
}

MMapFile* FileSystem::mmapFile(const std::string& fileName, Flag openMode,
                               char* start, size_t length, int prot,
                               int mapFlag, off_t offset, ssize_t fileLength)
{
    FSLIB_LONG_INTERVAL_LOG("fileName[%s], openMode[%d], length[%lu], prot[%d], "
                            "mapFlag[%d], offset[%ld], fileLength[%ld]",
                            fileName.c_str(), openMode, length, prot, mapFlag, offset, fileLength);
    ErrorCode __ec = EC_OK;
    if (unlikely(_useMock)) {
        if (ExceptionTrigger::CanTriggerException(OPERATION_MMAP_FILE, fileName, &__ec)) {
            return new MMapFile(fileName, -1, NULL, -1, -1, __ec);
        }
        __ec = ErrorGenerator::getInstance()->generateFileSystemError(OPERATION_MMAP_FILE, fileName);
        if (__ec != EC_OK) {
            return new MMapFile(fileName, -1, NULL, -1, -1, __ec);
        }
    }
    MMapFile* file = NULL;
    AbstractFileSystem* fs = NULL;

    ErrorCode ret = parseInternal(fileName, fs);
    if (ret == EC_OK) {
        file = fs->mmapFile(fileName, openMode, start, length,
                            prot, mapFlag, offset, fileLength);
        if (file && _mmapDontDump) {
            file->setDontDump();
        }
        return file;
    }

    AUTIL_LOG(ERROR, "parse path %s fail!", fileName.c_str());
    return new MMapFile(fileName, -1, NULL, -1, -1, EC_NOTSUP);
}

ErrorCode FileSystem::rename(const string& oldFile,
                             const string& newFile)
{
    FSLIB_LONG_INTERVAL_LOG("oldFile[%s], newFile[%s]", oldFile.c_str(), newFile.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_RENAME, oldFile);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    if (oldFile == newFile) {
        return EC_OK;
    }

    FsType oldFsType, newFsType;
    oldFsType = getFsType(oldFile);
    newFsType = getFsType(newFile);
    if (oldFsType != newFsType) {
        AUTIL_LOG(ERROR, "rename old path %s to new path %s fail, not support"
                  " rename path from different file systems", oldFile.c_str(),
                  newFile.c_str());
        return EC_NOTSUP;

    }

    AbstractFileSystem* oldFs = NULL;
    AbstractFileSystem* newFs = NULL;


    ret = parseInternal(oldFile, oldFs);
    if (ret != EC_OK) {
        AUTIL_LOG(ERROR, "rename old path %s to new path %s fail, parse old"
                  " path fail!", oldFile.c_str(), newFile.c_str());
        return ret;
    }

    ret = parseInternal(newFile, newFs);
    if (ret != EC_OK) {
        AUTIL_LOG(ERROR, "rename old path %s to new path %s fail, parse new"
                  " path fail!", oldFile.c_str(), newFile.c_str());
        return ret;
    }

    // if (oldFs != newFs) {
    //     AUTIL_LOG(ERROR, "rename old path %s to new path %s fail, not support"
    //               " rename path from different file systems", oldFile.c_str(),
    //               newFile.c_str());
    //     return EC_NOTSUP;
    // }

    ret = newFs->rename(oldFile, newFile);
    if (ret == EC_OK) {
        return ret;
    }
    AUTIL_LOG(WARN, "rename old path %s to new path %s fail, ec[%d]",
              oldFile.c_str(), newFile.c_str(), ret);
    return ret;
}

ErrorCode FileSystem::renameInternal(const PathInfo& oldInfo,
                                     const PathInfo& newInfo)
{
    if (oldInfo._fs != newInfo._fs) {
        AUTIL_LOG(ERROR, "internal rename old path %s to new path %s fail, "
                  "not support rename path from different file systems",
                  oldInfo._path.c_str(), newInfo._path.c_str());
        return EC_NOTSUP;
    }

    ErrorCode ret = newInfo._fs->rename(oldInfo._path, newInfo._path);
    if (ret == EC_OK) {
        return ret;
    }
    AUTIL_LOG(ERROR, "internal rename old path %s to new path %s fail, "
              "rename fail!", oldInfo._path.c_str(), newInfo._path.c_str());
    return ret;
}

ErrorCode FileSystem::getFileMeta(const string& fileName,
                                  FileMeta& fileMeta)
{
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", fileName.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_GET_FILE_META, fileName);
    if (unlikely(ret != EC_OK)) {
        return ret;
    }

    AbstractFileSystem* fs = NULL;

    ret = parseInternal(fileName, fs);
    if (ret == EC_OK) {
        ret = fs->getFileMeta(fileName, fileMeta);
        if (ret == EC_OK) {
            return ret;
        }
    }

    if (ret == EC_NOENT) {
        AUTIL_LOG(INFO, "get meta of file %s fail! [%d]", fileName.c_str(), ret);
    } else {
        AUTIL_LOG(WARN, "get meta of file %s fail! [%d]", fileName.c_str(), ret);
    }
    return ret;
}

ErrorCode FileSystem::getPathMeta(const std::string& path,
                                  PathMeta& pathMeta)
{
    FSLIB_LONG_INTERVAL_LOG("path[%s]", path.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_GET_FILE_META, path);
    if (unlikely(ret != EC_OK)) {
        return ret;
    }

    AbstractFileSystem* fs = NULL;

    ret = parseInternal(path, fs);
    if (ret == EC_OK) {
        ret = fs->getPathMeta(path, pathMeta);
        if (ret == EC_OK) {
            return ret;
        }
    }
    if (ret == EC_NOENT) {
        AUTIL_LOG(INFO, "get meta of path %s fail! [%d]", path.c_str(), ret);
    } else {
        AUTIL_LOG(WARN, "get meta of path %s fail! [%d]", path.c_str(), ret);
    }
    return ret;
}

ErrorCode FileSystem::isFile(const string& path) {
    FSLIB_LONG_INTERVAL_LOG("path[%s]", path.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_ISFILE, path);
    if (unlikely(ret != EC_OK)) {
        return ret;
    }

    AbstractFileSystem* fs = NULL;
    ret = parseInternal(path, fs);
    if (ret == EC_OK) {
        ret = fs->isFile(path);
        return ret;
    }
    AUTIL_LOG(ERROR, "parse path %s fail!", path.c_str());
    return ret;
}

bool FileSystem::generatePath(const string& srcPath,
                              const string& dstDir,
                              string& dstPath)
{
    string relativePath = srcPath;

    size_t pos = relativePath.rfind('/');
    if (pos != string::npos) {
        if (pos == relativePath.size() - 1) { // dir
            relativePath.resize(pos);
            pos = relativePath.rfind('/');
            if (pos != string::npos) {
                relativePath = relativePath.substr(pos + 1);
            } else {
                AUTIL_LOG(ERROR, "path %s is invalid", srcPath.c_str());
                return false;
            }
        } else {
            relativePath = relativePath.substr(pos + 1);
        }
    } //else currently only local could be

    return appendPath(dstDir, relativePath, dstPath);
}

bool FileSystem::appendPath(const string& dstDir, const string& relativeName,
                            string& dstPath)
{
    dstPath = dstDir;
    if (dstDir[dstDir.size() - 1] != '/') {
        dstPath.append(1, '/');
    }

    dstPath.append(relativeName);

    return true;
}

ErrorCode FileSystem::copy(const std::string& srcPath, const std::string& dstPath) {
    FSLIB_LONG_INTERVAL_LOG("srcPath[%s], dstPath[%s]", srcPath.c_str(), dstPath.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_COPY, srcPath);
    if (unlikely(ret != EC_OK)) {
        return ret;
    }

    if (srcPath == dstPath) {
        AUTIL_LOG(ERROR, "copy src path %s to dst path %s fail, dst path "
                  "should not equal src path!", srcPath.c_str(),
                  dstPath.c_str());
        return EC_NOTSUP;
    }

    AbstractFileSystem* srcFs;
    ret = parseInternal(srcPath, srcFs);
    if (ret != EC_OK) {
        AUTIL_LOG(ERROR, "copy src path %s to dst path %s fail, parse src path "
                  "fail!", srcPath.c_str(), dstPath.c_str());
        return ret;
    }

    AbstractFileSystem* dstFs;
    ret = parseInternal(dstPath, dstFs);
    if (ret != EC_OK) {
        AUTIL_LOG(ERROR, "copy src path %s to dst path %s fail, parse dst path "
                  "fail!", srcPath.c_str(), dstPath.c_str());
        return ret;
    }

    PathInfo srcInfo(srcFs, srcPath);
    PathInfo dstInfo(dstFs, dstPath);
    if (isZkLikeFileSystem(srcFs) && !isZkLikeFileSystem(dstFs))
    {
        ret = copyZKLikeFsToOtherFs(srcInfo, dstInfo, true, false);
    } else {
        ret = copyAll(srcInfo, dstInfo, true, false);
    }

    return ret;
}


ErrorCode FileSystem::copyZKLikeFsToOtherFs(const PathInfo& srcInfo,
        const PathInfo& dstInfo, bool recursive, bool toConsole)
{
    const string& srcPath = srcInfo._path;
    AbstractFileSystem* srcFs = srcInfo._fs;
    FileList fileList;
    bool created = false;

    ErrorCode ret = srcFs->listDir(srcPath, fileList);
    if (ret != EC_OK) {
        string errMsg = "copy src <%s> to dst <%s> fail, fail to list src node"
                        ", %s\n";
        FSLIB_INTERNAL_LOG_THREE_PARAM(toConsole, errMsg, srcInfo._path,
                dstInfo._path, getErrorString(ret));
        return ret;
    }

    if (fileList.size() == 0) { // treat it as file
        ret  = copyFile(srcInfo, dstInfo, toConsole, created);
        if (ret != EC_OK) {
            return ret;
        }
    } else { //treat it as directory
        if (!recursive) {
            string errMsg = "copy src <%s> to dst <%s> fail, parameter"
                            " -r is needed while copy directory\n";
            FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcInfo._path,
                    dstInfo._path);
            return EC_BADARGS;
        }

        ret = copyDir(srcInfo, dstInfo, fileList, toConsole, created, true);
        if (ret != EC_OK) {
            return ret;
        }
    }

    return EC_OK;
}

ErrorCode FileSystem::copyAll(const PathInfo& srcInfo,
                              const PathInfo& dstInfo,
                              bool recursive,
                              bool toConsole)
{
    bool exist = false;
    bool created = false;
    FileList fileList;
    ErrorCode ret = EC_OK;

    const string& srcPath = srcInfo._path;
    AbstractFileSystem* srcFs = srcInfo._fs;

    if (isZkLikeFileSystem(srcFs)) {
        ret = srcFs->listDir(srcPath, fileList);
        if (ret != EC_OK) {
            string errMsg = "copy src node <%s> to dst <%s> fail, fail to list"
                            " src node, %s\n";
            FSLIB_INTERNAL_LOG_THREE_PARAM(toConsole, errMsg, srcPath,
                    dstInfo._path, getErrorString(ret));
            return ret;
        }

        if (fileList.size() != 0 && !recursive) {
            string errMsg = "copy src node <%s> to dst <%s> fail, parameter"
                            " -r is needed while copy directory\n";
            FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcPath,
                    dstInfo._path);
            return EC_BADARGS;
        }
    }

    if (srcFs->isFile(srcPath) == EC_TRUE) {
        exist = true;
        ret  = copyFile(srcInfo, dstInfo, toConsole, created);
        if (ret != EC_OK) {
            return ret;
        }
    }

    if (srcFs->isDirectory(srcPath) == EC_TRUE) {
        if (!isZkLikeFileSystem(srcFs)) {
            if (!recursive) {
                string errMsg = "copy src <%s> to dst <%s> fail, parameter"
                                " -r is needed while copy directory\n";
                FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg,
                        srcPath, dstInfo._path);
                return EC_BADARGS;
            }

            ret = srcFs->listDir(srcPath, fileList);
            if (ret != EC_OK) {
                string errMsg = "copy src dir <%s> to dst <%s> fail, fail to list"
                                " src dir, %s\n";
                FSLIB_INTERNAL_LOG_THREE_PARAM(toConsole, errMsg,
                        srcInfo._path, dstInfo._path,
                        getErrorString(ret));
                return ret;
            }
        }

        exist = true;
        ret = copyDir(srcInfo, dstInfo, fileList, toConsole, created, false);
        if (ret != EC_OK) {
            return ret;
        }
    }

    if (!exist) {
        string errMsg = "copy src <%s> to dst <%s> fail, make sure src "
                        "does exist\n";
        FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcPath,
                dstInfo._path);
        return EC_NOENT;
    }

    return EC_OK;
}

ErrorCode FileSystem::copyFile(const PathInfo& srcInfo,
                               const PathInfo& dstInfo,
                               bool toConsole, bool& created)
{
    const string& srcPath = srcInfo._path;
    const string& dstPath = dstInfo._path;
    AbstractFileSystem* dstFs = dstInfo._fs;
    string dstFileName;

    bool isDstExistDirectory = (dstFs->isDirectory(dstPath) == EC_TRUE);
    if (isDstExistDirectory || dstPath.rfind('/') == dstPath.size() - 1) {
        if (!generatePath(srcPath, dstPath, dstFileName)) {
            string errMsg = "copy src <%s> to dst <%s> fail, cannot "
                            "generate dst filename.\n";
            FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcInfo._path,
                    dstInfo._path);
            return EC_PARSEFAIL;
        }
        if (!isDstExistDirectory) {
            created = true;
        }
    } else {
        dstFileName.assign(dstPath);
        created = true;
    }

    if (dstFs->isExist(dstFileName) == EC_TRUE) {
        string errMsg = "copy src <%s> to dst <%s> fail, dst path"
                        " already exist.\n";
        FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcInfo._path,
                dstInfo._path);
        return EC_EXIST;
    }

    PathInfo newPathInfo(dstInfo._fs, dstFileName);
    ErrorCode ret = copyFileInternal(srcInfo, newPathInfo);
    if (ret != EC_OK) {
        string errMsg = "copy src file <%s> to dst file <%s> fail, %s\n";
        FSLIB_INTERNAL_LOG_THREE_PARAM(toConsole, errMsg, srcInfo._path,
                newPathInfo._path, getErrorString(ret));
    }
    return ret;
}

ErrorCode FileSystem::copyDir(const PathInfo& srcInfo,
                              const PathInfo& dstInfo,
                              FileList& fileList,
                              bool toConsole, bool created,
                              bool special)
{
    const string& srcPath = srcInfo._path;
    const string& dstPath = dstInfo._path;
    AbstractFileSystem* dstFs = dstInfo._fs;
    ErrorCode ret = EC_OK;
    string dstDir;

    if (dstFs->isDirectory(dstPath) == EC_TRUE && !created) {
        if (!generatePath(srcPath, dstPath, dstDir)) {
            string errMsg = "copy src <%s> to dst <%s> fail, cannot "
                            "generate dst filename.\n";
            FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcInfo._path,
                    dstInfo._path);
            return EC_PARSEFAIL;
        }
    } else {
        dstDir.assign(dstPath);
    }
    PathInfo newDstInfo(dstInfo._fs, dstDir);

    if (dstFs->isDirectory(dstDir) != EC_TRUE) {
        ret = dstFs->mkDir(dstDir, true);
        if (ret != EC_OK) {
            string errMsg = "copy src dir <%s> to dst dir <%s> fail, make dst dir"
                            " fail, %s\n";
            FSLIB_INTERNAL_LOG_THREE_PARAM(toConsole, errMsg, srcInfo._path,
                    dstInfo._path, getErrorString(ret));
            return ret;
        }
    }

    string subSrcPath;
    for (size_t i = 0; i < fileList.size(); i++) {
        if (!appendPath(srcPath, fileList[i], subSrcPath)) {
            string errMsg = "copy src <%s> to dst <%s> fail, cannot "
                            "generate dst filename.\n";
            FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcInfo._path,
                    dstInfo._path);
            return EC_PARSEFAIL;
        }
        PathInfo newSrcInfo(srcInfo._fs, subSrcPath);

        if (special) {
            ret = copyZKLikeFsToOtherFs(newSrcInfo, newDstInfo, true, toConsole);
        } else {
            ret = copyAll(newSrcInfo, newDstInfo, true, toConsole);
        }

        if (ret != EC_OK) {
            return ret;
        }
    }

    return EC_OK;
}

ErrorCode FileSystem::move(const string& srcPath, const string& dstPath) {
    FSLIB_LONG_INTERVAL_LOG("srcPath[%s], dstPath[%s]", srcPath.c_str(), dstPath.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_MOVE, srcPath);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    return moveInternal(srcPath, dstPath, false);
}

ErrorCode FileSystem::moveInternal(const string& srcPath, const string& dstPath,
                                   bool toConsole)
{
    if (srcPath == dstPath) {
        string errMsg = "move fail, dstPath name <%s> should not"
                        " equal srcPath name <%s>\n";
        FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcPath, dstPath);
        return EC_NOTSUP;
    }

    AbstractFileSystem* srcFs;
    ErrorCode ret = parseInternal(srcPath, srcFs);
    if (ret != EC_OK) {
        string errMsg = "move src path <%s> to dst path <%s> fail, parse "
                        "src path fail!";
        FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcPath, dstPath);
        return ret;
    }

    AbstractFileSystem* dstFs;
    ret = parseInternal(dstPath, dstFs);
    if (ret != EC_OK) {
        string errMsg = "move src path <%s> to dst path <%s> fail, parse "
                        "dst path fail!";
        FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcPath, dstPath);
        return ret;
    }

    PathInfo srcInfo(srcFs, srcPath);
    PathInfo dstInfo(dstFs, dstPath);

    ret = EC_OK;
    if (isZkLikeFileSystem(srcFs) && !isZkLikeFileSystem(dstFs)) {
        ret = copyZKLikeFsToOtherFs(srcInfo, dstInfo, true, toConsole);
        if (ret == EC_OK) {
            ret = srcFs->remove(srcInfo._path);
            if (ret == EC_OK) {
                return ret;
            } else {
                string errMsg = "move src path <%s> to dst path <%s> fail, remove "
                                "src path fail!";
                FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcPath, dstPath);
            }
        }
        return ret;
    } else if (srcFs != dstFs) {
        ret = copyAll(srcInfo, dstInfo, true, toConsole);
        if (ret == EC_OK) {
            ret = srcFs->remove(srcInfo._path);
            if (ret == EC_OK) {
                return ret;
            } else {
                string errMsg = "move src path <%s> to dst path <%s> fail, remove "
                                "src path fail!";
                FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcPath, dstPath);
            }
        }
        return ret;
    }

    bool crossDeviceFlag = false;
    string dstFileName;
    if (srcFs->isFile(srcInfo._path) == EC_TRUE) {
        string fileName;
        if (dstFs->isDirectory(dstInfo._path) != EC_TRUE) {
            dstFileName.assign(dstInfo._path);
        } else {
            if (!generatePath(srcInfo._path, dstInfo._path, dstFileName)) {
                string errMsg = "move src <%s> to dst <%s> fail, cannot "
                                "generate dst filename.\n";
                FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcPath, dstPath);
                return EC_PARSEFAIL;
            }
        }

        PathInfo newDstInfo(dstFs, dstFileName);
        ret = renameInternal(srcInfo, newDstInfo);
        if (ret == EC_XDEV) {
            crossDeviceFlag = true;
        } else if (ret != EC_OK) {
            string errMsg = "rename src file <%s> to dst file <%s> fail, %s\n";
            FSLIB_INTERNAL_LOG_THREE_PARAM(toConsole, errMsg, srcInfo._path,
                    newDstInfo._path, getErrorString(ret));
            return ret;
        }
    } else if (srcFs->isDirectory(srcInfo._path) == EC_TRUE) {
        string dstDir;
        if (dstFs->isDirectory(dstInfo._path) == EC_TRUE) {
            if (!generatePath(srcInfo._path, dstInfo._path, dstDir)) {
                string errMsg = "move src <%s> to dst <%s> fail, cannot "
                                "generate dst filename.\n";
                FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcPath, dstPath);
                return EC_PARSEFAIL;
            }
        } else {
            dstDir.assign(dstInfo._path);
        }
        PathInfo newDstInfo(dstFs, dstDir);

        ret = renameInternal(srcInfo, newDstInfo);
        if (ret == EC_XDEV) {
            crossDeviceFlag = true;
        } else if (ret != EC_OK) {
            string errMsg = "rename src dir <%s> to dst dir <%s> fail, %s\n";
            FSLIB_INTERNAL_LOG_THREE_PARAM(toConsole, errMsg, srcInfo._path,
                    newDstInfo._path, getErrorString(ret));
            return ret;
        }
    } else {
        string errMsg = "move src <%s> to dst <%s> fail, make sure src "
                        "does exist\n";
        FSLIB_INTERNAL_LOG_TWO_PARAM(toConsole, errMsg, srcPath, dstPath);
        return EC_NOENT;
    }

    if (crossDeviceFlag) {
        ret = copyAll(srcInfo, dstInfo, true, toConsole);
        if (ret != EC_OK) {
            return ret;
        }
        ret = srcFs->remove(srcInfo._path);
        if (ret != EC_OK) {
            string errMsg = "move src path <%s> to dst path <%s> fail, remove "
                            "src path fail, %s\n";
            FSLIB_INTERNAL_LOG_THREE_PARAM(toConsole, errMsg, srcPath,
                    dstPath, getErrorString(ret));
            return ret;
        }
    }
    return EC_OK;
}

FileChecksum FileSystem::getFileChecksum(const string& fileName) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", fileName.c_str());
    return 0;
}

ErrorCode FileSystem::mkDir(const string& dirName, bool recursive) {
    FSLIB_LONG_INTERVAL_LOG("dirName[%s], recursive[%d]", dirName.c_str(), recursive);
    ErrorCode ret = GENERATE_ERROR(OPERATION_MKDIR, dirName);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    AbstractFileSystem* fs = NULL;

    ret = parseInternal(dirName, fs);
    if (ret == EC_OK) {
        return fs->mkDir(dirName, recursive);
    }

    AUTIL_LOG(ERROR, "parse internal directory %s fail!", dirName.c_str());
    return ret;
}

ErrorCode FileSystem::listDir(const string& dirName, FileList& fileList) {
    FSLIB_LONG_INTERVAL_LOG("dirName[%s]", dirName.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_LISTDIR, dirName);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    AbstractFileSystem* fs = NULL;

    ret = parseInternal(dirName, fs);
    if (ret == EC_OK) {
        return fs->listDir(dirName, fileList);
    }

    AUTIL_LOG(ERROR, "parse internal directory %s fail!", dirName.c_str());
    return ret;
}

ErrorCode FileSystem::listDir(const string& dirName, EntryList& entryList) {
    FSLIB_LONG_INTERVAL_LOG("dirName[%s]", dirName.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_LISTDIR, dirName);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    AbstractFileSystem* fs = NULL;

    ret = parseInternal(dirName, fs);
    if (ret == EC_OK) {
        return fs->listDir(dirName, entryList);
    }

    AUTIL_LOG(ERROR, "parse internal directory %s fail!", dirName.c_str());
    return ret;
}

ErrorCode FileSystem::listDir(const string& dirName, RichFileList& fileList) {
    FSLIB_LONG_INTERVAL_LOG("dirName[%s]", dirName.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_LISTDIR, dirName);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    AbstractFileSystem* fs = NULL;

    ret = parseInternal(dirName, fs);
    if (ret == EC_OK) {
        return fs->listDir(dirName, fileList);
    }

    AUTIL_LOG(ERROR, "parse internal directory %s fail!", dirName.c_str());
    return ret;
}

ErrorCode FileSystem::listDir(const std::string& dirName,
                              EntryInfoMap& entryInfoMap,
                              int32_t threadNum, int32_t threadQueueSize)
{
    FSLIB_LONG_INTERVAL_LOG("dirName[%s]", dirName.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_LISTDIR, dirName);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    autil::ThreadPoolPtr threadPool;
    static autil::ThreadPoolPtr gThreadPool;
    static autil::ThreadMutex gMutex;
    if (reuseThreadPool()) {
        autil::ScopedLock lock(gMutex);
        if (!gThreadPool) {
            auto tmp = autil::ThreadPoolPtr(new autil::ThreadPool(getThreadNum(), 1000));
            if (!tmp->start("fslibListDir")) {
                AUTIL_LOG(ERROR, "start thread pool fail!");
                return EC_FALSE;
            }
            gThreadPool = tmp;
        }
        threadPool = gThreadPool;
    } else {
        threadPool = autil::ThreadPoolPtr(new autil::ThreadPool(threadNum, threadQueueSize));
        if (!threadPool->start("fslibListDir")) {
            AUTIL_LOG(ERROR, "start thread pool fail!");
            return EC_FALSE;
        }
    }
    FileTraverser traverser(dirName, threadPool);
    traverser.traverse(true);
    traverser.wait();
    ret = traverser.getErrorCode();
    if (ret != EC_OK) {
        AUTIL_LOG(WARN, "list directory %s fail!", dirName.c_str());
    } else {
        entryInfoMap = traverser.getResult();
    }
    return ret;
}

ErrorCode FileSystem::isDirectory(const string& path) {
    FSLIB_LONG_INTERVAL_LOG("path[%s]", path.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_ISDIR, path);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    AbstractFileSystem* fs = NULL;
    ret = parseInternal(path, fs);
    if (ret == EC_OK) {
        return fs->isDirectory(path);
    }
    AUTIL_LOG(ERROR, "parse internal for path %s fail!", path.c_str());
    return ret;
}


ErrorCode FileSystem::isLink(const string& path) {
    FSLIB_LONG_INTERVAL_LOG("path[%s]", path.c_str());
    AbstractFileSystem* fs = NULL;

    ErrorCode ret = parseInternal(path, fs);
    if (ret == EC_OK) {
        ProxyFileSystem* proxyFs = dynamic_cast<ProxyFileSystem*>(fs);
        assert(proxyFs);
        LocalFileSystem* localFs = dynamic_cast<LocalFileSystem*>(
            proxyFs ? proxyFs->getFs() : fs);
        if (localFs) {
            return localFs->isLink(path);
        } else {
            return EC_FALSE;
        }
    }

    AUTIL_LOG(ERROR, "parse internal for path %s fail!", path.c_str());
    return ret;
}

ErrorCode FileSystem::remove(const string& path) {
    FSLIB_LONG_INTERVAL_LOG("path[%s]", path.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_REMOVE, path);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    AbstractFileSystem* fs = NULL;
    ret = parseInternal(path, fs);
    if (ret == EC_OK) {
        return fs->remove(path);
    }

    AUTIL_LOG(ERROR, "parse internal path %s fail!", path.c_str());
    return ret;
}

ErrorCode FileSystem::removeFile(const string& path) {
    FSLIB_LONG_INTERVAL_LOG("path[%s]", path.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_REMOVE, path);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    AbstractFileSystem* fs = NULL;
    ret = parseInternal(path, fs);
    if (ret == EC_OK) {
        return fs->removeFile(path);
    }

    AUTIL_LOG(ERROR, "parse internal path %s fail!", path.c_str());
    return ret;
}

ErrorCode FileSystem::removeDir(const string& path) {
    FSLIB_LONG_INTERVAL_LOG("path[%s]", path.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_REMOVE, path);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    AbstractFileSystem* fs = NULL;
    ret = parseInternal(path, fs);
    if (ret == EC_OK) {
        return fs->removeDir(path);
    }

    AUTIL_LOG(ERROR, "parse internal path %s fail!", path.c_str());
    return ret;
}

ErrorCode FileSystem::isExist(const string& path) {
    FSLIB_LONG_INTERVAL_LOG("path[%s]", path.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_ISEXIST, path);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    AbstractFileSystem* fs = NULL;

    ret = parseInternal(path, fs);
    if (ret == EC_OK) {
      return fs->isExist(path);
    }

    AUTIL_LOG(ERROR, "parse internal for path %s fail!", path.c_str());
    return ret;
}

string FileSystem::getErrorString(ErrorCode ec) {
    switch (ec) {
    case EC_OK:
        return FSLIB_EC_OK_ERRSTR;
    case EC_AGAIN:
        return FSLIB_EC_AGAIN_ERRSTR;
    case EC_BADF:
        return FSLIB_EC_BADF_ERRSTR;
    case EC_BADARGS:
        return FSLIB_EC_BADARGS_ERRSTR;
    case EC_BUSY:
        return FSLIB_EC_BUSY_ERRSTR;
    case EC_CONNECTIONLOSS:
        return FSLIB_EC_CONNECTIONLOSS_ERRSTR;
    case EC_EXIST:
        return FSLIB_EC_EXIST_ERRSTR;
    case EC_ISDIR:
        return FSLIB_EC_ISDIR_ERRSTR;
    case EC_KUAFU:
        return FSLIB_EC_KUAFU_ERRSTR;
    case EC_NOENT:
        return FSLIB_EC_NOENT_ERRSTR;
    case EC_NOTDIR:
        return FSLIB_EC_NOTDIR_ERRSTR;
    case EC_NOTEMPTY:
        return FSLIB_EC_NOTEMPTY_ERRSTR;
    case EC_NOTSUP:
        return FSLIB_EC_NOTSUP_ERRSTR;
    case EC_PARSEFAIL:
        return FSLIB_EC_PARSEFAIL_ERRSTR;
    case EC_OPERATIONTIMEOUT:
        return FSLIB_EC_OPERATIONTIMEOUT_ERRSTR;
    case EC_UNKNOWN:
        return FSLIB_EC_UNKNOWN_ERRSTR;
    case EC_PERMISSION:
        return FSLIB_EC_PERMISSION_ERRSTR;
    case EC_XDEV:
        return FSLIB_EC_XDEV_ERRSTR;
    case EC_INVALIDSTATE:
        return FSLIB_EC_INVALIDSTATE_ERRSTR;
    case EC_INIT_ZOOKEEPER_FAIL:
        return FSLIB_EC_INIT_ZOOKEEPER_FAIL_ERRSTR;
    case EC_SESSIONEXPIRED:
        return FSLIB_EC_SESSIONEXPIRED_ERRSTR;
    case EC_PANGU_FILE_LOCK:
        return FSLIB_EC_PANGU_FILE_LOCK_ERRSTR;
    case EC_LOCAL_DISK_NO_SPACE:
        return FSLIB_EC_LOCAL_DISK_NO_SPACE;
    default:
        return string(FSLIB_ERROR_CODE_NOT_SUPPORT) + " " +
            autil::StringUtil::toString(ec);
    }
}

FileLock* FileSystem::getFileLock(const string& fileName) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", fileName.c_str());

    AbstractFileSystem* fs = NULL;
    ErrorCode ret = parseInternal(fileName, fs);
    if (ret != EC_OK) {
        AUTIL_LOG(ERROR, "parse internal for path %s fail!", fileName.c_str());
        return NULL;
    }
    return fs->createFileLock(fileName);
}

FileReadWriteLock* FileSystem::getFileReadWriteLock(const string& fileName) {
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", fileName.c_str());

    AbstractFileSystem* fs = NULL;
    ErrorCode ret = parseInternal(fileName, fs);
    if (ret != EC_OK) {
        AUTIL_LOG(ERROR, "parse internal for path %s fail!", fileName.c_str());
        return NULL;
    }
    return fs->createFileReadWriteLock(fileName);
}

bool FileSystem::getScopedFileLock(const string& fileName,
                                   ScopedFileLock& scopedLock)
{
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", fileName.c_str());
    return scopedLock.init(getFileLock(fileName));
}

bool FileSystem::getScopedFileReadWriteLock(const string& fileName,
        const char mode, ScopedFileReadWriteLock& scopedLock)
{
    FSLIB_LONG_INTERVAL_LOG("fileName[%s]", fileName.c_str());
    return scopedLock.init(getFileReadWriteLock(fileName), mode);
}


ErrorCode FileSystem::copyFileInternal(const PathInfo& srcInfo,
                                       const PathInfo& dstInfo)
{
    const string& srcDstFile = srcInfo._path;
    const string& dstDstFile = dstInfo._path;
    AbstractFileSystem* srcFs = srcInfo._fs;
    AbstractFileSystem* dstFs = dstInfo._fs;

    if (srcDstFile == dstDstFile) {
        string errMsg = string("can not copy file with same file name %s "
                               "but different config");
        AUTIL_LOG(ERROR, errMsg.c_str(), srcDstFile.c_str());
        return EC_EXIST;
    }

    FileMeta srcFileMeta;
    srcFileMeta.fileLength = 0;
    int64_t srcFileLen = 0;

    ErrorCode ret = srcFs->getFileMeta(srcDstFile, srcFileMeta);
    if (ret != EC_OK) {
        AUTIL_LOG(ERROR, "get FileMeta of file %s fail", srcDstFile.c_str());
        return ret;
    }
    srcFileLen = srcFileMeta.fileLength;

    FilePtr source(srcFs->openFile(srcDstFile, READ));
    if (!source.get()) {
        AUTIL_LOG(ERROR, "open file %s fail!", srcDstFile.c_str());
        return EC_PARSEFAIL;
    }
    if (!source->isOpened()) {
        AUTIL_LOG(ERROR, "open file %s fail!", srcDstFile.c_str());
        return source->getLastError();
    }

    SafeBufferPtr safeBuffer;
    if (srcFileLen < BUFFER_SIZE) {
        safeBuffer.reset(new SafeBuffer(srcFileLen));
    } else {
        safeBuffer.reset(new SafeBuffer(BUFFER_SIZE));
    }

    if (srcFileLen == 0) {
        if (!isZkLikeFileSystem(srcFs) &&
            source->read(safeBuffer->getBuffer(), safeBuffer->getSize()) != 0)
        {
            return source->getLastError();
        }

        FilePtr destination(dstFs->openFile(dstDstFile, WRITE));
        if (!destination.get()) {
            AUTIL_LOG(ERROR, "open file %s fail!", dstDstFile.c_str());
            return EC_PARSEFAIL;
        }
        if (!destination->isOpened()) {
            AUTIL_LOG(ERROR, "open file %s fail!", dstDstFile.c_str());
            return destination->getLastError();
        }
        return EC_OK;
    }

    string tmpDstDestFile;
    if (isOssFileSystem(dstFs)) {
        tmpDstDestFile = dstDstFile;
    } else {
        tmpDstDestFile = dstDstFile + SUFFIX;
    }

    FilePtr destination(dstFs->openFile(tmpDstDestFile, WRITE));
    if (!destination.get()) {
        AUTIL_LOG(ERROR, "open file %s fail!", dstDstFile.c_str());
        return EC_PARSEFAIL;
    }
    if (!destination->isOpened()) {
        AUTIL_LOG(ERROR, "open file %s fail!", (tmpDstDestFile).c_str());
        return destination->getLastError();
    }

    ssize_t readLen = 0;
    while (!source->isEof() &&
           (readLen = source->read(safeBuffer->getBuffer(), safeBuffer->getSize())) >= 0)
    {
        ssize_t writeLen  = destination->write(safeBuffer->getBuffer(), readLen);
        if (writeLen != readLen) {
            AUTIL_LOG(ERROR, "write file %s fail!", dstDstFile.c_str());
            dstFs->remove(tmpDstDestFile);
            return destination->getLastError();
        }
    }

    if (readLen < 0) {
        AUTIL_LOG(ERROR, "read file %s fail!", srcDstFile.c_str());
        return source->getLastError();
    }

    ret = destination->close();
    if (ret != EC_OK) {
        AUTIL_LOG(ERROR, "close file %s fail!", (tmpDstDestFile).c_str());
        return ret;
    }

    if (!isOssFileSystem(dstFs)) {
        ret = dstFs->rename(tmpDstDestFile, dstDstFile);
        if (ret != EC_OK) {
            AUTIL_LOG(ERROR, "rename file %s fail!", (tmpDstDestFile).c_str());
            dstFs->remove(tmpDstDestFile);
            return ret;
        }
    }

    return EC_OK;
}

FsType FileSystem::getFsType(const std::string &srcPath) {
    FsType type;
    if (srcPath.size() == 0) {
        type = FSLIB_FS_UNKNOWN_FILESYSTEM;
    }
    size_t found = srcPath.find("://");
    if (found == string::npos) {
        type = FSLIB_FS_LOCAL_FILESYSTEM_NAME;
    } else {
        type = srcPath.substr(0, found);
    }
    return type;
}

ErrorCode FileSystem::readFile(const std::string &filePath, std::string &content) {
    FilePtr file(openFile(filePath, READ));
    if (!file->isOpened()) {
        return file->getLastError();
    }
    const int32_t readSize = 64*1024;
    char buf[readSize];
    ssize_t readBytes = -1;
    while ((readBytes = file->read(buf, readSize)) > 0) {
        content.append(buf, readBytes);
        if (readBytes < readSize) {
            break;
        }
    }
    if (!file->isEof()) {
        return file->getLastError();
    }
    return file->close();

    // ErrorCode ec = isFile(filePath);
    // if (EC_TRUE != ec) {
    //     return ec;
    // }
    // FileMeta fileMeta;
    // ec = getFileMeta(filePath, fileMeta);
    // if (EC_OK != ec) {
    //     return ec;
    // }
    // FilePtr file(openFile(filePath, READ));
    // if (!file->isOpened()) {
    //     return file->getLastError();
    // }
    // size_t fileLength = fileMeta.fileLength;
    // content.resize(fileLength);
    // char* data = const_cast<char*>(content.c_str());
    // ssize_t readBytes = file->read(data, fileLength);
    // if (readBytes != (ssize_t)fileLength) {
    //     return file->getLastError();
    // }
    // return file->close();
}

ErrorCode FileSystem::writeFile(const string& srcFileName, const string &content) {
    FilePtr file(openFile(srcFileName, WRITE));
    if (!file->isOpened()) {
        return file->getLastError();
    }
    ssize_t writeLen = file->write(content.c_str(), content.size());

    if (size_t(writeLen) != content.size()) {
        return file->getLastError();
    }
    return file->close();
}

string FileSystem::joinFilePath(const string &path, const string &subPath) {
    if (path == "") {
        return subPath;
    }
    if (subPath == "") {
        return path;
    }
    string tmp = path;
    if (*tmp.rbegin() != '/') {
        tmp += "/";
    }
    if (*subPath.begin() == '/') {
        tmp += subPath.substr(1, subPath.length() - 1);
    } else {
        tmp += subPath;
    }
    return tmp;
}

string FileSystem::getParentPath(const string& dir)
{
    if (dir.empty()) {
        return "";
    }
    if (dir == "/") {
        return dir;
    }
    size_t delimPos = string::npos;

    if ('/' == *(dir.rbegin())) {
        // the last charactor is '/', then rfind from the next char
        delimPos = dir.rfind('/', dir.size() - 2);
    } else {
        delimPos = dir.rfind('/');
    }

    if (string::npos == delimPos) {
        // not found '/', than parent dir is null
        return "";
    }

    string parentDir = dir.substr(0, delimPos);
    return parentDir;
}

ErrorCode FileSystem::parseInternal(const std::string& srcPath,
                                    AbstractFileSystem*& fs)
{
    FsType type;
    if (srcPath.size() == 0) {
        return EC_PARSEFAIL;
    }
    type = getFsType(srcPath);

    fs = FileSystemFactory::getInstance()->getFs(type);
    if (fs == NULL) {
        AUTIL_LOG(ERROR, "parse path %s fail, not support fs with type %s.",
                  srcPath.c_str(), type.c_str());
        return EC_NOTSUP;
    }

    return EC_OK;
}

bool FileSystem::isZkLikeFileSystem(const AbstractFileSystem* fileSystem) {
    return !(fileSystem->getCapability() & FSC_DISTINCT_FILE_DIR);
}

bool FileSystem::isOssFileSystem(const AbstractFileSystem* fileSystem) {
    // OSS do not support rename and copy operation should directly done, instead of copy to
    // a termp file then move it, for performance consideration
    return !(fileSystem->getCapability() & FSC_BUILTIN_RENAME_SUPPORT);
}

bool FileSystem::reuseThreadPool() {
    char* str = getenv(FSLIB_LISTDIR_REUSE_THREADPOOL);
    bool ret = true;
    if (str) {
        autil::StringUtil::parseTrueFalse(str, ret);
    }
    return ret;
}

int32_t FileSystem::getThreadNum() {
    char* str = getenv(FSLIB_LISTDIR_THREADNUM);
    int32_t ret;
    if (!str || !autil::StringUtil::fromString(str, ret)) {
        ret = 4;
    }
    return ret;
}

ErrorCode FileSystem::forward(const string &command, const string &path,
                              const string &args, string &output)
{
    FSLIB_LONG_INTERVAL_LOG("path[%s]", path.c_str());
    ErrorCode ret = GENERATE_ERROR(OPERATION_FORWARD, path);
    if(unlikely(ret!=EC_OK)){
        return ret;
    }

    AbstractFileSystem* fs = NULL;

    ret = parseInternal(path, fs);
    if (ret == EC_OK) {
        return fs->forward(command, path, args, output);
    }

    AUTIL_LOG(ERROR, "parse internal for path %s fail!", path.c_str());
    return ret;
}

void FileSystem::close()
{
    FileSystemFactory::getInstance()->close();
    MetricReporter::getInstance()->close();
}

util::MetricTagsHandlerPtr FileSystem::getMetricTagsHandler()
{
    return MetricReporter::getInstance()->getMetricTagsHandler();
}

bool FileSystem::setMetricTagsHandler(util::MetricTagsHandlerPtr tagsHandler)
{
    return MetricReporter::getInstance()->updateTagsHandler(tagsHandler);
}

FSCacheModule* FileSystem::getCacheModule()
{
    return FSCacheModule::getInstance();
}

void FileSystem::reportQpsMetric(const string& filePath,
                                 const string& opType,
                                 double value)
{
    MetricReporter::getInstance()->reportQpsMetric(filePath, opType, value);
}

void FileSystem::reportErrorMetric(const string& filePath,
                                   const string& opType,
                                   double value)
{
    MetricReporter::getInstance()->reportErrorMetric(filePath, opType, value);
}

void FileSystem::reportLatencyMetric(const string& filePath,
                                     const string& opType,
                                     int64_t latency)
{
    MetricReporter::getInstance()->reportLatencyMetric(filePath, opType, latency);
}

void FileSystem::reportDNErrorQps(const std::string& filePath,
                                  const std::string& opType,
                                  double value)
{
    MetricReporter::getInstance()->reportDNErrorQps(filePath, opType, value);
}

void FileSystem::reportDNReadErrorQps(const std::string& filePath,
                                      double value)
{
    MetricReporter::getInstance()->reportDNReadErrorQps(filePath, value);
}

void FileSystem::reportDNReadSpeed(const std::string& filePath,
                                   double speed)
{
    MetricReporter::getInstance()->reportDNReadSpeed(filePath, speed);
}

void FileSystem::reportDNReadLatency(const std::string& filePath,
                                     int64_t latency)
{
    MetricReporter::getInstance()->reportDNReadLatency(filePath, latency);
}

void FileSystem::reportDNReadAvgLatency(const std::string& filePath,
                                        int64_t latency)
{
    MetricReporter::getInstance()->reportDNReadAvgLatency(filePath, latency);
}

void FileSystem::reportDNWriteErrorQps(const std::string& filePath,
                                       double value)
{
    MetricReporter::getInstance()->reportDNWriteErrorQps(filePath, value);
}

void FileSystem::reportDNWriteSpeed(const std::string& filePath,
                                   double speed)
{
    MetricReporter::getInstance()->reportDNWriteSpeed(filePath, speed);
}

void FileSystem::reportDNWriteLatency(const std::string& filePath,
                                     int64_t latency)
{
    MetricReporter::getInstance()->reportDNWriteLatency(filePath, latency);
}

void FileSystem::reportDNWriteAvgLatency(const std::string& filePath,
                                         int64_t latency)
{
    MetricReporter::getInstance()->reportDNWriteAvgLatency(filePath, latency);
}

void FileSystem::reportMetaCachedPathCount(int64_t fileCount)
{
    MetricReporter::getInstance()->reportMetaCachedPathCount(fileCount);
}

void FileSystem::reportMetaCacheImmutablePathCount(int64_t fileCount)
{
    MetricReporter::getInstance()->reportMetaCacheImmutablePathCount(fileCount);
}

void FileSystem::reportMetaCacheHitQps(const string& filePath, double value)
{
    MetricReporter::getInstance()->reportMetaCacheHitQps(filePath, value);
}

void FileSystem::reportDataCachedFileCount(int64_t fileCount)
{
    MetricReporter::getInstance()->reportDataCachedFileCount(fileCount);
}

void FileSystem::reportDataCacheMemUse(int64_t fileCount)
{
    MetricReporter::getInstance()->reportDataCacheMemUse(fileCount);
}

void FileSystem::reportDataCacheHitQps(const string& filePath, double value)
{
    MetricReporter::getInstance()->reportDataCacheHitQps(filePath, value);
}

string FileSystem::getPathFromZkPath(const std::string &zkPath) {
    if (!autil::StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        AUTIL_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    std::string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    std::string::size_type pos = tmpStr.find("/");
    if (pos == std::string::npos) {
        return "";
    }
    return tmpStr.substr(pos);
}


FSLIB_END_NAMESPACE(fs);
