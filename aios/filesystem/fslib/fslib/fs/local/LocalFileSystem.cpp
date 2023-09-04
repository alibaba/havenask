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
#include "fslib/fs/local/LocalFileSystem.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "autil/StringUtil.h"
#include "fslib/fs/local/LocalDirectFile.h"
#include "fslib/fs/local/LocalFile.h"
#include "fslib/fs/local/LocalFileLock.h"
#include "fslib/fs/local/LocalFileReadWriteLock.h"

using namespace std;
FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(local, LocalFileSystem);

static const char DIR_DELIM = '/';
static const size_t MMAP_BLOCK_SIZE = 4 * 1024 * 1024; // 4M

LocalFileSystem::LocalFileSystem() {}

LocalFileSystem::~LocalFileSystem() {}

bool LocalFileSystem::normalizeFilePath(const string &filePath, string &normalizedFilePath) {
    normalizedFilePath.clear();
    if (filePath.size() == 0) {
        return false;
    }
    normalizedFilePath = filePath;
    if (normalizedFilePath.size() > 1 && *normalizedFilePath.rbegin() == '/') {
        normalizedFilePath.resize(normalizedFilePath.size() - 1);
    }
    if (autil::StringUtil::startsWith(normalizedFilePath, "LOCAL://")) {
        normalizedFilePath = normalizedFilePath.substr(7);
    }
    return true;
}

string LocalFileSystem::getParentDir(const string &currentDir) {
    if (currentDir.empty()) {
        return "";
    }

    size_t delimPos = string::npos;
    if (DIR_DELIM == *(currentDir.rbegin())) {
        // the last charactor is '/', then rfind from the next char
        delimPos = currentDir.rfind(DIR_DELIM, currentDir.size() - 2);
    } else {
        delimPos = currentDir.rfind(DIR_DELIM);
    }

    if (string::npos == delimPos) {
        // not found '/', than parent dir is null
        return "";
    }

    string parentDir = currentDir.substr(0, delimPos);
    return parentDir;
}

File *LocalFileSystem::openFile(const string &rawFileName, Flag flag) {
    string fileName("");
    if (!normalizeFilePath(rawFileName, fileName)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawFileName.c_str());
        return NULL;
    }
    bool useDirectIO = flag & USE_DIRECTIO;

    if (useDirectIO) {
        AUTIL_LOG(DEBUG, "open file[%s] in direct IO mode.", fileName.c_str());
        int fd = -1;
        switch (flag & FLAG_CMD_MASK) {
        case READ: {
            if (isExistInternal(fileName) != EC_TRUE) {
                AUTIL_LOG(WARN, "direct openFile %s fail, file not exist", fileName.c_str());
                return new LocalDirectFile(fileName, -1, EC_NOENT);
            }
            if (isDirectoryInternal(fileName) == EC_TRUE) {
                AUTIL_LOG(ERROR, "direct openFile %s fail, cannot open dir", fileName.c_str());
                return new LocalDirectFile(fileName, -1, EC_ISDIR);
            }
            fd = ::open(fileName.c_str(), O_DIRECT | O_RDONLY, S_IRWXU);
            break;
        }
        case WRITE: {
            const string &parentDir = getParentDir(fileName);
            if (parentDir != "" && isExistInternal(parentDir) != EC_TRUE) {
                ErrorCode ret = mkDirInternal(parentDir, true);
                if (ret != EC_OK && ret != EC_EXIST) {
                    AUTIL_LOG(ERROR, "openFile %s fail, cannot create parent dir", fileName.c_str());
                    return new LocalDirectFile(fileName, -1, ret);
                }
            }
            fd = ::open(fileName.c_str(), O_CREAT | O_DIRECT | O_WRONLY | O_TRUNC, S_IRWXU | S_IRGRP | S_IROTH);
            break;
        }
        case APPEND: {
            const string &parentDir = getParentDir(fileName);
            if (parentDir != "" && isExistInternal(parentDir) != EC_TRUE) {
                ErrorCode ret = mkDirInternal(parentDir, true);
                if (ret != EC_OK && ret != EC_EXIST) {
                    AUTIL_LOG(ERROR, "openFile %s fail, cannot create parent dir", fileName.c_str());
                    return new LocalDirectFile(fileName, -1, ret);
                }
            }
            fd = ::open(fileName.c_str(), O_CREAT | O_DIRECT | O_WRONLY | O_APPEND, S_IRWXU | S_IRGRP | S_IROTH);
            break;
        }
        default: {
            AUTIL_LOG(ERROR, "direct openFile %s fail, flag %d not support", fileName.c_str(), flag);
            return new LocalDirectFile(fileName, -1, EC_NOTSUP);
        }
        }
        if (fd > 0) {
            return new LocalDirectFile(fileName, fd);
        }
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "open file %s fail, %s", fileName.c_str(), strerror(errno));
        return new LocalDirectFile(fileName, -1, convertErrno(curErrno));
    } else {
        AUTIL_LOG(DEBUG, "open file[%s] in normal mode.", fileName.c_str());
        FILE *file = NULL;
        switch (flag & FLAG_CMD_MASK) {
        case READ: {
            AUTIL_LOG(TRACE1, "open file[%s] in read mode.", fileName.c_str());
            if (isExistInternal(fileName) != EC_TRUE) {
                AUTIL_LOG(INFO, "openFile %s fail, which does not exist", fileName.c_str());
                return new LocalFile(fileName, NULL, EC_NOENT);
            }
            if (isDirectoryInternal(fileName) == EC_TRUE) {
                AUTIL_LOG(ERROR, "openFile %s fail, cannot open dir", fileName.c_str());
                return new LocalFile(fileName, NULL, EC_ISDIR);
            }
            file = fopen(fileName.c_str(), "r");
            break;
        }
        case WRITE: {
            AUTIL_LOG(TRACE1, "open file[%s] in write mode.", fileName.c_str());
            const string &parentDir = getParentDir(fileName);
            if (parentDir != "" && isExistInternal(parentDir) != EC_TRUE) {
                ErrorCode ret = mkDirInternal(parentDir, true);
                if (ret != EC_OK && ret != EC_EXIST) {
                    AUTIL_LOG(ERROR, "openFile %s fail, cannot create parent dir", fileName.c_str());
                    return new LocalFile(fileName, NULL, ret);
                }
            }
            file = fopen(fileName.c_str(), "w");
            break;
        }
        case APPEND: {
            AUTIL_LOG(TRACE1, "open file[%s] in append mode.", fileName.c_str());
            const string &parentDir = getParentDir(fileName);
            if (parentDir != "" && isExistInternal(parentDir) != EC_TRUE) {
                ErrorCode ret = mkDirInternal(parentDir, true);
                if (ret != EC_OK && ret != EC_EXIST) {
                    AUTIL_LOG(ERROR, "openFile %s fail, cannot create parent dir", fileName.c_str());
                    return new LocalFile(fileName, NULL, ret);
                }
            }
            file = fopen(fileName.c_str(), "a");
            break;
        }
        default: {
            AUTIL_LOG(
                ERROR, "openFile %s fail, flag %d [%d] not support", fileName.c_str(), flag, flag & FLAG_CMD_MASK);
            return new LocalFile(fileName, NULL, EC_NOTSUP);
        }
        }

        if (file) {
            return new LocalFile(fileName, file);
        }

        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "open file %s fail, %s", fileName.c_str(), strerror(errno));
        return new LocalFile(fileName, NULL, convertErrno(curErrno));
    }
}

MMapFile *LocalFileSystem::mmapFile(
    const std::string &rawFileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset) {
    string fileName("");
    if (!normalizeFilePath(rawFileName, fileName)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawFileName.c_str());
        return NULL;
    }

    int fd;
    int pos = 0;
    switch (openMode) {
    case READ:
        fd = open(fileName.c_str(), O_RDONLY, 0644);
        break;
    case WRITE:
        fd = open(fileName.c_str(), O_RDWR | O_CREAT, 0644);
        break;
    default:
        AUTIL_LOG(ERROR, "mmap file %s fail, open mode not support.", fileName.c_str());
        return new MMapFile(fileName, -1, NULL, 0, 0, EC_NOTSUP);
    }

    if (fd == -1) {
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "mmap file %s fail, %s.", fileName.c_str(), strerror(errno));
        return new MMapFile(fileName, fd, NULL, 0, 0, convertErrno(curErrno));
    }

    struct stat statBuf;
    if (stat(fileName.c_str(), &statBuf) < 0) {
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "mmap file %s fail, get file length fail, %s.", fileName.c_str(), strerror(errno));
        return new MMapFile(fileName, fd, NULL, 0, 0, convertErrno(curErrno));
        ;
    }

    if (length == 0) {
        length = statBuf.st_size;
    }

    int truncRet = 0;
    if (openMode == WRITE) {
        truncRet = ftruncate(fd, length);
    }

    if (truncRet == -1) {
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "mmap file %s fail, truncate fail, %s.", fileName.c_str(), strerror(errno));
        return new MMapFile(fileName, fd, NULL, 0, 0, convertErrno(curErrno));
    }

    bool needLock = mapFlag & MAP_LOCKED;
    mapFlag &= ~MAP_LOCKED;
    char *base = (char *)mmap(NULL, length, prot, mapFlag, fd, offset);
    if (base == MAP_FAILED) {
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "mmap file %s fail, %s.", fileName.c_str(), strerror(errno));
        return new MMapFile(fileName, fd, NULL, 0, 0, convertErrno(curErrno));
    }

    ErrorCode ec = EC_OK;
    if (needLock) {
        ec = doLockMmapFile(base, length);
    }

    return new MMapFile(fileName, fd, base, length, pos, ec);
}

ErrorCode LocalFileSystem::doLockMmapFile(const char *base, int64_t fileLength) {
    for (int64_t offset = 0; offset < fileLength; offset += MMAP_BLOCK_SIZE) {
        int64_t actualLen = min(fileLength - offset, (int64_t)MMAP_BLOCK_SIZE);
        if (mlock(base + offset, actualLen) < 0) {
            return convertErrno(errno);
        }
    }
    return EC_OK;
}

ErrorCode LocalFileSystem::rename(const string &oldRawFileName, const string &newRawFileName) {
    string oldFileName("");
    if (!normalizeFilePath(oldRawFileName, oldFileName)) {
        AUTIL_LOG(ERROR, "renameFile %s fail, normalize path fail", oldRawFileName.c_str());
        return EC_PARSEFAIL;
    }
    string newFileName("");
    if (!normalizeFilePath(newRawFileName, newFileName)) {
        AUTIL_LOG(ERROR, "renameFile %s fail, normalize path fail", newRawFileName.c_str());
        return EC_PARSEFAIL;
    }

    if (isExistInternal(oldFileName) != EC_TRUE) {
        AUTIL_LOG(WARN, "renameFile %s to %s fail, oldFile does not exist.", oldFileName.c_str(), newFileName.c_str());
        return EC_NOENT;
    }

    if (isExistInternal(newFileName) == EC_TRUE) {
        AUTIL_LOG(ERROR, "renameFile %s fail, newFileName %s exist.", oldFileName.c_str(), newFileName.c_str());
        return EC_EXIST;
    }

    if (isFileInternal(oldFileName) == EC_TRUE && newFileName[newFileName.length() - 1] == '/') {
        AUTIL_LOG(ERROR, "renameFile %s to %s fail, oldFileName is not dir.", oldFileName.c_str(), newFileName.c_str());
        return EC_NOTDIR;
    }

    const string &parentDir = getParentDir(newFileName);
    if (parentDir != "" && isExistInternal(parentDir) != EC_TRUE) {
        ErrorCode ret = mkDirInternal(parentDir, true);
        if (ret != EC_OK && ret != EC_EXIST) {
            return ret;
        }
    }

    if (::rename(oldFileName.c_str(), newFileName.c_str()) != 0) {
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "renameFile %s fail, %s", oldFileName.c_str(), strerror(errno));
        return convertErrno(curErrno);
    }

    return EC_OK;
}

ErrorCode LocalFileSystem::link(const std::string &srcPath, const std::string &dstPath) {
    string oldFileName("");
    if (!normalizeFilePath(srcPath, oldFileName)) {
        AUTIL_LOG(ERROR, "linkFile %s fail, normalize path fail", srcPath.c_str());
        return EC_PARSEFAIL;
    }
    string newFileName("");
    if (!normalizeFilePath(dstPath, newFileName)) {
        AUTIL_LOG(ERROR, "linkFile %s fail, normalize path fail", dstPath.c_str());
        return EC_PARSEFAIL;
    }

    if (isExistInternal(oldFileName) != EC_TRUE) {
        AUTIL_LOG(WARN, "linkFile %s to %s fail, oldFile does not exist.", oldFileName.c_str(), newFileName.c_str());
        return EC_NOENT;
    }

    if (isExistInternal(newFileName) == EC_TRUE) {
        AUTIL_LOG(ERROR, "linkFile %s fail, newFileName %s exist.", oldFileName.c_str(), newFileName.c_str());
        return EC_EXIST;
    }
    const string &parentDir = getParentDir(newFileName);
    if (parentDir != "" && isExistInternal(parentDir) != EC_TRUE) {
        ErrorCode ret = mkDirInternal(parentDir, true);
        if (ret != EC_OK && ret != EC_EXIST) {
            return ret;
        }
    }
    if (::link(oldFileName.c_str(), newFileName.c_str()) != 0) {
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "linkFile %s fail, %s", oldFileName.c_str(), strerror(errno));
        return convertErrno(curErrno);
    }
    return EC_OK;
}

ErrorCode LocalFileSystem::getFileMeta(const string &rawFileName, FileMeta &meta) {
    string fileName("");
    if (!normalizeFilePath(rawFileName, fileName)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawFileName.c_str());
        return EC_PARSEFAIL;
    }
    struct stat statBuf;
    if (stat(fileName.c_str(), &statBuf) < 0) {
        int32_t curErrno = errno;
        AUTIL_LOG(INFO, "getFileMeta %s fail, %s", fileName.c_str(), strerror(errno));
        return convertErrno(curErrno);
    }

    meta.fileLength = statBuf.st_size;
    meta.createTime = statBuf.st_ctime;
    meta.lastModifyTime = statBuf.st_mtime;

    return EC_OK;
}

ErrorCode LocalFileSystem::isFile(const string &rawPath) {
    string path("");
    if (!normalizeFilePath(rawPath, path)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawPath.c_str());
        return EC_PARSEFAIL;
    }
    return isFileInternal(path);
}

ErrorCode LocalFileSystem::isFileInternal(const string &path) {
    struct stat buf;
    if (stat(path.c_str(), &buf) < 0) {
        int32_t curErrno = errno;
        AUTIL_LOG(INFO, "isFile for path %s fail, %s", path.c_str(), strerror(errno));
        return convertErrno(curErrno);
    }

    if (S_ISREG(buf.st_mode)) {
        return EC_TRUE;
    }
    return EC_FALSE;
}

ErrorCode LocalFileSystem::isLink(const string &rawPath) {
    string path;
    if (!normalizeFilePath(rawPath, path)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawPath.c_str());
        return EC_PARSEFAIL;
    }
    return isLinkInternal(path);
}

ErrorCode LocalFileSystem::isLinkInternal(const string &path) {
    struct stat buf;
    if (lstat(path.c_str(), &buf) < 0) {
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "isLink for path %s fail, %s", path.c_str(), strerror(errno));
        return convertErrno(curErrno);
    }
    if (S_ISLNK(buf.st_mode)) {
        return EC_TRUE;
    }
    return EC_FALSE;
}

FileChecksum LocalFileSystem::getFileChecksum(const string &fileName) { return 0; }

ErrorCode LocalFileSystem::mkNestDir(const string &dirName) {
    size_t pos = dirName.rfind('/');
    if (pos == string::npos) {
        if (mkdir(dirName.c_str(), 0755) < 0) {
            if (errno != EEXIST) {
                int32_t curErrno = errno;
                AUTIL_LOG(ERROR, "create directory %s fail, %s", dirName.c_str(), strerror(errno));
                return convertErrno(curErrno);
                ;
            }
        }
        return EC_OK;
    }

    string parentDir = dirName.substr(0, pos);
    if (!parentDir.empty() && access(parentDir.c_str(), F_OK) != 0) {
        ErrorCode ret = mkNestDir(parentDir);
        if (ret != EC_OK) {
            return ret;
        }
    }

    if (mkdir(dirName.c_str(), 0755) < 0) {
        if (errno != EEXIST) {
            int32_t curErrno = errno;
            AUTIL_LOG(ERROR, "create directory %s fail, %s", dirName.c_str(), strerror(errno));
            return convertErrno(curErrno);
        }
    }

    return EC_OK;
}

ErrorCode LocalFileSystem::mkDir(const string &rawDirName, bool recursive) {
    string dirName("");
    if (!normalizeFilePath(rawDirName, dirName)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawDirName.c_str());
        return EC_PARSEFAIL;
    }
    return mkDirInternal(dirName, recursive);
}

ErrorCode LocalFileSystem::mkDirInternal(const string &dirName, bool recursive) {
    string dir = dirName;
    size_t len = dir.size();
    if (dir[len - 1] == '/') {
        if (len == 1) {
            AUTIL_LOG(WARN, "directory %s already exist", dir.c_str());
            return EC_EXIST;
        } else {
            dir.resize(len - 1);
        }
    }

    if (access(dir.c_str(), F_OK) == 0) {
        AUTIL_LOG(INFO, "directory %s already exist", dirName.c_str());
        return EC_EXIST;
    }

    size_t pos = dir.rfind('/');
    if (pos == string::npos) {
        if (mkdir(dir.c_str(), 0755) < 0) {
            int32_t curErrno = errno;
            AUTIL_LOG(ERROR, "create directory %s fail, %s", dir.c_str(), strerror(errno));
            return convertErrno(curErrno);
            ;
        }
        return EC_OK;
    }
    string parentDir = dir.substr(0, pos);
    if (!parentDir.empty() && access(parentDir.c_str(), F_OK) != 0) {
        if (recursive) {
            ErrorCode ret = mkNestDir(parentDir);
            if (ret != EC_OK) {
                AUTIL_LOG(ERROR,
                          "create directory %s fail, fail to create "
                          "parent dir",
                          dir.c_str());
                return ret;
            }
        } else {
            int32_t curErrno = errno;
            AUTIL_LOG(ERROR, "create directory %s fail, parameter -p is needed", dir.c_str());
            return convertErrno(curErrno);
        }
    }

    if (mkdir(dir.c_str(), 0755) < 0) {
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "create directory %s fail, %s", dir.c_str(), strerror(errno));
        return convertErrno(curErrno);
    }

    return EC_OK;
}

ErrorCode LocalFileSystem::listDir(const string &rawDirName, FileList &fileList) {
    string dirName("");
    if (!normalizeFilePath(rawDirName, dirName)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawDirName.c_str());
        return EC_PARSEFAIL;
    }
    fileList.clear();

    DIR *dp;
    struct dirent *ep;
    dp = opendir(dirName.c_str());
    if (dp == NULL) {
        int32_t curErrno = errno;
        AUTIL_LOG(WARN, "open dir %s fail, %s", dirName.c_str(), strerror(errno));
        return convertErrno(curErrno);
        ;
    }

    while ((ep = readdir(dp)) != NULL) {
        if (strcmp(ep->d_name, ".") == 0 || strcmp(ep->d_name, "..") == 0) {
            continue;
        }
        fileList.push_back(ep->d_name);
    }
    if (closedir(dp) < 0) {
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "close dir %s fail, %s", dirName.c_str(), strerror(errno));
        return convertErrno(curErrno);
    }

    return EC_OK;
}

ErrorCode LocalFileSystem::listDir(const string &rawDirName, EntryList &entryList) {
    string dirName("");
    if (!normalizeFilePath(rawDirName, dirName)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawDirName.c_str());
        return EC_PARSEFAIL;
    }
    FileList fileList;
    ErrorCode ec = listDir(dirName, fileList);
    if (ec != EC_OK) {
        return ec;
    }
    entryList.clear();
    for (size_t i = 0; i < fileList.size(); ++i) {
        EntryMeta entry;
        entry.entryName = fileList[i];
        ec = isDirectoryInternal(dirName + "/" + fileList[i]);
        if (ec == EC_TRUE) {
            entry.isDir = true;
        } else if (ec == EC_FALSE) {
            entry.isDir = false;
        } else {
            AUTIL_LOG(ERROR, "isDirectory %s failed, errorCode: %d.", (dirName + "/" + fileList[i]).c_str(), ec);
            continue;
        }
        entryList.push_back(entry);
    }
    return EC_OK;
}

ErrorCode LocalFileSystem::listDir(const string &rawDirName, RichFileList &fileList) {
    string dirName("");
    if (!normalizeFilePath(rawDirName, dirName)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawDirName.c_str());
        return EC_PARSEFAIL;
    }
    fileList.clear();

    DIR *dp;
    struct dirent *ep;
    dp = opendir(dirName.c_str());
    if (dp == NULL) {
        int32_t curErrno = errno;
        AUTIL_LOG(WARN, "open dir %s fail, %s", dirName.c_str(), strerror(errno));
        return convertErrno(curErrno);
        ;
    }

    while ((ep = readdir(dp)) != NULL) {
        if (strcmp(ep->d_name, ".") == 0 || strcmp(ep->d_name, "..") == 0) {
            continue;
        }
        struct stat statBuf;
        string fileName = dirName + '/' + ep->d_name;
        if (stat(fileName.c_str(), &statBuf) < 0) {
            int32_t curErrno = errno;
            AUTIL_LOG(WARN, "stat %s fail, %s", fileName.c_str(), strerror(errno));
            return convertErrno(curErrno);
        }
        RichFileMeta meta;
        meta.path = ep->d_name;
        meta.size = statBuf.st_size;
        meta.physicalSize = meta.size;
        meta.createTime = statBuf.st_ctime;
        meta.lastModifyTime = statBuf.st_mtime;
        meta.isDir = S_ISDIR(statBuf.st_mode);
        fileList.push_back(meta);
    }
    if (closedir(dp) < 0) {
        int32_t curErrno = errno;
        AUTIL_LOG(ERROR, "close dir %s fail, %s", dirName.c_str(), strerror(errno));
        return convertErrno(curErrno);
    }

    return EC_OK;
}

ErrorCode LocalFileSystem::isDirectory(const string &rawPath) {
    string path("");
    if (!normalizeFilePath(rawPath, path)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawPath.c_str());
        return EC_PARSEFAIL;
    }
    return isDirectoryInternal(path);
}

ErrorCode LocalFileSystem::isDirectoryInternal(const string &path) {
    struct stat buf;
    if (stat(path.c_str(), &buf) < 0) {
        return convertErrno(errno);
    }

    if (S_ISDIR(buf.st_mode)) {
        return EC_TRUE;
    }
    return EC_FALSE;
}

ErrorCode LocalFileSystem::remove(const string &rawPath) {
    string path("");
    if (!normalizeFilePath(rawPath, path)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawPath.c_str());
        return EC_NOENT;
    }
    ErrorCode ret = isExistInternal(path);
    if (ret == EC_TRUE) {
        return removeInternal(path);
    } else if (ret == EC_FALSE) {
        return EC_NOENT;
    } else {
        return ret;
    }
}

ErrorCode LocalFileSystem::removeInternal(const string &path) {
    if (isLinkInternal(path) == EC_TRUE) {
        return removeTypeInternal(path, "link");
    }
    if (isFileInternal(path) == EC_TRUE) {
        return removeTypeInternal(path, "file");
    }
    if (isDirectoryInternal(path) == EC_TRUE) {
        return removeDirInternal(path);
    }
    return removeTypeInternal(path, "unknown");
}

ErrorCode LocalFileSystem::removeFile(const string &rawPath) {
    string path("");
    if (!normalizeFilePath(rawPath, path)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawPath.c_str());
        return EC_PARSEFAIL;
    }

    ErrorCode ret = isExistInternal(path);
    if (ret == EC_FALSE) {
        return EC_NOENT;
    } else if (ret != EC_TRUE) {
        return ret;
    }
    if (isLinkInternal(path) == EC_TRUE) {
        return removeTypeInternal(path, "link");
    }
    if (isFileInternal(path) == EC_TRUE) {
        return removeTypeInternal(path, "file");
    }
    if (isDirectoryInternal(path) == EC_TRUE) {
        return EC_ISDIR;
    }
    return removeTypeInternal(path, "unknown");
}

ErrorCode LocalFileSystem::removeDir(const string &rawPath) {
    string path("");
    if (!normalizeFilePath(rawPath, path)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawPath.c_str());
        return EC_PARSEFAIL;
    }

    ErrorCode ret = isExistInternal(path);
    if (ret == EC_FALSE) {
        return EC_NOENT;
    } else if (ret != EC_TRUE) {
        return ret;
    }
    if (isLinkInternal(path) == EC_TRUE) {
        return removeTypeInternal(path, "link");
    }
    if (isDirectoryInternal(path) == EC_TRUE) {
        return removeDirInternal(path);
    }
    if (isFileInternal(path) == EC_TRUE) {
        return EC_NOTDIR;
    }
    return removeTypeInternal(path, "unknown");
}

ErrorCode LocalFileSystem::removeTypeInternal(const string &path, const char *type) {
    if (::remove(path.c_str()) != 0) {
        if (errno != ENOENT) {
            int32_t curErrno = errno;
            AUTIL_LOG(ERROR, "remove %s %s fail, %s", type, path.c_str(), strerror(errno));
            return convertErrno(curErrno);
        }
    }
    return EC_OK;
}

ErrorCode LocalFileSystem::removeDirInternal(const string &path) {
    DIR *pDir;
    struct dirent *ent;
    string childName;
    pDir = opendir(path.c_str());
    if (pDir == NULL) {
        if (errno != ENOENT) {
            int32_t curErrno = errno;
            AUTIL_LOG(ERROR, "remove path fail %s", strerror(errno));
            return convertErrno(curErrno);
        } else {
            return EC_OK;
        }
    }
    while ((ent = readdir(pDir)) != NULL) {
        childName = path + '/' + ent->d_name;
        bool isDir = false;
        if (ent->d_type == DT_UNKNOWN) {
            struct stat sb;
            if (stat(childName.c_str(), &sb) == -1) {
                int32_t curErrno = errno;
                AUTIL_LOG(ERROR, "stat dir %s fail, %s", childName.c_str(), strerror(errno));
                return convertErrno(curErrno);
            }
            isDir = S_ISDIR(sb.st_mode);
        } else {
            isDir = ent->d_type & DT_DIR;
        }

        if (isDir) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) {
                continue;
            }
            if (removeInternal(childName) != EC_OK) {
                AUTIL_LOG(ERROR, "delete dir %s fail, %s", childName.c_str(), strerror(errno));
                closedir(pDir);
                return convertErrno(errno);
            }
        } else {
            if (::remove(childName.c_str()) != 0) {
                if (errno != ENOENT) {
                    int32_t curErrno = errno;
                    AUTIL_LOG(ERROR, "delete file %s fail, %s", childName.c_str(), strerror(errno));
                    closedir(pDir);
                    return convertErrno(curErrno);
                }
            }
        }
    }

    closedir(pDir);
    if (::remove(path.c_str()) != 0) {
        if (errno != ENOENT) {
            int32_t curErrno = errno;
            AUTIL_LOG(ERROR, "remove dir %s fail, %s", path.c_str(), strerror(errno));
            return convertErrno(curErrno);
        }
    }
    return EC_OK;
}

ErrorCode LocalFileSystem::isExist(const string &rawPath) {
    string path("");
    if (!normalizeFilePath(rawPath, path)) {
        AUTIL_LOG(ERROR, "openFile %s fail, normalize path fail", rawPath.c_str());
        return EC_PARSEFAIL;
    }
    return isExistInternal(path);
}

ErrorCode LocalFileSystem::isExistInternal(const string &path) {
    if (access(path.c_str(), F_OK) == 0) {
        return EC_TRUE;
    } else if (errno == ENOENT) {
        return EC_FALSE;
    }

    int32_t curErrno = errno;
    AUTIL_LOG(ERROR, "isPathExist %s fail, %s", path.c_str(), strerror(errno));
    return convertErrno(curErrno);
}

ErrorCode LocalFileSystem::convertErrno(int errNum) {
    switch (errNum) {
    case 0:
        return EC_OK;
    case EAGAIN:
        return EC_AGAIN;
    case EBADF:
        return EC_BADF;
    case EBUSY:
        return EC_BUSY;
    case EEXIST:
        return EC_EXIST;
    case EINVAL:
        return EC_BADARGS;
    case EISDIR:
        return EC_ISDIR;
    case ENOENT:
        return EC_NOENT;
    case ENOTDIR:
        return EC_NOTDIR;
    case ENOTEMPTY:
        return EC_NOTEMPTY;
    case EOPNOTSUPP:
    case EPERM:
        return EC_NOTSUP;
    case EACCES:
        return EC_PERMISSION;
    case EXDEV:
        return EC_XDEV;
    case ENOSPC:
        return EC_LOCAL_DISK_NO_SPACE;
    case EIO:
        return EC_LOCAL_DISK_IO_ERROR;
    default:
        AUTIL_LOG(ERROR, "unknown errNum: %d", errNum);
        return EC_UNKNOWN;
    }
}

FileReadWriteLock *LocalFileSystem::createFileReadWriteLock(const string &fileName) {
    return new LocalFileReadWriteLock(fileName);
}

FileLock *LocalFileSystem::createFileLock(const string &fileName) { return new LocalFileLock(fileName); }

FSLIB_END_NAMESPACE(local);
