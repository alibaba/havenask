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
#include <assert.h>

#include "autil/StackTracer.h"
#include "autil/TimeUtility.h"
#ifdef ENABLE_BEEPER
#include "beeper/beeper.h"
#endif
#include "fslib/cache/FSCacheModule.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/MemDataFile.h"
#include "fslib/fs/MetricReporter.h"
#include "fslib/fs/MultiSpeedController.h"
#include "fslib/fs/ProxyFile.h"
#include "fslib/fs/ProxyFileSystem.h"
#include "fslib/util/MetricTagsHandler.h"
#include "fslib/util/PathUtil.h"

using namespace std;
using namespace autil;
FSLIB_USE_NAMESPACE(cache);
FSLIB_USE_NAMESPACE(util);

FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, ProxyFileSystem);

#ifdef ENABLE_BEEPER
#define REPORT_OPERATION(level, filePath, format, args...)                                                             \
    do {                                                                                                               \
        kmonitor::MetricsTags kTags;                                                                                   \
        auto handler = MetricReporter::getInstance()->getMetricTagsHandler();                                          \
        if (handler) {                                                                                                 \
            handler->getTags(filePath, kTags);                                                                         \
        }                                                                                                              \
        beeper::EventTags tags(kTags.GetTagsMap());                                                                    \
        BEEPER_FORMAT_REPORT(FSLIB_ERROR_COLLECTOR_NAME, tags, format, args);                                          \
        AUTIL_LOG(level, format, args);                                                                                \
    } while (0)
#define REPORT_REMOVE_SUCCESS_OPERATION(filePath, format, args...)                                                     \
    do {                                                                                                               \
        kmonitor::MetricsTags kTags;                                                                                   \
        auto handler = MetricReporter::getInstance()->getMetricTagsHandler();                                          \
        if (handler) {                                                                                                 \
            handler->getTags(filePath, kTags);                                                                         \
        }                                                                                                              \
        beeper::EventTags tags(kTags.GetTagsMap());                                                                    \
        BEEPER_FORMAT_REPORT(FSLIB_REMOVE_COLLECTOR_NAME, tags, format, args);                                         \
    } while (0)
#else
#define REPORT_OPERATION(level, filePath, format, args...)                                                             \
    do {                                                                                                               \
        kmonitor::MetricsTags kTags;                                                                                   \
        auto handler = MetricReporter::getInstance()->getMetricTagsHandler();                                          \
        if (handler) {                                                                                                 \
            handler->getTags(filePath, kTags);                                                                         \
        }                                                                                                              \
        AUTIL_LOG(level, format, args);                                                                                \
    } while (0)

#define REPORT_REMOVE_SUCCESS_OPERATION(filePath, format, args...)                                                     \
    do {                                                                                                               \
        kmonitor::MetricsTags kTags;                                                                                   \
        auto handler = MetricReporter::getInstance()->getMetricTagsHandler();                                          \
        if (handler) {                                                                                                 \
            handler->getTags(filePath, kTags);                                                                         \
        }                                                                                                              \
    } while (0)
#endif

// typedef FileSystem::LatencyMetricReporter LatencyMetricReporter;

ProxyFileSystem::ProxyFileSystem(AbstractFileSystem *fs) : _fs(fs) {}

ProxyFileSystem::~ProxyFileSystem() { _fs = NULL; }

File *ProxyFileSystem::openFile(const string &fileName, Flag flag) { return openFile(fileName, flag, -1); }

File *ProxyFileSystem::openFile(const string &fileName, Flag flag, ssize_t fileLength) {
    string normPath = PathUtil::normalizePath(fileName);
    if (flag == READ) {
        string data;
        if (FSCacheModule::getInstance()->getFileDataFromCache(normPath, data)) {
            AUTIL_LOG(INFO,
                      "fslib openFile [%s] with READ flag from innerDataCache, len [%lu]",
                      normPath.c_str(),
                      data.size());
            return new MemDataFile(normPath, data);
        }
    }

    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem openFile [%s]", normPath.c_str());
    const char *opType = "open4unknown";
    if (flag == READ) {
        opType = FSLIB_METRIC_TAGS_OPTYPE_OPEN4READ;
    } else if (flag == WRITE) {
        opType = FSLIB_METRIC_TAGS_OPTYPE_OPEN4WRITE;
    } else if (flag == APPEND) {
        opType = FSLIB_METRIC_TAGS_OPTYPE_OPEN4APPEND;
    }
    File *file = NULL;
    {
        LatencyMetricReporter reporter(normPath, opType);
        file = _fs->openFile(normPath, flag, fileLength);
    }
    FileSystem::reportQpsMetric(normPath, opType);
    if (!file || (file->getLastError() != EC_OK && file->getLastError() != EC_NOENT)) {
        FileSystem::reportErrorMetric(normPath, opType);
        REPORT_OPERATION(ERROR, normPath, "fslib openFile [%s][%d] failed.", fileName.c_str(), flag);
    }
    if (file) {
        SpeedController *controller = MultiSpeedController::getInstance()->getReadSpeedController(normPath);
        return new ProxyFile(normPath, file, flag, controller);
    }
    return NULL;
}

MMapFile *ProxyFileSystem::mmapFile(
    const string &fileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset) {
    string normPath = PathUtil::normalizePath(fileName);
    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem mmapFile [%s]", normPath.c_str());
    return _fs->mmapFile(normPath, openMode, start, length, prot, mapFlag, offset);
}

MMapFile *ProxyFileSystem::mmapFile(const string &fileName,
                                    Flag openMode,
                                    char *start,
                                    size_t length,
                                    int prot,
                                    int mapFlag,
                                    off_t offset,
                                    ssize_t fileLength) {
    string normPath = PathUtil::normalizePath(fileName);
    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem mmapFile [%s]", normPath.c_str());
    return _fs->mmapFile(normPath, openMode, start, length, prot, mapFlag, offset, fileLength);
}

ErrorCode ProxyFileSystem::rename(const string &oldFileName, const string &newFileName) {
    assert(_fs);
    string oldNormPath = PathUtil::normalizePath(oldFileName);
    string newNormPath = PathUtil::normalizePath(newFileName);
    FSCacheModule::getInstance()->invalidatePath(oldFileName);

    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem rename [%s] to [%s]", oldNormPath.c_str(), newNormPath.c_str());
    ErrorCode ret;
    {
        LatencyMetricReporter reporter(oldNormPath, FSLIB_METRIC_TAGS_OPTYPE_RENAME);
        ret = _fs->rename(oldNormPath, newNormPath);
    }
    FileSystem::reportQpsMetric(oldNormPath, FSLIB_METRIC_TAGS_OPTYPE_RENAME);
    if (ret != EC_OK && ret != EC_NOENT && ret != EC_EXIST) {
        FileSystem::reportErrorMetric(oldNormPath, FSLIB_METRIC_TAGS_OPTYPE_RENAME);
        REPORT_OPERATION(ERROR,
                         oldNormPath,
                         "fslib rename [%s] to [%s] failed, ec[%d].",
                         oldFileName.c_str(),
                         newFileName.c_str(),
                         ret);
    }
    return ret;
}

ErrorCode ProxyFileSystem::link(const std::string &oldFileName, const std::string &newFileName) {
    assert(_fs);
    string oldNormPath = PathUtil::normalizePath(oldFileName);
    string newNormPath = PathUtil::normalizePath(newFileName);
    FSCacheModule::getInstance()->invalidatePath(oldFileName);

    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem link [%s] to [%s]", oldNormPath.c_str(), newNormPath.c_str());
    ErrorCode ret;
    {
        LatencyMetricReporter reporter(oldNormPath, FSLIB_METRIC_TAGS_OPTYPE_RENAME);
        ret = _fs->link(oldNormPath, newNormPath);
    }
    FileSystem::reportQpsMetric(oldNormPath, FSLIB_METRIC_TAGS_OPTYPE_RENAME);
    if (ret != EC_OK && ret != EC_NOENT && ret != EC_EXIST) {
        FileSystem::reportErrorMetric(oldNormPath, FSLIB_METRIC_TAGS_OPTYPE_RENAME);
        REPORT_OPERATION(ERROR,
                         oldNormPath,
                         "fslib link [%s] to [%s] failed, ec[%d].",
                         oldFileName.c_str(),
                         newFileName.c_str(),
                         ret);
    }
    return ret;
}

ErrorCode ProxyFileSystem::getFileMeta(const string &fileName, FileMeta &fileMeta) {
    assert(_fs);
    string normPath = PathUtil::normalizePath(fileName);
    PathMeta pathMeta;
    ErrorCode ret;
    if (FSCacheModule::getInstance()->needCacheMeta(normPath)) {
        ret = getPathMeta(normPath, pathMeta);
        if (EC_OK == ret) {
            fileMeta.fileLength = pathMeta.length;
            fileMeta.lastModifyTime = pathMeta.lastModifyTime;
            fileMeta.createTime = pathMeta.createTime;
        }
        return ret;
    }

    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem getFileMeta [%s]", normPath.c_str());
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
        ret = _fs->getFileMeta(normPath, fileMeta);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
    if (ret != EC_OK && ret != EC_NOENT) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
        REPORT_OPERATION(ERROR, normPath, "fslib getfilemeta [%s] failed, ec[%d].", fileName.c_str(), ret);
    }
    return ret;
}

ErrorCode ProxyFileSystem::getPathMeta(const string &path, PathMeta &pathMeta) {
    assert(_fs);
    string normPath = PathUtil::normalizePath(path);
    bool matchImmutablePath = false;
    matchImmutablePath = FSCacheModule::getInstance()->matchImmutablePath(normPath);
    if (FSCacheModule::getInstance()->getPathMeta(normPath, pathMeta)) {
        return EC_OK;
    } else if (matchImmutablePath) {
        return EC_NOENT;
    }

    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem getPathMeta [%s]", normPath.c_str());
    ErrorCode ret;
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
        ret = _fs->getPathMeta(normPath, pathMeta);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
    if (ret == EC_OK) {
        FSCacheModule::getInstance()->addPathMeta(
            normPath, pathMeta.length, pathMeta.createTime, pathMeta.lastModifyTime, !pathMeta.isFile);
    } else if (ret != EC_NOENT) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
        REPORT_OPERATION(ERROR, normPath, "fslib getpathmeta [%s] failed, ec[%d].", path.c_str(), ret);
    }
    return ret;
}

ErrorCode ProxyFileSystem::isFile(const string &path) {
    assert(_fs);
    string normPath = PathUtil::normalizePath(path);
    ErrorCode ret;
    PathMeta pathMeta;
    if (FSCacheModule::getInstance()->needCacheMeta(normPath)) {
        ret = getPathMeta(normPath, pathMeta);
        if (EC_OK == ret) {
            return pathMeta.isFile ? EC_TRUE : EC_FALSE;
        }
        return ret;
    }

    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem isFile [%s]", normPath.c_str());
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
        ret = _fs->isFile(normPath);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
    if (ret != EC_TRUE && ret != EC_FALSE && ret != EC_OK) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
        REPORT_OPERATION(WARN, normPath, "fslib isFile [%s] failed, ec[%d].", path.c_str(), ret);
    }
    return ret;
}

FileChecksum ProxyFileSystem::getFileChecksum(const string &fileName) {
    assert(_fs);
    return _fs->getFileChecksum(fileName);
}

ErrorCode ProxyFileSystem::mkDir(const string &dirName, bool recursive) {
    assert(_fs);
    string normPath = PathUtil::normalizePath(dirName);
    PathMeta pathMeta;
    if (FSCacheModule::getInstance()->getPathMeta(normPath, pathMeta)) {
        return EC_EXIST;
    }

    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem mkDir [%s]", normPath.c_str());
    ErrorCode ret;
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_MKDIR);
        ret = _fs->mkDir(normPath, recursive);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_MKDIR);
    if (ret == EC_OK) {
        int64_t ts = TimeUtility::currentTimeInSeconds();
        FSCacheModule::getInstance()->addPathMeta(normPath, 0, ts, ts, true);
    } else if (ret != EC_EXIST) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_MKDIR);
        REPORT_OPERATION(ERROR, normPath, "fslib mkdir [%s] failed, ec[%d].", dirName.c_str(), ret);
    }
    return ret;
}

ErrorCode ProxyFileSystem::listDir(const string &dirName, FileList &fileList) {
    assert(_fs);
    string normPath = PathUtil::normalizePath(dirName);
    vector<pair<string, PathMeta>> subPathMetas;
    if (FSCacheModule::getInstance()->listInImmutablePath(normPath, subPathMetas, false)) {
        for (auto p : subPathMetas) {
            fileList.push_back(p.first);
        }
        return EC_OK;
    }

    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem listDir [%s]", normPath.c_str());
    ErrorCode ret;
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_LIST);
        ret = _fs->listDir(normPath, fileList);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_LIST);
    if (ret != EC_OK && ret != EC_NOENT) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_LIST);
        REPORT_OPERATION(ERROR, normPath, "fslib listDir [%s] failed, ec[%d].", dirName.c_str(), ret);
    }
    return ret;
}

ErrorCode ProxyFileSystem::listDir(const std::string &dirName, RichFileList &fileList) {
    assert(_fs);
    string normPath = PathUtil::normalizePath(dirName);
    vector<pair<string, PathMeta>> subPathMetas;

    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem listDir [%s]", normPath.c_str());
    ErrorCode ret;
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_LIST);
        ret = _fs->listDir(normPath, fileList);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_LIST);
    if (ret != EC_OK && ret != EC_NOENT) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_LIST);
        REPORT_OPERATION(ERROR, normPath, "fslib listDir [%s] failed, ec[%d].", dirName.c_str(), ret);
    }
    return ret;
}

ErrorCode ProxyFileSystem::listDir(const string &dirName, EntryList &entryList) {
    string normPath = PathUtil::normalizePath(dirName);
    vector<pair<string, PathMeta>> subPathMetas;
    if (FSCacheModule::getInstance()->listInImmutablePath(normPath, subPathMetas, false)) {
        for (auto p : subPathMetas) {
            EntryMeta eMeta;
            eMeta.isDir = !p.second.isFile;
            eMeta.entryName = p.first;
            entryList.push_back(eMeta);
        }
        return EC_OK;
    }

    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem listDir [%s]", normPath.c_str());
    ErrorCode ret;
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_LIST);
        ret = _fs->listDir(normPath, entryList);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_LIST);
    if (ret != EC_OK && ret != EC_NOENT) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_LIST);
        REPORT_OPERATION(ERROR, normPath, "fslib listDir [%s] failed, ec[%d].", dirName.c_str(), ret);
    }
    return ret;
}

ErrorCode ProxyFileSystem::isDirectory(const string &path) {
    string normPath = PathUtil::normalizePath(path);
    ErrorCode ret;
    PathMeta pathMeta;
    if (FSCacheModule::getInstance()->needCacheMeta(normPath)) {
        ret = getPathMeta(normPath, pathMeta);
        if (EC_OK == ret) {
            return pathMeta.isFile ? EC_FALSE : EC_TRUE;
        }
        return ret;
    }

    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem isDirectory [%s]", normPath.c_str());
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
        ret = _fs->isDirectory(normPath);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
    if (ret != EC_TRUE && ret != EC_FALSE && ret != EC_OK && ret != EC_NOENT) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
        REPORT_OPERATION(ERROR, normPath, "fslib isDir [%s] failed, ec[%d].", path.c_str(), ret);
    }
    return ret;
}

ErrorCode ProxyFileSystem::remove(const string &path) {
    string normPath = PathUtil::normalizePath(path);
    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem remove [%s]", normPath.c_str());
    ErrorCode ret;
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_DELETE);
        ret = _fs->remove(normPath);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_DELETE);
    if (ret != EC_OK && ret != EC_NOENT) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_DELETE);
        REPORT_OPERATION(ERROR, normPath, "fslib remove [%s] failed, ec[%d].", path.c_str(), ret);
    }

    if (ret == EC_OK || ret == EC_NOENT) {
        FSCacheModule::getInstance()->invalidatePath(path);
    }

    if (ret == EC_OK) {
        REPORT_REMOVE_SUCCESS_OPERATION(normPath, "fslib remove [%s] success.", path.c_str());
    }
    return ret;
}

ErrorCode ProxyFileSystem::removeFile(const string &path) {
    string normPath = PathUtil::normalizePath(path);
    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem remove file [%s]", normPath.c_str());
    ErrorCode ret;
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_DELETE_FILE);
        ret = _fs->removeFile(normPath);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_DELETE_FILE);
    if (ret != EC_OK && ret != EC_NOENT) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_DELETE_FILE);
        REPORT_OPERATION(ERROR, normPath, "fslib remove file [%s] failed, ec[%d].", path.c_str(), ret);
    }

    if (ret == EC_OK || ret == EC_NOENT) {
        FSCacheModule::getInstance()->invalidatePath(path);
    }

    if (ret == EC_OK) {
        REPORT_REMOVE_SUCCESS_OPERATION(normPath, "fslib removeFile [%s] success.", path.c_str());
    }
    return ret;
}

ErrorCode ProxyFileSystem::removeDir(const string &path) {
    string normPath = PathUtil::normalizePath(path);
    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem remove dir [%s]", normPath.c_str());
    ErrorCode ret;
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_DELETE_DIR);
        ret = _fs->removeDir(normPath);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_DELETE_DIR);
    if (ret != EC_OK && ret != EC_NOENT) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_DELETE_DIR);
        REPORT_OPERATION(ERROR, normPath, "fslib remove dir [%s] failed, ec[%d].", path.c_str(), ret);
    }

    if (ret == EC_OK || ret == EC_NOENT) {
        FSCacheModule::getInstance()->invalidatePath(path);
    }

    if (ret == EC_OK) {
        REPORT_REMOVE_SUCCESS_OPERATION(normPath, "fslib removeDir [%s] success.", path.c_str());
    }
    return ret;
}

ErrorCode ProxyFileSystem::isExist(const string &path) {
    string normPath = PathUtil::normalizePath(path);
    ErrorCode ret;
    PathMeta pathMeta;
    if (FSCacheModule::getInstance()->needCacheMeta(normPath)) {
        ret = getPathMeta(normPath, pathMeta);
        if (ret == EC_OK) {
            return EC_TRUE;
        }
        if (ret == EC_NOENT) {
            return EC_FALSE;
        }
        return ret;
    }

    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem isExist [%s]", normPath.c_str());
    {
        LatencyMetricReporter reporter(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
        ret = _fs->isExist(normPath);
    }
    FileSystem::reportQpsMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
    if (ret != EC_TRUE && ret != EC_FALSE && ret != EC_OK) {
        FileSystem::reportErrorMetric(normPath, FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA);
        REPORT_OPERATION(ERROR, normPath, "fslib isExist [%s] failed, ec[%d].", path.c_str(), ret);
    }
    return ret;
}

FileReadWriteLock *ProxyFileSystem::createFileReadWriteLock(const string &fileName) {
    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem createFileReadWriteLock [%s]", fileName.c_str());
    FileSystem::reportQpsMetric(fileName, FSLIB_METRIC_TAGS_OPTYPE_CREATE_READWRITE_LOCK);
    return _fs->createFileReadWriteLock(fileName);
}

FileLock *ProxyFileSystem::createFileLock(const string &fileName) {
    assert(_fs);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem createFileLock [%s]", fileName.c_str());
    FileSystem::reportQpsMetric(fileName, FSLIB_METRIC_TAGS_OPTYPE_CREATE_FILELOCK);
    return _fs->createFileLock(fileName);
}

FileSystemCapability ProxyFileSystem::getCapability() const {
    assert(_fs);
    return _fs->getCapability();
}

ErrorCode ProxyFileSystem::forward(const string &command, const string &path, const string &args, string &output) {
    assert(_fs);
    string normPath = PathUtil::normalizePath(path);
    STACK_TRACER_LOG(INFO, "FSLIB_FileSystem forward [%s]", normPath.c_str());
    FSCacheModule::getInstance()->invalidatePath(normPath);
    return _fs->forward(command, normPath, args, output);
}

AbstractFileSystem *ProxyFileSystem::getFs() const { return _fs; }

FSLIB_END_NAMESPACE(fs);
