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
#ifdef ENABLE_BEEPER
#include "beeper/beeper.h"
#endif

#include <cstdarg>

#include "autil/EnvUtil.h"
#include "autil/NetUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/cache/FSCacheModule.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/MetricReporter.h"
#include "fslib/fs/ProxyFile.h"
#include "fslib/util/MetricTagsHandler.h"

using namespace std;
FSLIB_USE_NAMESPACE(cache);

FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, ProxyFile);

#ifdef ENABLE_BEEPER
#define REPORT_DN_ERROR(filePath, format, args...)                                                                     \
    do {                                                                                                               \
        kmonitor::MetricsTags kTags;                                                                                   \
        auto handler = MetricReporter::getInstance()->getMetricTagsHandler();                                          \
        if (handler) {                                                                                                 \
            handler->getTags(filePath, kTags);                                                                         \
        }                                                                                                              \
        beeper::EventTags tags(kTags.GetTagsMap());                                                                    \
        BEEPER_FORMAT_REPORT(FSLIB_ERROR_COLLECTOR_NAME, tags, format, args);                                          \
        AUTIL_LOG(ERROR, format, args);                                                                                \
    } while (0)
#else
#define REPORT_DN_ERROR(filePath, format, args...)                                                                     \
    do {                                                                                                               \
        kmonitor::MetricsTags kTags;                                                                                   \
        auto handler = MetricReporter::getInstance()->getMetricTagsHandler();                                          \
        if (handler) {                                                                                                 \
            handler->getTags(filePath, kTags);                                                                         \
        }                                                                                                              \
        AUTIL_LOG(ERROR, format, args);                                                                                \
    } while (0)
#endif

ProxyFile::ProxyFile(const string &filePath, File *file, Flag flag, SpeedController *controller)
    : File(file->getFileName(), file->getLastError())
    , _file(file)
    , _filePath(filePath)
    , _flag(flag)
    , _writeLen(0)
    , _controller(controller) {
    if (_flag == WRITE) {
        string tmp = autil::EnvUtil::getEnv("FSLIB_ENABLE_PRINT_FILE_WRITE_INFO");
        if (tmp == "true") {
            int64_t createTimestamp = autil::TimeUtility::currentTime();
            _md5Stream.reset(new Md5Stream);
            AUTIL_LOG(INFO,
                      "[FSLIB_INFO] operate_type=[CREATE] path=[%s] timestamp=[%ld] ip=[%s] host=[%s]",
                      _filePath.c_str(),
                      createTimestamp,
                      GetIp().c_str(),
                      GetHostName().c_str());
        }
    }
}

ProxyFile::~ProxyFile() {
    if (_file->isOpened()) {
        close();
    }
    DELETE_AND_SET_NULL(_file);
}

ssize_t ProxyFile::read(void *buffer, size_t length) {
    assert(_file);
    ssize_t readTotal = 0;
    ssize_t actualRead = 0;
    size_t leftLen = length;
    {
        ReaderLatencyMetricReporter reporter(_filePath);
        while (readTotal < (ssize_t)length) {
            int64_t quotaRead = _controller->reserveQuota(leftLen);
            assert(quotaRead > 0);
            actualRead = _file->read((uint8_t *)buffer + readTotal, quotaRead);
            if (unlikely(actualRead < 0)) {
                FileSystem::reportDNReadErrorQps(_filePath);
                REPORT_DN_ERROR(_filePath, "fslib readFile [%s] failed.", _filePath.c_str());
                return actualRead;
            }
            readTotal += actualRead;
            leftLen -= actualRead;
            FileSystem::reportDNReadSpeed(_filePath, actualRead);
            if (actualRead < quotaRead) {
                break;
            }
        }
        reporter.setLength(readTotal);
    }

    return readTotal;
}

ssize_t ProxyFile::write(const void *buffer, size_t length) {
    assert(_file);
    ssize_t ret;
    {
        WriterLatencyMetricReporter reporter(_filePath);
        ret = _file->write(buffer, length);
        reporter.setLength(ret);
    }

    if (ret < 0) {
        FileSystem::reportDNWriteErrorQps(_filePath);
        REPORT_DN_ERROR(_filePath, "fslib writeFile [%s] failed.", _filePath.c_str());
    } else {
        FileSystem::reportDNWriteSpeed(_filePath, length);
        if (_md5Stream) {
            _md5Stream->Put((const uint8_t *)buffer, length);
            _writeLen += length;
        }
    }
    return ret;
}

ssize_t ProxyFile::pread(void *buffer, size_t length, off_t offset) {
    assert(_file);
    ssize_t readTotal = 0;
    ssize_t actualRead = 0;
    size_t leftLen = length;
    {
        ReaderLatencyMetricReporter reporter(_filePath);
        while (readTotal < (ssize_t)length) {
            int64_t quotaRead = _controller->reserveQuota(leftLen);
            assert(quotaRead > 0);
            actualRead = _file->pread((uint8_t *)buffer + readTotal, quotaRead, offset + readTotal);
            if (unlikely(actualRead < 0)) {
                FileSystem::reportDNReadErrorQps(_filePath);
                REPORT_DN_ERROR(_filePath, "fslib readFile [%s] failed.", _filePath.c_str());
                return actualRead;
            }
            readTotal += actualRead;
            leftLen -= actualRead;
            FileSystem::reportDNReadSpeed(_filePath, actualRead);
            if (actualRead < quotaRead) {
                break;
            }
        }
        reporter.setLength(readTotal);
    }
    return readTotal;
}

#if (__cplusplus >= 201703L)
void ProxyFile::pread(IOController *controller, void *buffer, size_t length, off_t offset, function<void()> callback) {
    assert(_file);
    // TODO add metric
    return _file->pread(controller, buffer, length, offset, callback);
}
#endif

ssize_t ProxyFile::preadv(const iovec *iov, int iovcnt, off_t offset) {
    // TODO add metric
    //  attention : think more when reporting metrics, down frame may call pread interface directly
    assert(_file);
    return _file->preadv(iov, iovcnt, offset);
}

#if (__cplusplus >= 201703L)
void ProxyFile::preadv(
    IOController *controller, const iovec *iov, int iovcnt, off_t offset, function<void()> callback) {
    // TODO add metric
    //  attention : think more when reporting metrics, down frame may call pread interface directly
    assert(_file);
    return _file->preadv(controller, iov, iovcnt, offset, callback);
}
#endif

ssize_t ProxyFile::pwrite(const void *buffer, size_t length, off_t offset) {
    assert(_file);
    if (_md5Stream) {
        _md5Stream.reset();
    }
    ssize_t ret;
    {
        WriterLatencyMetricReporter reporter(_filePath);
        ret = _file->pwrite(buffer, length, offset);
        reporter.setLength(ret);
    }
    if (ret < 0) {
        FileSystem::reportDNWriteErrorQps(_filePath);
        REPORT_DN_ERROR(_filePath, "fslib writeFile [%s] failed.", _filePath.c_str());
    } else {
        FileSystem::reportDNWriteSpeed(_filePath, length);
    }
    return ret;
}

#if (__cplusplus >= 201703L)
void ProxyFile::pwrite(IOController *controller, void *buffer, size_t length, off_t offset, function<void()> callback) {
    assert(_file);
    // TODO add metric
    return _file->pwrite(controller, buffer, length, offset, callback);
}

void ProxyFile::pwritev(
    IOController *controller, const iovec *iov, int iovcnt, off_t offset, function<void()> callback) {
    // TODO add metric
    //  attention : think more when reporting metrics, down frame may call pread interface directly
    assert(_file);
    return _file->pwritev(controller, iov, iovcnt, offset, callback);
}
#endif

ErrorCode ProxyFile::ioctlImpl(IOCtlRequest request, va_list args) { return _file->ioctlImpl(request, args); }

ErrorCode ProxyFile::getLastError() const {
    assert(_file);
    return _file->getLastError();
}

ErrorCode ProxyFile::flush() {
    assert(_file);
    ErrorCode ec = EC_OK;
    {
        LatencyMetricReporter reporter(_filePath, FSLIB_METRIC_TAGS_OPTYPE_FLUSH);
        ec = _file->flush();
    }
    if (ec != EC_OK) {
        FileSystem::reportDNErrorQps(_filePath, FSLIB_METRIC_TAGS_OPTYPE_FLUSH);
        REPORT_DN_ERROR(_filePath, "fslib flush [%s] failed, ec[%d].", _filePath.c_str(), ec);
    }
    FileSystem::reportQpsMetric(_filePath, FSLIB_METRIC_TAGS_OPTYPE_FLUSH);
    return ec;
}

ErrorCode ProxyFile::sync() {
    assert(_file);
    ErrorCode ec = EC_OK;
    {
        LatencyMetricReporter reporter(_filePath, FSLIB_METRIC_TAGS_OPTYPE_SYNC);
        ec = _file->sync();
    }
    if (ec != EC_OK) {
        FileSystem::reportDNErrorQps(_filePath, FSLIB_METRIC_TAGS_OPTYPE_SYNC);
        REPORT_DN_ERROR(_filePath, "fslib sync [%s] failed, ec[%d].", _filePath.c_str(), ec);
    }
    FileSystem::reportQpsMetric(_filePath, FSLIB_METRIC_TAGS_OPTYPE_SYNC);
    return ec;
}

ErrorCode ProxyFile::close() {
    assert(_file);
    const char *opType = FSLIB_METRIC_TAGS_OPTYPE_CLOSE4WRITE;
    if (_flag == READ) {
        opType = FSLIB_METRIC_TAGS_OPTYPE_CLOSE4READ;
        FSCacheModule::getInstance()->putFileDataToCache(_filePath, _file);
    }

    ErrorCode ec = EC_OK;
    {
        LatencyMetricReporter reporter(_filePath, opType);
        ec = _file->close();
    }
    if (ec != EC_OK) {
        FileSystem::reportDNErrorQps(_filePath, opType);
        REPORT_DN_ERROR(_filePath, "fslib close [%s] failed, ec[%d].", _filePath.c_str(), ec);
    }
    if (_md5Stream && ec == EC_OK) {
        int64_t modifyTimestamp = autil::TimeUtility::currentTime();
        AUTIL_LOG(INFO,
                  "[FSLIB_INFO] operate_type=[CLOSE] path=[%s] timestamp=[%ld] md5=[%s] ip=[%s] host=[%s]",
                  _filePath.c_str(),
                  modifyTimestamp,
                  GetMD5String().c_str(),
                  GetIp().c_str(),
                  GetHostName().c_str());
        _md5Stream.reset();
    }
    FileSystem::reportQpsMetric(_filePath, opType);
    return ec;
}

ErrorCode ProxyFile::seek(int64_t offset, SeekFlag flag) {
    assert(_file);
    ErrorCode ec = _file->seek(offset, flag);
    if (ec != EC_OK) {
        FileSystem::reportDNErrorQps(_filePath, FSLIB_METRIC_TAGS_OPTYPE_SEEK);
        REPORT_DN_ERROR(_filePath, "fslib seek [%s] failed, ec[%d].", _filePath.c_str(), ec);
    }
    return ec;
}

int64_t ProxyFile::tell() {
    assert(_file);
    return _file->tell();
}

bool ProxyFile::isOpened() const {
    assert(_file);
    return _file->isOpened();
}

bool ProxyFile::isEof() {
    assert(_file);
    return _file->isEof();
}

string ProxyFile::GetMD5String() const {
    if (!_md5Stream) {
        return string("");
    }
    return _md5Stream->GetMd5String();
}

string ProxyFile::GetIp() const {
    string ip;
    if (autil::NetUtil::GetDefaultIp(ip)) {
        return ip;
    }
    return string("");
}

string ProxyFile::GetHostName() const {
    string hostname;
    if (autil::NetUtil::GetHostName(hostname)) {
        return hostname;
    }
    return string("");
}

FSLIB_END_NAMESPACE(fs);
