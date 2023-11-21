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
#include <unistd.h>
#include <limits>
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "access_log/local/LocalAccessLogReader.h"

using namespace std;
using namespace fslib;
using namespace autil;

AUTIL_DECLARE_AND_SETUP_LOGGER(access_log, LocalAccessLogReader);

namespace access_log {

static const string RELATIVE_LOG_DIR = "logs/";
static const string ACCESS_LOG = "access.log";

LocalAccessLogReader::LocalAccessLogReader(const string &logName, const string &logDir)
    : AccessLogReader(logName), _logDir(logDir), _fileOffset(0) {}

LocalAccessLogReader::LocalAccessLogReader(const std::string &logName, std::vector<fslib::fs::File *> logHandlers)
    : AccessLogReader(logName), _fileOffset(0), _logHanlers(std::move(logHandlers)) {}

LocalAccessLogReader::~LocalAccessLogReader() { releaseResource(); }

bool LocalAccessLogReader::init() {
    _logFileNamePrefix = ACCESS_LOG;
    if (!_logDir.empty()) {
        return true;
    }

    // if logDir not specified, use the logs dir in current dir
    char curDir[FILENAME_MAX];
    if (!getcwd(curDir, FILENAME_MAX)) {
        AUTIL_LOG(ERROR,
                  "LocalAccessLogReader init error, "
                  "get current work dir failed");
        return false;
    }

    _logDir = fslib::fs::FileSystem::joinFilePath(string(curDir), RELATIVE_LOG_DIR);

    return true;
}

bool LocalAccessLogReader::initLogHandler() {
    releaseResource();

    FileList fileList;
    getLocalFileNames(_logFileNamePrefix, fileList);
    if (fileList.size() < 1) {
        AUTIL_LOG(INFO, "get local file [%s] list", _logDir.c_str());
        return false;
    }

    size_t fileSize = fileList.size();
    for (int32_t i = fileSize - 1; i >= 0; i--) {
        string completePath = fs::FileSystem::joinFilePath(_logDir, fileList[i]);
        fs::File *input = fs::FileSystem::openFile(completePath, fslib::Flag::READ);
        if (!input->isOpened()) {
            AUTIL_LOG(WARN, "open file [%s] failed", completePath.c_str());
            delete input;
            continue;
        }
        _logHanlers.push_back(input);
    }
    _fileOffset = 0;
    return true;
}

void LocalAccessLogReader::releaseResource() {
    for (auto &handler : _logHanlers) {
        handler->close();
        delete handler;
    }
    _logHanlers.clear();
}

fs::File *LocalAccessLogReader::getCurHandler() {
    if (_fileOffset >= _logHanlers.size()) {
        AUTIL_LOG(WARN, "read end, no more log");
        return nullptr;
    }
    return _logHanlers[_fileOffset];
}

bool LocalAccessLogReader::read(uint32_t count, vector<PbAccessLog> &accessLogs,
                                int32_t statusCode) {
    if (_logHanlers.empty()) {
        if (!initLogHandler()) {
            return false;
        }
    }

    auto curHandler = getCurHandler();
    if (!curHandler) {
        return true;
    }

#define GET_NEXT_HANDLER()            \
    {                                 \
        _fileOffset++;                \
        curHandler = getCurHandler(); \
        if (!curHandler) {            \
            return true;              \
        }                             \
    }

    uint32_t retryTimes = 0;
    while (accessLogs.size() < count) {
        if (curHandler->isEof()) {
            GET_NEXT_HANDLER();
        }
        PbAccessLog pbLog;
        if (!readOneLog(curHandler, pbLog)) {
            GET_NEXT_HANDLER();
            if (++retryTimes >= AccessLogReader::MAX_RETRY_TIME) {
                AUTIL_LOG(ERROR, "retry read log from local exceed MAX_RETRY_TIME.");
                return false;
            }
            continue;
        }
        if (pbLog.logname() == _logName &&
            (statusCode == std::numeric_limits<int32_t>::min() || pbLog.statuscode() == statusCode)) {
            accessLogs.push_back(pbLog);
        }
    }
#undef GET_NEXT_HANDLER
    return true;
}

bool LocalAccessLogReader::readCustomLog(uint32_t count, vector<string> &customLogs) {
    if (_logHanlers.empty()) {
        if (!initLogHandler()) {
            AUTIL_LOG(INFO, "init log handler failed");
            return false;
        }
    }

    auto curHandler = getCurHandler();
    if (!curHandler) {
        return true;
    }

#define GET_NEXT_HANDLER()            \
    {                                 \
        _fileOffset++;                \
        curHandler = getCurHandler(); \
        if (!curHandler) {            \
            return true;              \
        }                             \
    }

    uint32_t retryTimes = 0;
    while (customLogs.size() < count) {
        if (curHandler->isEof()) {
            GET_NEXT_HANDLER();
        }
        string rawLog;
        if (!readRawLog(curHandler, rawLog)) {
            GET_NEXT_HANDLER();
            if (++retryTimes >= AccessLogReader::MAX_RETRY_TIME) {
                AUTIL_LOG(ERROR, "retry read log from local exceed MAX_RETRY_TIME.");
                return false;
            }
            continue;
        }
        customLogs.push_back(rawLog);
    }
#undef GET_NEXT_HANDLER
    return true;
}

bool LocalAccessLogReader::readRawLog(fslib::fs::File *curHandler, std::string &rawStr) {
    char header[LOG_HEADER_SIZE];

    auto ret = curHandler->read(header, LOG_HEADER_SIZE);
    if (LenType(ret) < LOG_HEADER_SIZE) {
        AUTIL_LOG(WARN, "read log header from file failed[%d]<[%d]", LenType(ret), LOG_HEADER_SIZE);
        return false;
    }

    int64_t logLength = AccessLogReader::parseHeader(header);
    if (logLength == -1) {
        AUTIL_LOG(WARN, "parse head failed");
        return false;
    }

    char *log = new char[logLength];
    ret = curHandler->read(log, logLength);
    if (ret < logLength) {
        AUTIL_LOG(ERROR, "read log file failed");
        return false;
    }

    rawStr.assign(log, logLength);
    delete[] log;
    return true;
}

bool LocalAccessLogReader::readOneLog(fs::File *curHandler, PbAccessLog &pbLog) {
    string logStr;
    if (!readRawLog(curHandler, logStr)) {
        return false;
    }
    return pbLog.ParseFromString(logStr);
}

void LocalAccessLogReader::getLocalFileNames(
        const std::string &logFileNamePrefix, FileList &fileList)
{
    FileList tmpList;
    fslib::ErrorCode ec = fslib::fs::FileSystem::listDir(_logDir, tmpList);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "list log dir [%s] failed.", _logDir.c_str());
        return;
    }

    bool latestLogExist = false;
    for (const auto &fileName : tmpList) {
        if (fileName == logFileNamePrefix) {
            latestLogExist = true;
        } else if (StringUtil::startsWith(fileName, logFileNamePrefix)) {
            fileList.push_back(fileName);
        }
    }
    sort(fileList.begin(), fileList.end());
    if (latestLogExist) {
        fileList.push_back(logFileNamePrefix);
    }
}

void LocalAccessLogReader::getLocalFullPathFileNames(
        const std::string &logFileNamePrefix, FileList &fileList)
{
    FileList localFileNames;
    getLocalFileNames(logFileNamePrefix, localFileNames);
    if (localFileNames.size() > 0) {
        fileList.emplace_back(fs::FileSystem::joinFilePath(_logDir, localFileNames[0]));
        for (size_t i = localFileNames.size() - 1; i > 0; --i) {
            fileList.emplace_back(fs::FileSystem::joinFilePath(_logDir, localFileNames[i]));
        }
    }
}

}
