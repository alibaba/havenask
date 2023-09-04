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
#include "fslib/fs/ErrorGenerator.h"

#include <fstream>
#include <sys/stat.h>

#include "autil/EnvUtil.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "fslib/common/common_define.h"
#include "fslib/util/SafeBuffer.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

FSLIB_USE_NAMESPACE(util);

FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, ErrorGenerator);

const size_t ErrorGenerator::NO_EXCEPTION_COUNT = 1000000000;

bool ErrorTrigger::triggerError() {
    ScopedLock lock(mLock);
    if (mIOCount >= mNormalIOCount) {
        return true;
    }
    mIOCount++;
    return false;
}

ErrorGenerator::ErrorGenerator() : _needGenerateError(false) { init(); }

ErrorGenerator::~ErrorGenerator() {}

ErrorGenerator *ErrorGenerator::getInstance() {
    static ErrorGenerator *ptr = 0;
    static autil::ThreadMutex gLock;

    if (!ptr) {
        autil::ScopedLock lock(gLock);
        if (!ptr) {
            ptr = new ErrorGenerator;
            static std::unique_ptr<ErrorGenerator> destroyer(ptr);
        }
    }
    return const_cast<ErrorGenerator *>(ptr);
}

void ErrorGenerator::clearMethodVisitCounter() {
    autil::ScopedLock lock(mMutex);
    _methodVisitCounter.clear();
}

ErrorGenerator::MethodVisitCount ErrorGenerator::getMethodVisitCounter() {
    autil::ScopedLock lock(mMutex);
    return _methodVisitCounter;
}

void ErrorGenerator::setErrorMap(FileSystemErrorMap map) {
    autil::ScopedLock lock(mMutex);
    _fileSystemErrorMap = map;
}

void ErrorGenerator::init() {
    string errorFile = autil::EnvUtil::getEnv(FSLIB_ERROR_CONFIG_PATH);
    if (errorFile.empty()) {
        AUTIL_LOG(INFO, "Environment variable FSLIB_ERROR_CONFIG_PATH not set.");
        return;
    }
    ifstream fin(errorFile);

    if (!fin.good()) {
        cerr << "load FSLIB_ERROR_CONFIG_PATH:" << errorFile << " FAILED" << endl;
        return;
    }
    string path;
    while (fin >> path) {
        string errStr("");
        getline(fin, errStr, ';');
        string method;
        FileSystemError fsError;
        bool ret = parseErrorString(errStr, method, fsError);
        if (!ret) {
            cerr << "parse error string FAILED: " << errStr << endl;
            _fileSystemErrorMap.clear();
            return;
        }
        _fileSystemErrorMap[make_pair(path, method)] = fsError;
    }
}

ErrorCode ErrorGenerator::generateFileSystemError(const string &operate, const string &targetPath) {
    if (_errorTrigger && _errorTrigger->triggerError()) {
        return EC_UNKNOWN;
    }
    FileSystemErrorMap::const_iterator opIt;
    string parsePath = targetPath;
    {
        autil::ScopedLock lock(mMutex);
        MethodVisitCount::iterator it = _methodVisitCounter.find(make_pair(parsePath, operate));
        if (it == _methodVisitCounter.end()) {
            _methodVisitCounter[make_pair(parsePath, operate)] = 1;
        } else {
            it->second++;
        }
        opIt = getError(operate, parsePath);
        if (opIt == _fileSystemErrorMap.end()) {
            return EC_OK;
        }
    }
    const FileSystemError &fsError = opIt->second;
    doDelay(fsError.delay);
    if (fsError.until > 0 && !reachUntil(parsePath, fsError.until)) {
        return EC_OK;
    }
    if (fsError.retryCount > 0 && !needRetry(parsePath, fsError.retryCount)) {
        return EC_OK;
    }
    AUTIL_LOG(INFO, "file [%s] operate [%s] trigger error", targetPath.c_str(), operate.c_str());
    return fsError.ec;
}

ErrorCode ErrorGenerator::generateFileError(const string &operate, const string &targetPath, uint64_t offset) {
    if (_errorTrigger && _errorTrigger->triggerError()) {
        return EC_UNKNOWN;
    }
    FileSystemErrorMap::const_iterator opIt;
    string parsePath = targetPath;
    {
        autil::ScopedLock lock(mMutex);
        MethodVisitCount::iterator it = _methodVisitCounter.find(make_pair(parsePath, operate));
        if (it == _methodVisitCounter.end()) {
            _methodVisitCounter[make_pair(parsePath, operate)] = 1;
        } else {
            it->second++;
        }
        opIt = getError(operate, parsePath);
        if (opIt == _fileSystemErrorMap.end()) {
            return EC_OK;
        }
    }
    const FileSystemError &fsError = opIt->second;
    doDelay(fsError.delay);
    if (fsError.until > 0 && !reachUntil(parsePath, fsError.until)) {
        return EC_OK;
    }
    if (fsError.retryCount > 0 && !needRetry(parsePath, fsError.retryCount)) {
        return EC_OK;
    }
    if (offset >= fsError.offset) {
        AUTIL_LOG(INFO, "file [%s] operate [%s] trigger error", targetPath.c_str(), operate.c_str());
        return fsError.ec;
    }
    return EC_OK;
}

void ErrorGenerator::doDelay(uint64_t delay) {
    if (delay > 0) {
        usleep(delay * 1000);
    }
}

void ErrorGenerator::resetPathVisitCount() {
    autil::ScopedLock lock(mMutex);
    _pathVisitCount.clear();
}

bool ErrorGenerator::needRetry(const string &parsePath, uint32_t retry) {
    autil::ScopedLock lock(mMutex);
    if ((_pathVisitCount[parsePath] % retry) == 0) {
        return false;
    }
    return true;
}

bool ErrorGenerator::reachUntil(const string &parsePath, uint32_t until) {
    autil::ScopedLock lock(mMutex);
    if (_pathVisitCount[parsePath] != until) {
        return false;
    }
    return true;
}

ErrorGenerator::FileSystemErrorMap::const_iterator ErrorGenerator::getError(const string &operate,
                                                                            const string &parsePath) {
    FileSystemErrorMap::const_iterator opIt = _fileSystemErrorMap.find(make_pair(parsePath, operate));
    if (opIt == _fileSystemErrorMap.end()) {
        return _fileSystemErrorMap.end();
    }

    PathVisitCount::iterator it = _pathVisitCount.find(parsePath);
    if (it == _pathVisitCount.end()) {
        _pathVisitCount[parsePath] = 1;
    } else {
        it->second++;
    }

    return opIt;
}

bool ErrorGenerator::parseErrorString(const string &errStr, string &method, FileSystemError &fsError) const {
    StringTokenizer st(errStr, " ", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        const string &error = st[i];
        size_t pos = error.find('=');
        if (pos == string::npos) {
            return false;
        }
        string type = error.substr(0, pos);
        string value = error.substr(pos + 1);
        if (type == ERROR_TYPE_METHOD) {
            method = value;
        } else if (type == ERROR_TYPE_ERROR_CODE) {
            int ec;
            if (!StringUtil::strToInt32(value.c_str(), ec)) {
                return false;
            }
            fsError.ec = (ErrorCode)ec;
        } else if (type == ERROR_TYPE_OFFSET) {
            if (!StringUtil::strToUInt64(value.c_str(), fsError.offset)) {
                return false;
            }
        } else if (type == ERROR_TYPE_DELAY) {
            if (!StringUtil::strToUInt64(value.c_str(), fsError.delay)) {
                return false;
            }
        } else if (type == ERROR_TYPE_RETRY) {
            if (!StringUtil::strToUInt32(value.c_str(), fsError.retryCount)) {
                return false;
            }
        }
    }
    return true;
}

FSLIB_END_NAMESPACE(fs);
