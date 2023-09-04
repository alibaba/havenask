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
#include "swift/broker/storage_dfs/FileManager.h"

#include <algorithm>
#include <assert.h>
#include <memory>
#include <ostream>
#include <stdio.h>
#include <utility>

#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "fslib/fs/FileSystem.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/FileManagerUtil.h"
#include "swift/util/PanguInlineFileUtil.h"

using namespace std;
using namespace fslib::fs;
using namespace autil;
using namespace swift::protocol;
using namespace swift::util;

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, FileManager);

FileManager::FileManager() : _lastDelFileTime(0) {}
FileManager::~FileManager() {}

int64_t FileManager::getLastMessageId() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    if (_filePairVec.empty()) {
        return -1;
    } else {
        return _filePairVec.back()->getEndMessageId() - 1;
    }
}

int64_t FileManager::getMinMessageId() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    if (_filePairVec.empty()) {
        return -1;
    } else {
        return _filePairVec.front()->beginMessageId;
    }
}
protocol::ErrorCode FileManager::init(const std::string &dataPath, const ObsoleteFileCriterion &obsoleteFileCriterion) {
    vector<string> extendDataPathVec;
    util::InlineVersion inlineVersion;
    return init(dataPath, extendDataPathVec, obsoleteFileCriterion, inlineVersion);
}

protocol::ErrorCode FileManager::init(const string &dataPath,
                                      const vector<string> &extendDataPathVec,
                                      const ObsoleteFileCriterion &obsoleteFileCriterion,
                                      const util::InlineVersion &inlineVersion,
                                      bool enableFastRecover) {
    _filePairVec.clear();
    _dataPath = dataPath;
    _obsoleteFileCriterion = obsoleteFileCriterion;
    _enableFastRecover = enableFastRecover;
    assert(!_dataPath.empty());
    // normalize the data path.
    if (*(_dataPath.rbegin()) != '/') {
        _dataPath += "/";
    }
    fslib::ErrorCode ec = FileSystem::isExist(_dataPath);
    // init from scratch
    if (fslib::EC_FALSE == ec) { // dataPath not exist
        ec = FileSystem::mkDir(_dataPath, true);
        if (ec != fslib::EC_OK && ec != fslib::EC_EXIST) {
            AUTIL_LOG(ERROR,
                      "make dir[%s] failed, fslib error[%d %s]",
                      _dataPath.c_str(),
                      ec,
                      FileSystem::getErrorString(ec).c_str());
            return ERROR_BROKER_MKDIR;
        }
        AUTIL_LOG(INFO, "make dir[%s] success", _dataPath.c_str());
    } else if (fslib::EC_TRUE != ec) { // isExist error happen
        AUTIL_LOG(ERROR,
                  "isExist[%s] error, fslib error[%d %s]",
                  _dataPath.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
        return ERROR_BROKER_FS_OPERATE;
    }
    for (size_t i = 0; i < extendDataPathVec.size(); i++) {
        ec = FileSystem::isExist(extendDataPathVec[i]);
        if (fslib::EC_TRUE == ec) {
            _extendDataPathVec.push_back(extendDataPathVec[i]);
            AUTIL_LOG(INFO, "add old data path[%s]", extendDataPathVec[i].c_str());
        } else {
            AUTIL_LOG(INFO, "old data path[%s] not exist", extendDataPathVec[i].c_str());
            return ERROR_BROKER_FS_OPERATE;
        }
    }
    if (_enableFastRecover) {
        auto errorCode = updateInlineVersion(inlineVersion);
        if (ERROR_NONE != errorCode) {
            return errorCode;
        }
    } else {
        auto errorCode = removeInlineFile();
        if (ERROR_NONE != errorCode) {
            return errorCode;
        }
    }
    // recover from data_path
    FileManagerUtil::FileLists fileList;
    ec = listFiles(fileList);
    if (fslib::EC_OK != ec) {
        return ERROR_BROKER_FS_OPERATE;
    }
    ErrorCode errorCode = FileManagerUtil::initFilePairVec(fileList, _filePairVec);
    if (errorCode != ERROR_NONE) {
        return errorCode;
    }
    errorCode = adjustFilePairVec(_filePairVec);
    if (errorCode != ERROR_NONE) {
        return errorCode;
    }
    return sealLastFilePair(_filePairVec);
}

protocol::ErrorCode FileManager::initForReadOnly(const string &dataPath, const vector<string> &extendDataPathVec) {
    _filePairVec.clear();
    _dataPath = dataPath;
    assert(!_dataPath.empty());
    // normalize the data path.
    if (*(_dataPath.rbegin()) != '/') {
        _dataPath += "/";
    }
    fslib::ErrorCode ec = FileSystem::isExist(_dataPath);
    // init from scratch
    if (fslib::EC_FALSE == ec) { // dataPath not exist
        ec = FileSystem::mkDir(_dataPath, true);
        if (ec != fslib::EC_OK && ec != fslib::EC_EXIST) {
            AUTIL_LOG(ERROR,
                      "make dir[%s] failed, fslib error[%d %s]",
                      _dataPath.c_str(),
                      ec,
                      FileSystem::getErrorString(ec).c_str());
            return ERROR_BROKER_MKDIR;
        }
        AUTIL_LOG(INFO, "make dir[%s] success", _dataPath.c_str());
    } else if (fslib::EC_TRUE != ec) { // isExist error happen
        AUTIL_LOG(ERROR,
                  "isExist[%s] error, fslib error[%d %s]",
                  _dataPath.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
        return ERROR_BROKER_FS_OPERATE;
    }
    for (size_t i = 0; i < extendDataPathVec.size(); i++) {
        ec = FileSystem::isExist(extendDataPathVec[i]);
        if (fslib::EC_TRUE == ec) {
            _extendDataPathVec.push_back(extendDataPathVec[i]);
            AUTIL_LOG(INFO, "add old data path[%s]", extendDataPathVec[i].c_str());
        } else {
            AUTIL_LOG(INFO, "old data path[%s] not exist", extendDataPathVec[i].c_str());
            return ERROR_BROKER_FS_OPERATE;
        }
    }
    // recover from data_path
    FileManagerUtil::FileLists fileList;
    ec = listFiles(fileList);
    if (fslib::EC_OK != ec) {
        return ERROR_BROKER_FS_OPERATE;
    }
    ErrorCode errorCode = FileManagerUtil::initFilePairVec(fileList, _filePairVec);
    if (errorCode != ERROR_NONE) {
        return errorCode;
    }
    errorCode = adjustFilePairVec(_filePairVec);
    if (errorCode != ERROR_NONE) {
        return errorCode;
    }
    return ERROR_NONE;
}

protocol::ErrorCode FileManager::removeInlineFile() {
    const string &inlineFilePath = FileSystem::joinFilePath(_dataPath, FileManagerUtil::INLINE_FILE);
    bool isExist = false;
    if (fslib::EC_OK == PanguInlineFileUtil::isInlineFileExist(inlineFilePath, isExist) && isExist) {
        auto ec = FileSystem::remove(inlineFilePath);
        if (ec == fslib::EC_OK) {
            AUTIL_LOG(INFO, "remove inline file success, path[%s]", inlineFilePath.c_str());
        } else {
            AUTIL_LOG(ERROR,
                      "remove inline file failed, path[%s] fslib errorcode [%d], errormsg [%s]",
                      inlineFilePath.c_str(),
                      ec,
                      fslib::fs::FileSystem::getErrorString(ec).c_str());
            return ERROR_BROKER_FS_OPERATE;
        }
    }
    return ERROR_NONE;
}

protocol::ErrorCode FileManager::adjustFilePairVec(FilePairVec &filePairVec) {
    if (filePairVec.empty()) {
        return ERROR_NONE;
    }
    int32_t i = 0;
    for (i = (int32_t)filePairVec.size() - 1; i >= 0; i--) {
        // only remove empty file at the end
        int64_t endId = -1;
        ErrorCode ec = filePairVec[i]->getEndMessageId(endId);
        if (ec != ERROR_NONE) {
            return ec;
        }
        if (filePairVec[i]->beginMessageId == endId) { // empty file, remove it
            if (!FileManagerUtil::removeFilePair(filePairVec[i])) {
                AUTIL_LOG(WARN, "remove empty file[%s] failed", filePairVec[i]->metaFileName.c_str());
                return ERROR_BROKER_FS_OPERATE;
            } else {
                AUTIL_LOG(INFO, "remove empty file[%s] success", filePairVec[i]->metaFileName.c_str());
                filePairVec.pop_back();
            }
        } else {
            break;
        }
    }

    for (i = 0; i + 1 < (int32_t)filePairVec.size(); ++i) {
        AUTIL_LOG(INFO,
                  "adjusting file pair:[%s] beginMessageId[%ld]",
                  filePairVec[i]->metaFileName.c_str(),
                  filePairVec[i]->beginMessageId);
        filePairVec[i]->setNext(filePairVec[i + 1]);
    }
    return ERROR_NONE;
}

FilePairPtr FileManager::createNewFilePair(int64_t msgid, int64_t timestamp) {
    {
        ScopedReadWriteLock lock(_rwLock, 'r');
        if (!_filePairVec.empty() && _filePairVec.back()->beginMessageId == msgid) { // has created
            assert(_filePairVec.back()->getEndMessageId() == msgid);
            assert(_filePairVec.back()->beginTimestamp == timestamp);
            return _filePairVec.back();
        }
    }

    string prefix = FileManagerUtil::generateFilePrefix(msgid, timestamp);
    prefix = _dataPath + prefix;
    string dataFileName = prefix + FileManagerUtil::DATA_SUFFIX;
    string metaFileName = prefix + FileManagerUtil::META_SUFFIX;
    FilePairPtr filePair(new FilePair(dataFileName, metaFileName, msgid, timestamp, true));
    // no dump at first
    filePair->setEndMessageId(msgid);
    ScopedReadWriteLock lock(_rwLock, 'w');
    if (_filePairVec.size() != 0) {
        _filePairVec.back()->endAppending();
        _filePairVec.back()->setNext(filePair);
    }
    _filePairVec.push_back(filePair);
    return filePair;
}

FilePairPtr FileManager::getLastFilePair() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    if (_filePairVec.empty()) {
        return FilePairPtr();
    }

    return _filePairVec.back();
}

FilePairPtr FileManager::getFilePairById(int64_t messageId) const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    if (_filePairVec.size() == 0) {
        return FilePairPtr();
    }
    if (_filePairVec[0]->beginMessageId > messageId) {
        return _filePairVec[0];
    }
    size_t i = 0;
    for (i = 0; i < _filePairVec.size() - 1; ++i) {
        if (_filePairVec[i]->beginMessageId <= messageId && _filePairVec[i + 1]->beginMessageId > messageId) {
            return _filePairVec[i];
        }
    }
    if (i == _filePairVec.size() - 1) {
        if (_filePairVec[i]->getEndMessageId() > messageId) {
            return _filePairVec[i];
        }
    }
    return FilePairPtr();
}

FilePairPtr FileManager::getFilePairByTime(int64_t timestamp) const {
    ScopedReadWriteLock lock(_rwLock, 'r');

    // because we don't know endTimeStamp for every file. (for efficiency)
    // so if current file's begin timestamp > given timestamp,
    // we infer the previous file has the message with the timestamp.
    // if even the oldest message's timestamp > given timestamp,
    // we will return the oldest message.

    for (size_t i = 0; i < _filePairVec.size(); ++i) {
        int64_t time = _filePairVec[i]->beginTimestamp;
        if (time > timestamp) {
            return _filePairVec[i > 0 ? i - 1 : 0];
        } else if (time == timestamp) {
            return _filePairVec[i];
        }
    }

    if (!_filePairVec.empty())
        return _filePairVec.back();
    return FilePairPtr();
}

void FileManager::syncFilePair() {
    AUTIL_LOG(INFO, "Start sync file pair dfs[%s]", _dataPath.c_str());
    FileManagerUtil::FileLists fileList;
    fslib::ErrorCode ec = listFiles(fileList);
    if (fslib::EC_OK != ec) {
        return;
    }
    FilePairVec filePairVec;
    ErrorCode errorCode = FileManagerUtil::initFilePairVec(fileList, filePairVec);
    if (errorCode != ERROR_NONE) {
        return;
    }
    if (filePairVec.empty()) {
        return;
    }
    int64_t minMessageId = (*filePairVec.begin())->beginMessageId;
    {
        ScopedReadWriteLock lock(_rwLock, 'w');
        FilePairVec::iterator it = _filePairVec.begin();
        for (; it != _filePairVec.end(); ++it) {
            if ((*it)->beginMessageId >= minMessageId) {
                break;
            }
        }
        _filePairVec.erase(_filePairVec.begin(), it);
    }
    AUTIL_LOG(INFO, "Finish sync file pair dfs[%s], size[%d]", _dataPath.c_str(), (int)_filePairVec.size());
}

int32_t FileManager::getObsoleteFilePos(uint32_t reservedFileCount, int64_t commitedTs) const {
    int32_t obsoleteFilePos = -1;
    int64_t curTime = TimeUtility::currentTime();
    ScopedReadWriteLock lock(_rwLock, 'r');
    for (size_t i = 0; i < _filePairVec.size(); ++i) {
        if (_filePairVec.size() - i <= reservedFileCount) {
            break;
        }
        int64_t beginTime = _filePairVec[i]->beginTimestamp;
        if (isFileObsolete(beginTime, curTime, commitedTs)) {
            fslib::FileMeta fileMeta;
            fslib::ErrorCode ec = FileSystem::getFileMeta(_filePairVec[i]->metaFileName, fileMeta);
            if (ec != fslib::EC_OK) {
                AUTIL_LOG(ERROR,
                          "get file meta[%s] failed, fslib errorcode[%d], errormsg[%s]",
                          _filePairVec[i]->metaFileName.c_str(),
                          ec,
                          fslib::fs::FileSystem::getErrorString(ec).c_str());
                continue;
            }
            AUTIL_LOG(INFO, "get file meta[%s] success", _filePairVec[i]->metaFileName.c_str());
            if (isFileObsolete(int64_t(fileMeta.lastModifyTime * 1000 * 1000), curTime, commitedTs)) {
                obsoleteFilePos = (int32_t)i;
            } else {
                break;
            }
        } else {
            break;
        }
    }
    return obsoleteFilePos;
}

void FileManager::doDeleteObsoleteFile() {
    FilePairVec::iterator it = _obsoleteFilePairVec.begin();
    while (it != _obsoleteFilePairVec.end()) {
        if (1 == (*it).use_count()) {
            if (FileManagerUtil::removeFilePair(*it)) {
                it = _obsoleteFilePairVec.erase(it);
            } else {
                it++;
            }
        } else {
            it++;
        }
    }
}

void FileManager::deleteObsoleteFile(int64_t commitedTimestamp) {
    syncFilePair();
    uint32_t reservedFileCount =
        _obsoleteFileCriterion.reservedFileCount > 1 ? _obsoleteFileCriterion.reservedFileCount : 1;
    if (_filePairVec.size() <= (size_t)reservedFileCount) {
        doDeleteObsoleteFile();
        return;
    }
    int32_t obsoleteFilePos = getObsoleteFilePos(reservedFileCount, commitedTimestamp);
    if (obsoleteFilePos >= 0) {
        ScopedReadWriteLock lock(_rwLock, 'w');
        FilePairVec::iterator beginIt = _filePairVec.begin();
        FilePairVec::iterator endIt = _filePairVec.begin() + obsoleteFilePos + 1;
        for (FilePairVec::iterator it = beginIt; it != endIt; ++it) {
            _obsoleteFilePairVec.push_back(*it);
        }
        _filePairVec.erase(beginIt, endIt);
    }
    doDeleteObsoleteFile();
}

fslib::ErrorCode FileManager::doListFiles(const fslib::FileList &paths, FileManagerUtil::FileLists &fileList) {
    return FileManagerUtil::listFiles(paths, fileList);
}

fslib::ErrorCode FileManager::listFiles(FileManagerUtil::FileLists &fileList) {
    auto ec = doListFiles({_dataPath}, fileList);
    if (fslib::EC_OK != ec) {
        return ec;
    }
    ec = doListFiles(_extendDataPathVec, fileList);
    if (fslib::EC_OK != ec) {
        return ec;
    }
    for (auto it = fileList.begin(); it != fileList.end();) {
        if (FileManagerUtil::INLINE_FILE == it->second) {
            it = fileList.erase(it);
        } else {
            ++it;
        }
    }
    return ec;
}

void FileManager::delExpiredFile(int64_t commitedTimestamp) {
    if (_obsoleteFileCriterion.obsoleteFileTimeInterval != -1 && _obsoleteFileCriterion.reservedFileCount != -1) {
        int64_t curTime = TimeUtility::currentTime();
        if (_lastDelFileTime + _obsoleteFileCriterion.delObsoleteFileInterval < curTime) {
            AUTIL_LOG(INFO, "begin del expired file in path[%s]", _dataPath.c_str());
            deleteObsoleteFile(commitedTimestamp);
            AUTIL_LOG(INFO, "end del expired file in path[%s]", _dataPath.c_str());
            _lastDelFileTime = curTime;
        }
    }
}

bool FileManager::isTimestamp(int64_t obsoleteTime, int64_t &realTime) const {
    static const int64_t TIMESTAMP_START = int64_t(1640966400) * 3600 * 1000 * 1000; // 2022-1-1 0:0:0
    if (obsoleteTime >= TIMESTAMP_START) {
        realTime = obsoleteTime / 3600;
        return true;
    } else {
        realTime = -1;
        return false;
    }
}

bool FileManager::isFileObsolete(int64_t fileTime, int64_t curTime, int64_t commitedTs) const {
    int64_t realObsoleteTime = -1;
    bool isTs = isTimestamp(_obsoleteFileCriterion.obsoleteFileTimeInterval, realObsoleteTime);
    if (isTs && (fileTime < realObsoleteTime)) {
        return true;
    }
    if (COMMITTED_TIMESTAMP_INVALID == commitedTs) {
        return isTs ? false : (curTime - fileTime) > _obsoleteFileCriterion.obsoleteFileTimeInterval;
    } else {
        return fileTime < commitedTs;
    }
}

ErrorCode FileManager::sealOtherWriter(const util::InlineVersion &inlineVersion) {
    ErrorCode rec = updateInlineVersion(inlineVersion);
    if (ERROR_NONE != rec) {
        return rec;
    }
    return sealLastFilePair(_filePairVec);
}

ErrorCode FileManager::updateInlineVersion(const util::InlineVersion &inlineVersion) {
    if (!_enableFastRecover) {
        return ERROR_NONE;
    }
    string inlineContent;
    const string &inlineFilePath = FileSystem::joinFilePath(_dataPath, FileManagerUtil::INLINE_FILE);
    if (fslib::EC_OK != PanguInlineFileUtil::getInlineFile(inlineFilePath, inlineContent)) {
        AUTIL_LOG(ERROR, "read inline[%s] fail", inlineFilePath.c_str());
        return ERROR_BROKER_READ_FILE;
    }
    util::InlineVersion curInlineVersion;
    if (!inlineContent.empty()) {
        if (!curInlineVersion.deserialize(inlineContent)) {
            AUTIL_LOG(ERROR, "deserialize cur inline version[%s] fail", inlineContent.c_str());
            return ERROR_BROKER_INLINE_VERSION_INVALID;
        }
    }
    if (inlineVersion < curInlineVersion) {
        AUTIL_LOG(ERROR,
                  "[%s]update inline version[%s] < current[%s], error",
                  inlineFilePath.c_str(),
                  inlineVersion.toDebugString().c_str(),
                  curInlineVersion.toDebugString().c_str());
        return ERROR_BROKER_INLINE_VERSION_INVALID;
    } else if (inlineVersion == curInlineVersion) { // topic modify, broker restart case
        AUTIL_LOG(INFO,
                  "[%s]update inline version[%s] == current[%s], do nothing",
                  inlineFilePath.c_str(),
                  inlineVersion.toDebugString().c_str(),
                  curInlineVersion.toDebugString().c_str());
        _inlineVersion = inlineVersion;
        return ERROR_NONE;
    }
    auto ec = PanguInlineFileUtil::updateInlineFile(inlineFilePath, curInlineVersion, inlineVersion);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR,
                  "update inline file[%s] version[%s -> %s] fail, error[%d %s]",
                  inlineFilePath.c_str(),
                  curInlineVersion.toDebugString().c_str(),
                  inlineVersion.toDebugString().c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
        return ERROR_BROKER_FS_OPERATE;
    }
    AUTIL_LOG(INFO,
              "update inline file[%s] version[%s -> %s] success",
              inlineFilePath.c_str(),
              curInlineVersion.toDebugString().c_str(),
              inlineVersion.toDebugString().c_str());
    _inlineVersion = inlineVersion;
    return ERROR_NONE;
}

ErrorCode FileManager::refreshVersion() {
    if (!_inlineVersion.valid()) {
        return ERROR_BROKER_INLINE_VERSION_INVALID;
    }
    return sealOtherWriter(_inlineVersion);
}

protocol::ErrorCode FileManager::sealLastFilePair(const FilePairVec &filePairVec) {
    if (!_enableFastRecover) {
        return ERROR_NONE;
    }
    if (filePairVec.empty()) {
        return ERROR_NONE;
    }
    size_t lastPos = filePairVec.size() - 1;
    string args, output;
    auto ec = PanguInlineFileUtil::sealFile(filePairVec[lastPos]->metaFileName);
    if (fslib::EC_OK != ec) {
        return ERROR_BROKER_FS_OPERATE;
    }
    ec = PanguInlineFileUtil::sealFile(filePairVec[lastPos]->dataFileName);
    if (fslib::EC_OK != ec) {
        return ERROR_BROKER_FS_OPERATE;
    }
    return ERROR_NONE;
}

string FileManager::getInlineFilePath() const {
    return FileSystem::joinFilePath(_dataPath, FileManagerUtil::INLINE_FILE);
}

} // namespace storage
} // namespace swift
