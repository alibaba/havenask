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
#include "fslib/fs/FileTraverser.h"

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fslib.h"

using namespace std;
using namespace autil;

FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, FileTraverser);

FileTraverser::FileTraverser(const string &basePath, ThreadPoolPtr threadPool) {
    _basePath = basePath;
    _threadPool = threadPool;
    _errorCode = EC_OK;
}

FileTraverser::~FileTraverser() { wait(); }

bool FileTraverser::convertToEntryMeta(const string &path, EntryMeta &entryMeta) {
    ErrorCode errorCode = FileSystem::isDirectory(_basePath + path);
    if (errorCode != EC_TRUE && errorCode != EC_FALSE) {
        AUTIL_LOG(WARN, "File not exist, filename [%s]", (_basePath + path).c_str());
        setErrorCode(errorCode);
        return false;
    }

    entryMeta.entryName = path;
    if (errorCode == EC_TRUE) {
        entryMeta.isDir = true;
    } else {
        entryMeta.isDir = false;
    }

    return true;
}

void FileTraverser::traverse(bool recursive) {
    string filter;
    splitFilter(_basePath, filter);

    ErrorCode errorCode = FileSystem::isDirectory(_basePath);
    if (errorCode == EC_FALSE) {
        AUTIL_LOG(WARN, "[%s] is not a directory.", _basePath.c_str());
        setErrorCode(EC_NOTDIR);
        return;
    } else if (errorCode != EC_TRUE) {
        AUTIL_LOG(WARN, "[%s] is not existed.", _basePath.c_str());
        setErrorCode(errorCode);
        return;
    }

    if (_basePath.at(_basePath.length() - 1) != '/') {
        _basePath += '/';
    }

    EntryMeta entryMeta;
    if (!convertToEntryMeta("", entryMeta)) {
        return;
    }

    traverseNextLevel(entryMeta, filter, recursive);
}

void FileTraverser::traverseNextLevel(const EntryMeta &entryMeta, string filter, bool recursive) {
    {
        ScopedReadLock lock(_lock);
        if (_errorCode != EC_OK) {
            return;
        }
    }
    _notifier.inc();
    auto task = [this, entryMeta, filter, recursive]() { traverseOneLevel(entryMeta, filter, recursive); };
    _threadPool->pushTask(task, false, true);
}

void FileTraverser::traverseOneLevel(const EntryMeta &entryMeta, const string &filter, bool recursive) {
    doTraverseOneLevel(entryMeta, filter, recursive);
    _notifier.dec();
}

void FileTraverser::doTraverseOneLevel(const EntryMeta &entryMeta, const string &filter, bool recursive) {
    string path = entryMeta.entryName;
    AUTIL_LOG(TRACE1, "path [%s], filter [%s]", path.c_str(), filter.c_str());
    if (filter.empty()) {
        if (!processFile(entryMeta)) {
            return;
        }
    }
    if (entryMeta.isDir == false || (!entryMeta.entryName.empty() && filter.empty() && !recursive)) {
        return;
    }
    string absPath = _basePath + path;
    if (!path.empty()) {
        ErrorCode ec = FileSystem::isLink(absPath);
        if (ec == EC_TRUE) {
            return;
        } else if (ec != EC_FALSE) {
            setErrorCode(ec);
            string errorString = FileSystem::getErrorString(ec);
            AUTIL_LOG(WARN, "isLink %s failed, %s", absPath.c_str(), errorString.c_str());
            return;
        }
    }

    EntryList entryList;
    ErrorCode ec = FileSystem::listDir(absPath, entryList);
    if (ec != EC_OK) {
        setErrorCode(ec);
        string errorString = FileSystem::getErrorString(ec);
        if (errorString == "path does not exist!") {
            errorString = "maybe a subdir is not exist!";
        }
        AUTIL_LOG(
            WARN, "Get filenames failed, path [%s], error [%s].", (_basePath + path).c_str(), errorString.c_str());
        return;
    }
    EntryList::iterator it = entryList.begin();
    for (; it != entryList.end(); ++it) {
        EntryMeta entryMeta = *it;
        if (!match(entryMeta.entryName, filter)) {
            continue;
        }
        if (!path.empty()) {
            entryMeta.entryName = path + "/" + entryMeta.entryName;
        }
        traverseNextLevel(entryMeta, "", recursive);
    }
}

bool FileTraverser::processFile(const EntryMeta &entryMeta) {
    string path = entryMeta.entryName;
    if (path == "") {
        return true;
    }
    bool isDir = entryMeta.isDir;
    {
        ScopedReadLock lock(_lock);
        if (_result.find(path) != _result.end()) {
            return true;
        }
    }
    uint64_t fileSize = 0;
    if (!isDir) {
        FileMeta fileMeta;
        ErrorCode errorCode = fs::FileSystem::getFileMeta(_basePath + path, fileMeta);
        if (errorCode != EC_OK) {
            setErrorCode(errorCode);
            AUTIL_LOG(WARN, "Get fileMeta failed, fileName [%s]", path.c_str());
            return false;
        }
        fileSize = fileMeta.fileLength;
    }
    ScopedWriteLock lock(_lock);
    EntryInfo &entryInfo = _result[path];
    entryInfo.entryName = path;
    entryInfo.length = fileSize;
    return true;
}

void FileTraverser::splitFilter(string &path, string &filter) {
    if (path.at(path.length() - 1) != '*') {
        filter = "";
        return;
    }
    size_t pos = path.rfind("/");
    if (pos == string::npos) {
        filter = path.substr();
        path = "";
    } else {
        filter = path.substr(pos + 1);
        path = path.substr(0, pos);
    }
}

bool FileTraverser::match(const string &fileName, const string &filter) const {
    assert(filter.empty() || filter.at(filter.length() - 1) == '*');
    size_t len = filter.length();
    if (len <= 1) {
        return true;
    }
    if (fileName.compare(0, len - 1, filter, 0, len - 1) == 0) {
        return true;
    }
    return false;
}

void FileTraverser::setErrorCode(const ErrorCode errorCode) {
    ScopedWriteLock lock(_lock);
    if (_errorCode == EC_OK) {
        _errorCode = errorCode;
    }
}

FSLIB_END_NAMESPACE(common);
