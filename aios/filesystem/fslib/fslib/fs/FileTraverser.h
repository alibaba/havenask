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
#ifndef FSLIB_FILETRAVERSER_H
#define FSLIB_FILETRAVERSER_H

#include <map>

#include "autil/Lock.h"
#include "autil/ThreadPool.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"

FSLIB_BEGIN_NAMESPACE(fs);

class FileTraverser {
public:
    FileTraverser(const std::string &basePath, autil::ThreadPoolPtr threadPool);
    ~FileTraverser();

private:
    FileTraverser(const FileTraverser &);
    FileTraverser &operator=(const FileTraverser &);

public:
    void traverse(bool recursive = true);
    void wait() { _notifier.wait(); }
    const ErrorCode &getErrorCode() const { return _errorCode; }
    const EntryInfoMap &getResult() const { return _result; }

private:
    void splitFilter(std::string &path, std::string &filter);
    void traverseNextLevel(const EntryMeta &entryMeta, std::string filter, bool recursive);
    void traverseOneLevel(const EntryMeta &entryMeta, const std::string &filter, bool recursive);
    void doTraverseOneLevel(const EntryMeta &entryMeta, const std::string &filter, bool recursive);
    bool processFile(const EntryMeta &entryMeta);
    bool match(const std::string &fileName, const std::string &filter) const;
    void setErrorCode(const ErrorCode errorCode);
    bool convertToEntryMeta(const std::string &path, EntryMeta &entryMeta);

public:
    friend class FileTraverserTest;

private:
    std::string _basePath;
    autil::ThreadPoolPtr _threadPool;
    EntryInfoMap _result;
    ErrorCode _errorCode;
    autil::ReadWriteLock _lock;
    autil::TerminateNotifier _notifier;
};

FSLIB_END_NAMESPACE(fs);

#endif // FSLIB_FILETRAVERSER_H
