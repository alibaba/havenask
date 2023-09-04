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
#pragma once

#include <map>
#include <memory>
#include <stddef.h>
#include <string>

#include "fslib/common/common_type.h"

namespace suez {

class Directory;

using DirectoryPtr = std::shared_ptr<Directory>;

class CleanResult {
public:
    CleanResult() : _cleaned(false), _hasError(false) {}
    CleanResult(bool cleaned, bool hasError) : _cleaned(cleaned), _hasError(hasError) {}

public:
    void setCleaned() { _cleaned = true; }
    void setError() { _hasError = true; }
    void update(const CleanResult &other) {
        _cleaned = _cleaned | other._cleaned;
        _hasError = _hasError | other._hasError;
    }
    bool cleaned() { return _cleaned; }
    bool hasError() { return _hasError; }

private:
    bool _cleaned;
    bool _hasError;
};

class Directory {
public:
    Directory();
    ~Directory();
    Directory(const std::string &base, const std::string &name, size_t keepCount);

public:
    DirectoryPtr addSubDir(const std::string &name, size_t keepCount);
    void syncSubFromDisk(const std::string &absPath);
    CleanResult sync(const DirectoryPtr &target);
    bool syncSub();
    void updateKeepCount(size_t keepCount);
    size_t getKeepCount() const { return _keepCount; }
    std::string getAbsPath() const { return _absPath; }
    fslib::ErrorCode remove();

private:
    bool _syncSub;
    std::string _absPath;
    std::map<std::string, DirectoryPtr> _subDirs;
    size_t _keepCount;
};

class TargetLayout {
public:
    TargetLayout();
    ~TargetLayout();

public:
    void addTarget(const std::string &base, const std::string &tableName, const std::string &version, size_t keepCount);
    CleanResult sync();
    static void strip(std::string &str, const std::string &chars) {
        str.erase(str.find_last_not_of(chars) + 1);
        str.erase(0, str.find_first_not_of(chars));
    }

private:
    DirectoryPtr getBaseDir(const std::string &absPath);

private:
    std::map<std::string, DirectoryPtr> _baseMap;
};

} // namespace suez
