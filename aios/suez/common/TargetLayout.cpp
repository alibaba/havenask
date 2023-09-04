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
#include "suez/common/TargetLayout.h"

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "fslib/fs/FileSystem.h"
#include "suez/sdk/PathDefine.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TargetLayout);

namespace suez {

Directory::Directory() : _syncSub(false), _keepCount(0) {}

Directory::Directory(const std::string &base, const std::string &name, size_t keepCount)
    : _syncSub(false), _absPath(PathDefine::join(base, name)), _keepCount(keepCount) {}

Directory::~Directory() {}

DirectoryPtr Directory::addSubDir(const string &name, size_t keepCount) {
    _syncSub = true;
    auto it = _subDirs.find(name);
    if (_subDirs.end() != it) {
        it->second->updateKeepCount(keepCount);
        return it->second;
    }
    DirectoryPtr sub(new Directory(_absPath, name, keepCount));
    _subDirs.insert(make_pair(name, sub));
    return sub;
}

void Directory::syncSubFromDisk(const string &absPath) {
    fslib::FileList fileList;
    auto ec = fslib::fs::FileSystem::isExist(absPath);
    if (ec != fslib::EC_TRUE) {
        if (ec != fslib::EC_FALSE) {
            AUTIL_LOG(ERROR, "check [%s] exist failed, error code[%d]", absPath.c_str(), ec);
        }
        return;
    }
    ec = fslib::fs::FileSystem::listDir(absPath, fileList);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR, "listDir [%s] failed", absPath.c_str());
        return;
    }
    for (const auto &sub : fileList) {
        DirectoryPtr subPtr(new Directory(absPath, sub, 0));
        _subDirs.insert(make_pair(sub, subPtr));
    }
}

CleanResult Directory::sync(const DirectoryPtr &target) {
    CleanResult res;
    if (!target->syncSub()) {
        return res;
    }
    const auto &targetSubDir = target->_subDirs;
    syncSubFromDisk(target->getAbsPath());
    std::vector<DirectoryPtr> toRemove;
    for (const auto &currentDir : _subDirs) {
        auto it = targetSubDir.find(currentDir.first);
        if (targetSubDir.end() != it) {
            auto syncSubRes = currentDir.second->sync(it->second);
            res.update(syncSubRes);
        } else {
            toRemove.push_back(currentDir.second);
        }
    }
    sort(toRemove.begin(), toRemove.end(), [](const DirectoryPtr &left, const DirectoryPtr &right) {
        return left->getAbsPath() > right->getAbsPath();
    });
    size_t count = target->getKeepCount();
    if (toRemove.size() > count) {
        auto it = toRemove.rbegin();
        AUTIL_LOG(INFO, "begin to remove data path [%s]", (*it)->getAbsPath().c_str());
        auto s = (*it)->remove();
        if (s != fslib::EC_OK) {
            AUTIL_LOG(ERROR, "error when cleaning path [%s], code [%d]", (*it)->getAbsPath().c_str(), s);
            res.setError();
        } else {
            res.setCleaned();
        }
    }
    return res;
}

bool Directory::syncSub() { return _syncSub; }

void Directory::updateKeepCount(size_t keepCount) { _keepCount = std::max(_keepCount, keepCount); }

fslib::ErrorCode Directory::remove() { return fslib::fs::FileSystem::remove(_absPath); }

TargetLayout::TargetLayout() {}

TargetLayout::~TargetLayout() {}

void TargetLayout::addTarget(const std::string &base,
                             const std::string &tableName,
                             const std::string &version,
                             size_t keepCount) {
    if (base.empty() || tableName.empty() || version.empty()) {
        AUTIL_LOG(ERROR, "%s %s %s can't be empty", base.c_str(), tableName.c_str(), version.c_str());
        return;
    }
    auto strippedVersion = version;
    TargetLayout::strip(strippedVersion, "/ ");
    auto baseDir = getBaseDir(base);
    auto tableDir = baseDir->addSubDir(tableName, keepCount);
    tableDir->addSubDir(strippedVersion, 0);
}

CleanResult TargetLayout::sync() {
    CleanResult res;
    for (const auto &base : _baseMap) {
        Directory dir;
        auto ret = dir.sync(base.second);
        res.update(ret);
    }
    return res;
}

DirectoryPtr TargetLayout::getBaseDir(const std::string &absPath) {
    auto it = _baseMap.find(absPath);
    if (_baseMap.end() != it) {
        return it->second;
    } else {
        DirectoryPtr sub(new Directory(absPath, "", 0));
        _baseMap.insert(make_pair(absPath, sub));
        return sub;
    }
}

} // namespace suez
