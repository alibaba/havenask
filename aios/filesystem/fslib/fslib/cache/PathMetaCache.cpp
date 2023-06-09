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
#include "fslib/cache/PathMetaCache.h"
#include "fslib/cache/DirectoryMapIterator.h"
#include "fslib/util/PathUtil.h"

using namespace std;
using namespace autil;
FSLIB_USE_NAMESPACE(util);

FSLIB_BEGIN_NAMESPACE(cache);
AUTIL_DECLARE_AND_SETUP_LOGGER(cache, PathMetaCache);

PathMetaCache::PathMetaCache() {}

PathMetaCache::~PathMetaCache() {}

void PathMetaCache::addPathMeta(
        const string& normPath, int64_t fileLength,
        uint64_t createTime, uint64_t modifyTime, bool isDirectory)
{
    PathMetaPtr info(new PathMeta);
    info->isFile = !isDirectory;
    info->length = fileLength;
    info->createTime = createTime;
    info->lastModifyTime = modifyTime;

    ScopedLock lock(_lock);
    FastPathMetaMap::const_iterator iter = _fastMap.find(normPath);
    if (iter != _fastMap.end()) {
        AUTIL_LOG(INFO, "PathMeta for path [%s] already exist in MetaCache, will update",
                  normPath.c_str());
    }
    _pathMetaMap[normPath] = info;
    _fastMap[normPath] = info;
}

void PathMetaCache::markImmutablePath(const string& normImmutablePath, bool exist)
{
    ScopedLock lock(_lock);
    _immutablePathMap[normImmutablePath] = exist;
}

bool PathMetaCache::removeFile(const string& normalPath)
{
    ScopedLock lock(_lock);
    PathMetaMap::const_iterator it = _pathMetaMap.find(normalPath);
    if (it != _pathMetaMap.end()) {
        const PathMetaPtr& fileInfo = it->second;
        if (!fileInfo->isFile) {
            AUTIL_LOG(ERROR, "Path [%s] is not a file, remove failed",
                      normalPath.c_str());  
            return false;
        }
        _fastMap.erase(normalPath); // fastFileNodeMap should erase first
        _pathMetaMap.erase(normalPath);
        return true;
    }
    return false;
}
    
bool PathMetaCache::removeDirectory(const string& normalPath)
{
    ScopedLock lock(_lock);
    PathMetaPtr infoPtr;
    FastPathMetaMap::const_iterator iter = _fastMap.find(normalPath);
    if (iter != _fastMap.end()) {
        infoPtr = iter->second;
    }
    if (infoPtr && infoPtr->isFile) {
        AUTIL_LOG(ERROR, "path [%s] is a file!", normalPath.c_str());
        return false;
    }
    DirectoryMapIterator<PathMetaPtr> iterator(_pathMetaMap, normalPath);
    while (iterator.hasNext()) {
        auto curIt = iterator.next();
        _fastMap.erase(curIt->first);
        iterator.erase(curIt);
    }

    PathMetaMap::iterator it = _pathMetaMap.find(normalPath);
    if (it != _pathMetaMap.end()) {
        _fastMap.erase(it->first);
        _pathMetaMap.erase(it);
    }

    ImmutablePathMap::const_iterator pIter = _immutablePathMap.begin();
    for ( ; pIter != _immutablePathMap.end(); ) {
        const string& immutablePath = pIter->first;
        if (immutablePath == normalPath || PathUtil::isInRootPath(immutablePath, normalPath)) {
            _immutablePathMap.erase(pIter++);
            continue;
        }
        pIter++;
    }
    return true;
}

bool PathMetaCache::matchImmutablePath(const string& normalPath) const
{
    ScopedLock lock(_lock);
    ImmutablePathMap::const_iterator iter = _immutablePathMap.begin();
    for ( ; iter != _immutablePathMap.end(); iter++) {
        const string& immutablePath = iter->first;
        if (PathUtil::isInRootPath(normalPath, immutablePath)) {
            return true;
        }
    }
    return false;
}

bool PathMetaCache::isExist(const string& normalPath) const
{
    ScopedLock lock(_lock);
    if (_fastMap.find(normalPath) != _fastMap.end()) {
        return true;
    }
    auto iter = _immutablePathMap.find(normalPath);
    return iter != _immutablePathMap.end() && iter->second;
}

bool PathMetaCache::getPathMeta(const string& normalPath, PathMeta& fileInfo) const
{
    ScopedLock lock(_lock);
    FastPathMetaMap::const_iterator iter = _fastMap.find(normalPath);
    if (iter != _fastMap.end()) {
        fileInfo = *(iter->second);
        return true;
    }

    auto pIter = _immutablePathMap.find(normalPath);
    if (pIter != _immutablePathMap.end() && pIter->second) {
        fileInfo.isFile = false;
        fileInfo.length = -1;
        fileInfo.createTime = -1;
        fileInfo.lastModifyTime = -1;
        return true;
    }
    return false;
}

size_t PathMetaCache::getFileLength(const string& normalPath) const
{
    ScopedLock lock(_lock);
    FastPathMetaMap::const_iterator iter = _fastMap.find(normalPath);
    if (iter == _fastMap.end()) {
        return 0;
    }
    return iter->second->length;
}

bool PathMetaCache::ListPathMetaInCache(const string& normalPath,
                                        vector<pair<string, PathMeta>>& pathMetas,
                                        bool recursive)
{
    ScopedLock lock(_lock);
    PathMetaPtr infoPtr;
    FastPathMetaMap::const_iterator iter = _fastMap.find(normalPath);
    if (iter != _fastMap.end()) {
        infoPtr = iter->second;
    }
    if (infoPtr && infoPtr->isFile) {
        AUTIL_LOG(ERROR, "path [%s] is not directory!", normalPath.c_str());
        return false;
    }
    
    DirectoryMapIterator<PathMetaPtr> iterator(_pathMetaMap, normalPath);
    while (iterator.hasNext()) {
        auto curIt = iterator.next();
        const string& path = curIt->first;
        if (!recursive && path.find("/", normalPath.size() + 1) != string::npos) {
            continue;
        }
        string relativePath = path.substr(normalPath.size() + 1);
        pathMetas.push_back(make_pair(relativePath, *curIt->second));
    }
    return true;
}

int64_t PathMetaCache::getCachedPathCount() const
{
    ScopedLock lock(_lock);
    return _fastMap.size();
}

int64_t PathMetaCache::getImmutablePathCount() const
{
    ScopedLock lock(_lock);
    return _immutablePathMap.size();
}

FSLIB_END_NAMESPACE(cache);

