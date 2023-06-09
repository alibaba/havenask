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
#ifndef FSLIB_PATHMETACACHE_H
#define FSLIB_PATHMETACACHE_H

#include "autil/Log.h"
#include "autil/Lock.h"
#include "fslib/fslib.h"
#include <map>
#include <set>
#include <unordered_map>

FSLIB_BEGIN_NAMESPACE(cache);

class PathMetaCache
{
public:
    PathMetaCache();
    ~PathMetaCache();
    
public:
    void addPathMeta(const std::string& normPath, int64_t fileLength,
                     uint64_t createTime, uint64_t modifyTime, bool isDirectory);

    bool removeFile(const std::string& normPath);
    
    bool removeDirectory(const std::string& normPath);

    bool isExist(const std::string& normPath) const;

    bool getPathMeta(const std::string& normPath, PathMeta& fileInfo) const;

    size_t getFileLength(const std::string& normPath) const;

    void markImmutablePath(const std::string& normImmutablePath, bool exist = true);

    bool matchImmutablePath(const std::string& normPath) const;

    bool ListPathMetaInCache(const std::string& normPath,
                             std::vector<std::pair<std::string, PathMeta>>& pathMetas,
                             bool recursive);

    int64_t getCachedPathCount() const;
    int64_t getImmutablePathCount() const;
    
private:
    FSLIB_TYPEDEF_SHARED_PTR(PathMeta);
    typedef std::map<std::string, PathMetaPtr> PathMetaMap;
    typedef std::unordered_map<std::string, PathMetaPtr> FastPathMetaMap;

    // first : path, second : exist
    typedef std::map<std::string, bool> ImmutablePathMap;

    PathMetaMap _pathMetaMap;
    FastPathMetaMap _fastMap;
    ImmutablePathMap _immutablePathMap;
    mutable autil::RecursiveThreadMutex _lock;
};

FSLIB_TYPEDEF_SHARED_PTR(PathMetaCache);

FSLIB_END_NAMESPACE(cache);

#endif //FSLIB_PATHMETACACHE_H
