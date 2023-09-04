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
#ifndef FSLIB_FILEDATACACHE_H
#define FSLIB_FILEDATACACHE_H

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LruCache.h"
#include "fslib/fslib.h"

FSLIB_BEGIN_NAMESPACE(cache);

class FileDataCache {
public:
    FileDataCache(int64_t totalCacheSize);
    ~FileDataCache();

public:
    bool put(const std::string &filePath, const std::string &content);
    bool get(const std::string &filePath, std::string &content, int64_t &ts);

    void removeFile(const std::string &filePath);
    void removeDirectory(const std::string &path);

    int64_t getCacheMemUse() const;
    int64_t getCachedFileCount() const;

private:
    class MemDataGetSizeCallBack {
    public:
        uint32_t operator()(const std::string &str) { return str.length(); }
    };

    typedef autil::LruCache<std::string, std::string, MemDataGetSizeCallBack> MemDataCache;

private:
    // key: path, value: ts
    typedef std::map<std::string, int64_t> CacheMetaMap;

    MemDataCache _cache;
    CacheMetaMap _metaMap;
    mutable autil::RecursiveThreadMutex _lock;
};

FSLIB_TYPEDEF_SHARED_PTR(FileDataCache);

FSLIB_END_NAMESPACE(cache);

#endif // FSLIB_FILEDATACACHE_H
