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
#ifndef FSLIB_FSCACHEMODULE_H
#define FSLIB_FSCACHEMODULE_H

#include <unordered_set>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "fslib/fslib.h"
#include "fslib/util/RegularExpr.h"
#include "fslib/util/Singleton.h"

FSLIB_BEGIN_NAMESPACE(fs);
class File;
FSLIB_END_NAMESPACE(fs);

FSLIB_BEGIN_NAMESPACE(cache);

class FileDataCache;
FSLIB_TYPEDEF_SHARED_PTR(FileDataCache);
class PathMetaCache;
FSLIB_TYPEDEF_SHARED_PTR(PathMetaCache);

class FSCacheModule : public util::Singleton<FSCacheModule> {
public:
    class PatternMatcher {
    public:
        enum MatchMode {
            MM_DISABLE,
            MM_WHITE,
            MM_BLACK,
        };

    public:
        PatternMatcher();
        ~PatternMatcher();

    public:
        void init(const std::string &mode, const std::vector<std::string> &patterns);
        bool match(const std::string &filePath) const;

    private:
        MatchMode _mode;
        util::RegularExprVector _regExprVec;
    };

public:
    FSCacheModule();
    ~FSCacheModule();

public:
    // meta cache
    bool addPathMeta(
        const std::string &filePath, int64_t fileLength, uint64_t createTime, uint64_t modifyTime, bool isDirectory);

    bool setImmutablePath(const std::string &immutablePath,
                          const std::vector<std::pair<std::string, PathMeta>> &innerPathMetas);

    bool matchImmutablePath(const std::string &path) const;

    bool getPathMeta(const std::string &filePath, PathMeta &pathMeta) const;

    bool listInImmutablePath(const std::string &path,
                             std::vector<std::pair<std::string, PathMeta>> &subFiles,
                             bool recursive) const;

    // data cache
    void putFileDataToCache(const std::string &filePath, fs::File *file);
    bool getFileDataFromCache(const std::string &filePath, std::string &data) const;

    // clear all cache data, not thread-safe
    void reset();
    // clear cache data of target path ( file or dir )
    void invalidatePath(const std::string &path);

    bool needCacheMeta(const std::string &path) const { return _metaCache && needCache(path); }
    bool needCacheData(const std::string &path) const { return _dataCache && needCache(path); }

    int64_t getMetaCachedPathCount() const;
    int64_t getMetaCacheImmutablePathCount() const;

    int64_t getDataCachedFileCount() const;
    int64_t getDataCacheMemUse() const;

private:
    void initFromEnv();
    void initCacheSupportFsTypes();
    void initCacheFileMatchPatterns();
    void initFslibMetaCache();
    void initFslibDataCache();

    void parseDataCacheParam(const std::string &dataParamStr, int64_t &maxFileSize, int64_t &totalCacheSize);

    bool getFileLength(const std::string &filePath, fs::File *file, int64_t &len);

    bool needCache(const std::string &filePath) const;

private:
    static const int64_t DEFAULT_CACHE_SINGLE_FILE_THRESHOLD;
    static const int64_t MAX_CACHE_SINGLE_FILE_THRESHOLD;
    static const int64_t DEFAULT_TOTAL_DATA_CACHE_SIZE;

private:
    std::unordered_set<std::string> _supportFsTypeSet;
    FileDataCachePtr _dataCache;
    PathMetaCachePtr _metaCache;
    int64_t _cacheFileThreshold;
    PatternMatcher _patternMatcher;
    AUTIL_LOG_DECLARE();
};

FSLIB_END_NAMESPACE(cache);

#endif // FSLIB_FSCACHEMODULE_H
