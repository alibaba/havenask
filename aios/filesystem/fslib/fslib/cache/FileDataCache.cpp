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
#include "fslib/cache/FileDataCache.h"
#include "fslib/cache/DirectoryMapIterator.h"
#include "fslib/util/PathUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
FSLIB_USE_NAMESPACE(util);

FSLIB_BEGIN_NAMESPACE(cache);
AUTIL_DECLARE_AND_SETUP_LOGGER(cache, FileDataCache);

FileDataCache::FileDataCache(int64_t totalCacheSize)
    : _cache(totalCacheSize)
{}

FileDataCache::~FileDataCache()
{}

bool FileDataCache::put(const string& normPath, const string& content)
{
    int64_t ts = TimeUtility::currentTime();
    ScopedLock lock(_lock);
    if (_cache.put(normPath, content)) {
        AUTIL_LOG(INFO, "put file [%s] to innerDataCache, fileLength [%lu]",
                  normPath.c_str(), content.size());
        _metaMap[normPath] = ts;
        return true;
    }
    return false;
}

bool FileDataCache::get(const string& normPath, string& content, int64_t& ts)
{
    ScopedLock lock(_lock);
    auto iter = _metaMap.find(normPath);
    if (iter == _metaMap.end()) {
        return false;
    }
    ts = iter->second;
    return _cache.get(normPath, content);
}

void FileDataCache::removeFile(const string& normPath)
{
    ScopedLock lock(_lock);
    if (_metaMap.find(normPath) == _metaMap.end()) {
        return;
    }

    AUTIL_LOG(INFO, "remove file [%s] in innerDataCache", normPath.c_str());
    _cache.invalidate(normPath);
    _metaMap.erase(normPath);
}

void FileDataCache::removeDirectory(const string& normPath)
{
    ScopedLock lock(_lock);
    DirectoryMapIterator<int64_t> iterator(_metaMap, normPath);
    while (iterator.hasNext()) {
        auto curIt = iterator.next();
        _cache.invalidate(curIt->first);
        AUTIL_LOG(INFO, "remove file [%s] in innerDataCache", curIt->first.c_str());
        iterator.erase(curIt);
    }
}

int64_t FileDataCache::getCacheMemUse() const
{
    return _cache.getCacheSizeUsed();
}

int64_t FileDataCache::getCachedFileCount() const
{
    return _cache.getKeyCount();
}

FSLIB_END_NAMESPACE(cache);

