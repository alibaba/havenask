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

#include <assert.h>
#include <deque>
#include <map>
#include <pthread.h>
#include <stddef.h>
#include <string>
#include <sys/types.h>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"

namespace fslib { namespace fs {
class File;
}} // namespace fslib::fs

namespace indexlib { namespace file_system {

class SessionFileCache
{
public:
    typedef std::deque<fslib::fs::File*> SessionObjectVec;
    typedef std::map<std::string, SessionObjectVec> SessionObjectMap;
    typedef std::map<pthread_t, SessionObjectMap> ObjectCache;

public:
    SessionFileCache() noexcept;
    virtual ~SessionFileCache();

private:
    SessionFileCache(const SessionFileCache&) = delete;
    SessionFileCache& operator=(const SessionFileCache&) = delete;

public:
    FSResult<fslib::fs::File*> Get(const std::string& filePath, ssize_t fileLength) noexcept;
    void Put(fslib::fs::File* object, const std::string& filePath) noexcept;
    size_t BucketSize() const noexcept { return _objectCache.size(); }

private:
    // FOR TEST
    size_t GetSessionFileCount(pthread_t tid, const std::string& filePath) noexcept
    {
        if (_objectCache.find(tid) == _objectCache.end()) {
            return 0;
        }
        SessionObjectMap& objMap = _objectCache[tid];
        if (objMap.find(filePath) == objMap.end()) {
            return 0;
        }
        return objMap[filePath].size();
    }

private:
    ObjectCache _objectCache;
    autil::ReadWriteLock _lock;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SessionFileCache> SessionFileCachePtr;

////////////////////////////////////////////////////////////////////////////////

}} // namespace indexlib::file_system
