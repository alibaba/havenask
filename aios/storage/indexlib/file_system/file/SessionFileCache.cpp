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
#include "indexlib/file_system/file/SessionFileCache.h"

#include <iostream>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, SessionFileCache);

SessionFileCache::SessionFileCache() noexcept {}

SessionFileCache::~SessionFileCache()
{
    for (typename ObjectCache::iterator it = _objectCache.begin(); it != _objectCache.end(); it++) {
        SessionObjectMap& objMap = it->second;
        for (typename SessionObjectMap::iterator vit = objMap.begin(); vit != objMap.end(); vit++) {
            SessionObjectVec& objVec = vit->second;
            for (typename SessionObjectVec::iterator fit = objVec.begin(); fit != objVec.end(); fit++) {
                fslib::fs::File* file = *fit;
                *fit = nullptr;
                if (file != nullptr && file->isOpened()) {
                    fslib::ErrorCode ec = file->close();
                    if (ec != fslib::EC_OK) {
                        AUTIL_LOG(ERROR, "Close FAILED, file: [%s]", file->getFileName());
                        cerr << "Close FAILED, file: " << file->getFileName() << endl;
                    } else {
                        AUTIL_LOG(INFO, "Clear file in sessionCache: [%s]", file->getFileName());
                    }
                }
                DELETE_AND_SET_NULL(file);
            }
        }
    }
}

FSResult<fslib::fs::File*> SessionFileCache::Get(const string& filePath, ssize_t fileLength) noexcept
{
    pthread_t tid = pthread_self();
    SessionObjectMap* objMap = NULL;
    {
        ScopedReadLock rlock(_lock);
        typename ObjectCache::iterator it = _objectCache.find(tid);
        if (it != _objectCache.end()) {
            objMap = &(it->second);
        }
    }

    if (objMap == NULL) {
        ScopedWriteLock wlock(_lock);
        typename ObjectCache::iterator it = _objectCache.find(tid);
        if (it == _objectCache.end()) {
            _objectCache[tid] = SessionObjectMap();
            objMap = &(_objectCache[tid]);
        } else {
            objMap = &(it->second);
        }
    }
    assert(objMap != NULL);
    SessionObjectVec& objVec = (*objMap)[filePath];
    fslib::fs::File* obj = nullptr;
    if (!objVec.empty()) {
        obj = objVec.front();
        objVec.pop_front();
        if (obj != nullptr && obj->seek(0, fslib::FILE_SEEK_SET) == fslib::EC_OK) {
            AUTIL_LOG(INFO, "reuse file [%s] from session file cache, thread_id [%d]", filePath.c_str(), (int)tid);
            return {FSEC_OK, obj};
        }
    }
    AUTIL_LOG(INFO, "open new file [%s] in session file cache, thread_id [%d]", filePath.c_str(), (int)tid);
    obj = fslib::fs::FileSystem::openFile(filePath, fslib::READ, false, fileLength);
    if (!obj || !obj->isOpened()) {
        AUTIL_LOG(ERROR, "open file [%s] failed", filePath.c_str());
        return {FSEC_ERROR, nullptr};
    }
    return {FSEC_OK, obj};
}

void SessionFileCache::Put(fslib::fs::File* object, const string& filePath) noexcept
{
    if (!object) {
        return;
    }

    if (!object->isOpened()) {
        AUTIL_LOG(ERROR, "recycle file [%s] in not opened", filePath.c_str());
        DELETE_AND_SET_NULL(object);
        return;
    }

    pthread_t tid = pthread_self();
    SessionObjectMap* poolMap = NULL;
    {
        ScopedReadLock rlock(_lock);
        typename ObjectCache::iterator it = _objectCache.find(tid);
        if (it != _objectCache.end()) {
            poolMap = &(it->second);
        }
    }

    if (!poolMap) {
        fslib::ErrorCode ec = object->close();
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(ERROR, "Close FAILED, file: [%s]", filePath.c_str());
            cerr << "Close FAILED, file: " << filePath << endl;
        }
        DELETE_AND_SET_NULL(object);
        return;
    }
    SessionObjectVec& objVec = (*poolMap)[filePath];
    AUTIL_LOG(INFO, "recycle file [%s] in session file cache, thread_id [%d]", filePath.c_str(), (int)tid);
    objVec.push_back(object);
}
}} // namespace indexlib::file_system
