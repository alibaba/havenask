#include "indexlib/file_system/session_file_cache.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SessionFileCache);

SessionFileCache::SessionFileCache() {}

SessionFileCache:: ~SessionFileCache()
{
    for (typename ObjectCache::iterator it = _objectCache.begin();
         it != _objectCache.end(); it++)
    {
        SessionObjectMap &objMap = it->second;
        for (typename SessionObjectMap::iterator vit = objMap.begin(); vit != objMap.end(); vit++) {
            SessionObjectVec &objVec = vit->second;
            for (typename SessionObjectVec::iterator fit = objVec.begin(); fit != objVec.end(); fit++) {
                File* file = *fit;
                *fit = nullptr;
                if (file != nullptr && file->isOpened())
                {
                    ErrorCode ec = file->close();
                    if (ec != EC_OK)
                    {
                        IE_LOG(ERROR, "Close FAILED, file: [%s]", file->getFileName());
                        cerr<< "Close FAILED, file: " << file->getFileName() << endl;
                    }
                    else
                    {
                        IE_LOG(INFO, "Clear file in sessionCache: [%s]", file->getFileName());
                    }
                }
                DELETE_AND_SET_NULL(file);
            }
        }
    }
}

File* SessionFileCache::Get(const string& filePath, ssize_t fileLength) {
    pthread_t tid = pthread_self();
    SessionObjectMap *objMap = NULL;
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
    File* obj = nullptr;
    if (!objVec.empty()) {
        obj = objVec.front();
        objVec.pop_front();
        if (obj != nullptr && obj->seek(0, FILE_SEEK_SET) == EC_OK)
        {
            IE_LOG(INFO, "reuse file [%s] from session file cache, thread_id [%d]",
                   filePath.c_str(), (int)tid);
            return obj;
        }
    }
    IE_LOG(INFO, "open new file [%s] in session file cache, thread_id [%d]",
           filePath.c_str(), (int)tid);
    obj = FileSystem::openFile(filePath, READ, false, fileLength);
    return obj;
}

void SessionFileCache::Put(File* object, const string& filePath) {
    if (!object)
    {
        return;
    }

    if (!object->isOpened())
    {
        IE_LOG(ERROR, "recycle file [%s] in not opened", filePath.c_str());
        DELETE_AND_SET_NULL(object);
        return;
    }

    pthread_t tid = pthread_self();
    SessionObjectMap *poolMap = NULL;
    {
        ScopedReadLock rlock(_lock);
        typename ObjectCache::iterator it = _objectCache.find(tid);
        if (it != _objectCache.end()) {
            poolMap = &(it->second);
        }
    }

    if (!poolMap) {
        ErrorCode ec = object->close();
        if (ec != EC_OK)
        {
            IE_LOG(ERROR, "Close FAILED, file: [%s]", filePath.c_str());
            cerr<< "Close FAILED, file: " << filePath << endl;
        }
        DELETE_AND_SET_NULL(object);
        return;
    }
    SessionObjectVec& objVec = (*poolMap)[filePath];
    IE_LOG(INFO, "recycle file [%s] in session file cache, thread_id [%d]",
           filePath.c_str(), (int)tid);
    objVec.push_back(object);
}

IE_NAMESPACE_END(file_system);

