#ifndef __INDEXLIB_SESSION_FILE_CACHE_H
#define __INDEXLIB_SESSION_FILE_CACHE_H

#include <tr1/memory>
#include <autil/Lock.h>
#include <pthread.h>
#include <deque>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(file_system);

class SessionFileCache
{
public:
    typedef std::deque<fslib::fs::File*> SessionObjectVec;
    typedef std::map<std::string, SessionObjectVec> SessionObjectMap;
    typedef std::map<pthread_t, SessionObjectMap> ObjectCache;
public:
    SessionFileCache();
    virtual ~SessionFileCache();
    
private:
    SessionFileCache(const SessionFileCache &);
    SessionFileCache& operator = (const SessionFileCache &);
    
public:
    fslib::fs::File* Get(const std::string& filePath, ssize_t fileLength);
    void Put(fslib::fs::File* object, const std::string& filePath);
    size_t BucketSize() const { return _objectCache.size(); }

private:
    // FOR TEST
    size_t GetSessionFileCount(pthread_t tid, const std::string& filePath)
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
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SessionFileCache);

////////////////////////////////////////////////////////////////////////////////
        
class FileCacheGuard {
public:
    FileCacheGuard(SessionFileCache *owner, const std::string& filePath, ssize_t fileLength = -1)
        : _owner(owner)
    {
        if (_owner) {
            _filePath = filePath;
            _object = _owner->Get(filePath, fileLength);
        }
    }

    virtual ~FileCacheGuard() {
        if (_object != NULL) {
            _owner->Put(_object, _filePath);
        }
    }
public:
    fslib::fs::File* Get() const {
        return _object;
    }
    fslib::fs::File* Steal() {
        fslib::fs::File* obj = _object;
        _object = nullptr;
        return obj;
    }
private:
    SessionFileCache *_owner;
    fslib::fs::File* _object;
    std::string _filePath;
};

DEFINE_SHARED_PTR(SessionFileCache);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SESSION_FILE_CACHE_H
