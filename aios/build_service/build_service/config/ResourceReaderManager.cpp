#include "build_service/config/ResourceReaderManager.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, ResourceReaderManager);

ResourceReaderManager::ResourceReaderManager()
{
    _enableCache = true;
    // default interval is 10 second
    _clearLoopInterval = 60 * 1000000;
    _cacheExpireTime = 3600;
    {
        const char* param = getenv(BS_ENV_ENABLE_RESOURCE_READER_CACHE.c_str());
        bool enableCache;
        if (param != NULL &&
            StringUtil::fromString(string(param), enableCache))
        {
            _enableCache = enableCache;
        }
    }
    if (!_enableCache)
    {
        return;
    }

    {
        const char* param = getenv(BS_ENV_ENABLE_RESOURCE_READER_CACHE_EXPIRE_TIME.c_str());
        int64_t expireTime;
        if (param != NULL &&
            StringUtil::fromString(string(param), expireTime))
        {
            _cacheExpireTime = expireTime;
        }
    }
    
    #ifdef NDEBUG
    {
        const char* param = getenv(BS_ENV_ENABLE_RESOURCE_READER_CACHE_CLEAR_INTERVAL.c_str());
        int64_t clearInterval;
        if (param != NULL &&
            StringUtil::fromString(string(param), clearInterval))
        {
            _clearLoopInterval = clearInterval * 1000000;
        }
    }
    #else
    _clearLoopInterval = 2 * 1000000;
    #endif
    BS_LOG(INFO, "clear interval %ld, cache expire time %ld.", _clearLoopInterval, _cacheExpireTime);
    _loopThread = LoopThread::createLoopThread(
        tr1::bind(&ResourceReaderManager::clearLoop, this),
        _clearLoopInterval, "BsResClear");
    if (!_loopThread)
    {
        string errorMsg = "failed to create loop thread for ResourceReaderManger";
        cerr << errorMsg;
    }
}

ResourceReaderManager::~ResourceReaderManager()
{
}
    
ResourceReaderPtr ResourceReaderManager::getResourceReader(const std::string& configPath)
{
    ResourceReaderManager* manager = ResourceReaderManager::GetInstance();
    ResourceReaderPtr reader;
    if (!manager->_enableCache)
    {
        reader.reset(new ResourceReader(configPath));
        reader->init();
        return reader;
    }

    {
        autil::ScopedLock lock(manager->_cacheMapLock);
        auto iter = manager->_readerCache.find(configPath);
        int64_t ts = TimeUtility::currentTimeInSeconds();
        if (iter != manager->_readerCache.end()) {
            // if reader is in cache, update its timestamp
            iter->second.second = ts;
            return iter->second.first;
        }
        reader.reset(new ResourceReader(configPath));
        manager->_readerCache[configPath] = make_pair(reader, ts);
        BS_LOG(INFO, "add [%s:%ld] to readerCache[%zu].",
               configPath.c_str(), ts, manager->_readerCache.size());
        reader->init();
    }
    assert(reader);
    return reader;
}

void ResourceReaderManager::clearLoop()
{
    autil::ScopedLock lock(_cacheMapLock);
    auto iter = _readerCache.begin();
    vector<string> rmQueue;
    int64_t ts = TimeUtility::currentTimeInSeconds();
    while(iter != _readerCache.end()) {
        if (iter->second.first.use_count() <= 1 &&
            ts - iter->second.second >= _cacheExpireTime) {
            rmQueue.push_back(iter->first);
        }
        iter++;
    }
    for (auto configPath : rmQueue) {
        _readerCache.erase(configPath);
        cerr << "remove readerCache[" << configPath << "]" << endl;
    }
}
    
}
}
