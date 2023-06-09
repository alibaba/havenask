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
#include "build_service/config/ResourceReaderManager.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace build_service { namespace config {
BS_LOG_SETUP(config, ResourceReaderManager);

ResourceReaderManager::ResourceReaderManager()
{
    _enableCache = true;
    _close = false;
    // default interval is 10 second
    _clearLoopInterval = 60 * 1000000;
    _cacheExpireTime = 3600;
    {
        const char* param = getenv(BS_ENV_ENABLE_RESOURCE_READER_CACHE.c_str());
        bool enableCache;
        if (param != NULL && StringUtil::fromString(string(param), enableCache)) {
            _enableCache = enableCache;
        }
    }
    if (!_enableCache) {
        return;
    }

    {
        const char* param = getenv(BS_ENV_ENABLE_RESOURCE_READER_CACHE_EXPIRE_TIME.c_str());
        int64_t expireTime;
        if (param != NULL && StringUtil::fromString(string(param), expireTime)) {
            _cacheExpireTime = expireTime;
        }
    }

#ifdef NDEBUG
    {
        const char* param = getenv(BS_ENV_ENABLE_RESOURCE_READER_CACHE_CLEAR_INTERVAL.c_str());
        int64_t clearInterval;
        if (param != NULL && StringUtil::fromString(string(param), clearInterval)) {
            _clearLoopInterval = clearInterval * 1000000;
        }
    }
#else
    _clearLoopInterval = 2 * 1000000;
#endif

    BS_LOG(INFO, "clear interval %ld, cache expire time %ld.", _clearLoopInterval, _cacheExpireTime);
    _loopThread =
        LoopThread::createLoopThread(bind(&ResourceReaderManager::clearLoop, this), _clearLoopInterval, "BsResClear");
    if (!_loopThread) {
        string errorMsg = "failed to create loop thread for ResourceReaderManger";
        cerr << errorMsg;
    }
}

ResourceReaderManager::~ResourceReaderManager()
{
    _close = true;
    _loopThread.reset();
}

ResourceReaderPtr ResourceReaderManager::getResourceReader(const std::string& configPath)
{
    ResourceReaderManager* manager = ResourceReaderManager::GetInstance();
    ResourceReaderPtr reader;
    if (!manager->_enableCache) {
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
            // return iter->second.first;
            reader = iter->second.first;
        }
        if (reader) {
            if (reader->isInit()) {
                return reader;
            }
            AUTIL_LOG(WARN, "Get ResourceReader[%s] success, but ResourceReader init has not completed, wait.",
                      configPath.c_str());
        } else {
            reader.reset(new ResourceReader(configPath));
            manager->_readerCache[configPath] = make_pair(reader, ts);
            BS_LOG(INFO, "add [%s:%ld] to readerCache[%zu].", configPath.c_str(), ts, manager->_readerCache.size());
        }
    }
    reader->init();
    assert(reader);
    return reader;
}

void ResourceReaderManager::clearLoop()
{
    if (_close) {
        return;
    }
    {
        autil::ScopedLock lock(_cacheMapLock);
        auto iter = _readerCache.begin();
        vector<string> rmQueue;
        int64_t ts = TimeUtility::currentTimeInSeconds();
        while (iter != _readerCache.end()) {
            if (iter->second.first.use_count() <= 1 && ts - iter->second.second >= _cacheExpireTime) {
                rmQueue.push_back(iter->first);
            }
            iter++;
        }
        for (const auto& configPath : rmQueue) {
            _readerCache.erase(configPath);
            cerr << "remove readerCache[" << configPath << "]" << endl;
        }
    }
}

}} // namespace build_service::config
