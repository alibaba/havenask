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
#include "autil/ThreadPoolManager.h"

#include <cstddef>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"

using namespace std;
namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, ThreadPoolManager);

ThreadPoolManager::ThreadPoolManager() {
}

ThreadPoolManager::~ThreadPoolManager() {
    for (map<string, ThreadPool*>::iterator it = _pools.begin();
         it != _pools.end(); ++it)
    {
        delete it->second;
    }
    _pools.clear();
}

bool ThreadPoolManager::addThreadPool(const string& threadPoolsConfigStr) {
    StringTokenizer st1(threadPoolsConfigStr, ";",
                        StringTokenizer::TOKEN_IGNORE_EMPTY |
                        StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator iter = st1.begin(); iter != st1.end(); iter++) {
        StringTokenizer st2(*iter, "|",
                            StringTokenizer::TOKEN_IGNORE_EMPTY |
                            StringTokenizer::TOKEN_TRIM);
        if (st2.getNumTokens() != 3) {
            AUTIL_LOG(ERROR, "parse threadPoolConfigStr[%s] failed!",
                    threadPoolsConfigStr.c_str());
            return false;
        }
        string poolName = st2[0];
        if (_pools.end() != _pools.find(poolName)) {
            AUTIL_LOG(ERROR, "parse threadPoolConfigStr[%s] failed!",
                    threadPoolsConfigStr.c_str());
            return false;
        }

        uint32_t taskQueueSize;
        uint32_t threadNum;
        if (!(StringUtil::fromString<uint32_t>(st2[1], taskQueueSize) &&
              StringUtil::toString(taskQueueSize) == st2[1])) {
            AUTIL_LOG(ERROR, "parse threadPoolConfigStr[%s] failed!",
                    threadPoolsConfigStr.c_str());
            return false;
        }
        if (!(StringUtil::fromString<uint32_t>(st2[2], threadNum) &&
              StringUtil::toString(threadNum) == st2[2])) {
            AUTIL_LOG(ERROR, "parse threadPoolConfigStr[%s] failed!",
                    threadPoolsConfigStr.c_str());
            return false;
        }

        ThreadPool *pool = new ThreadPool(threadNum, taskQueueSize);
        _pools[poolName] = pool;
    }
    return true;
}

bool ThreadPoolManager::addThreadPool(const string&poolName,
                                       int32_t queueSize, int32_t threadNum)
{
    if (_pools.end() != _pools.find(poolName)) {
        AUTIL_LOG(ERROR, "add thread pool[%s] failed, pool name already existed.",
                poolName.c_str());
        return false;
    }

    ThreadPool *pool = new ThreadPool(threadNum, queueSize);
    _pools[poolName] = pool;
    return true;
}

bool ThreadPoolManager::start() {
    bool ret = true;
    for (map<string, ThreadPool*>::iterator it = _pools.begin();
         it != _pools.end(); ++it)
    {
        AUTIL_LOG(INFO, "start pool [%s]", it->first.c_str());
        bool success = it->second->start(it->first);
        ret = ret && success;
    }
    return ret;
}

void ThreadPoolManager::stop(ThreadPool::STOP_TYPE stopType) {
    for (map<string, ThreadPool*>::iterator it = _pools.begin();
         it != _pools.end(); ++it)
    {
        it->second->stop(stopType);
    }
}

size_t ThreadPoolManager::getItemCount() const {
    size_t itemCount = 0;
    for (map<string, ThreadPool*>::const_iterator it = _pools.begin();
         it != _pools.end(); ++it)
    {
        itemCount += it->second->getItemCount();
    }
    return itemCount;
}

size_t ThreadPoolManager::getTotalThreadNum() const {
    size_t threadNum = 0;
    for (map<string, ThreadPool*>::const_iterator it = _pools.begin();
         it != _pools.end(); ++it)
    {
        threadNum += it->second->getThreadNum();
    }
    return threadNum;
}

size_t ThreadPoolManager::getTotalQueueSize() const {
    size_t queueSize = 0;
    for (map<string, ThreadPool*>::const_iterator it = _pools.begin();
         it != _pools.end(); ++it)
    {
        queueSize += it->second->getQueueSize();
    }
    return queueSize;
}

std::map<std::string, size_t> ThreadPoolManager::getQueueSizeMap() const {
    std::map<std::string, size_t> ret;
    for (auto &&poolItem : _pools) {
        ret[poolItem.first] = poolItem.second->getQueueSize();
    }
    return ret;
}

std::map<std::string, size_t> ThreadPoolManager::getItemCountMap() const {
    std::map<std::string, size_t> ret;
    for (auto &&poolItem : _pools) {
        ret[poolItem.first] = poolItem.second->getItemCount();
    }
    return ret;
}

std::map<std::string, size_t> ThreadPoolManager::getActiveThreadCount() const{
    std::map<std::string, size_t> ret;
    for (auto&& poolItem : _pools) {
        ret[poolItem.first] = poolItem.second->getActiveThreadNum();
    }
    return ret;
}

std::map<std::string, size_t> ThreadPoolManager::getTotalThreadCount() const{
    std::map<std::string, size_t> ret;
    for (auto&& poolItem : _pools) {
        ret[poolItem.first] = poolItem.second->getThreadNum();
    }
    return ret;
}

ThreadPool* ThreadPoolManager::getThreadPool(const string&poolName) {
    map<string, ThreadPool*>::iterator it = _pools.find(poolName);
    if (_pools.end() == it) {
        return NULL;
    }
    return it->second;
}

}
