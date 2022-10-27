#include <ha3/service/ThreadPoolManager.h>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, ThreadPoolManager);

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
            HA3_LOG(ERROR, "parse threadPoolConfigStr[%s] failed!", 
                    threadPoolsConfigStr.c_str());
            return false;
        }
        string poolName = st2[0];
        if (_pools.end() != _pools.find(poolName)) {
            HA3_LOG(ERROR, "parse threadPoolConfigStr[%s] failed!", 
                    threadPoolsConfigStr.c_str());
            return false;
        }

        uint32_t taskQueueSize;
        uint32_t threadNum;
        if (!(StringUtil::fromString<uint32_t>(st2[1], taskQueueSize) &&
              StringUtil::toString(taskQueueSize) == st2[1])) {
            HA3_LOG(ERROR, "parse threadPoolConfigStr[%s] failed!", 
                    threadPoolsConfigStr.c_str());
            return false;
        }
        if (!(StringUtil::fromString<uint32_t>(st2[2], threadNum) &&
              StringUtil::toString(threadNum) == st2[2])) {
            HA3_LOG(ERROR, "parse threadPoolConfigStr[%s] failed!", 
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
        HA3_LOG(ERROR, "add thread pool[%s] failed, pool name already existed.",
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
        bool success = it->second->start();
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

END_HA3_NAMESPACE(service);

