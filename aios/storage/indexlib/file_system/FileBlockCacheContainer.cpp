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
#include "indexlib/file_system/FileBlockCacheContainer.h"

#include "autil/StringUtil.h"
#include "indexlib/util/TaskScheduler.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileBlockCacheContainer);

FileBlockCacheContainer::FileBlockCacheContainer() {}

FileBlockCacheContainer::~FileBlockCacheContainer() {}

bool FileBlockCacheContainer::Init(const string& configStr, const MemoryQuotaControllerPtr& globalMemoryQuotaController,
                                   const TaskSchedulerPtr& taskScheduler, const MetricProviderPtr& metricProvider)
{
    autil::ScopedLock lock(_lock);
    _taskScheduler = taskScheduler;
    if (!_taskScheduler) {
        _taskScheduler = TaskSchedulerPtr(new TaskScheduler());
    }
    _metricProvider = metricProvider;
    vector<string> singleConfigStrVec;
    autil::StringUtil::fromString(configStr, singleConfigStrVec, "|");
    for (auto& singleConfigStr : singleConfigStrVec) {
        FileBlockCachePtr item(new FileBlockCache);
        if (!item->Init(singleConfigStr, globalMemoryQuotaController, _taskScheduler, metricProvider)) {
            return false;
        }

        if (GetFileBlockCache(item->GetLifeCycle()) != nullptr) {
            AUTIL_LOG(ERROR, "init global fileBlockCache with param [%s] fail, duplicate lifeCycle [%s]",
                      configStr.c_str(), item->GetLifeCycle().c_str());
            return false;
        }
        _globalFileBlockCacheMap[item->GetLifeCycle()] = item;
    }

    if (GetFileBlockCache("").get() == nullptr) {
        // make sure default cache must exist
        FileBlockCachePtr item(new FileBlockCache);
        if (!item->Init("", globalMemoryQuotaController, _taskScheduler, metricProvider)) {
            return false;
        }
        _globalFileBlockCacheMap[item->GetLifeCycle()] = item;
    }
    return true;
}

const FileBlockCachePtr& FileBlockCacheContainer::GetFileBlockCache(const string& lifeCycle) const
{
    autil::ScopedLock lock(_lock);
    auto iter = _globalFileBlockCacheMap.find(lifeCycle);
    if (iter == _globalFileBlockCacheMap.end()) {
        static FileBlockCachePtr nullFileCache;
        return nullFileCache;
    }
    return iter->second;
}

const FileBlockCachePtr& FileBlockCacheContainer::GetAvailableFileCache(const string& lifeCycle) const
{
    const FileBlockCachePtr& exactCache = GetFileBlockCache(lifeCycle);
    if (exactCache.get() != nullptr) {
        return exactCache;
    }
    // if no liftCycle exactly matched cache, will use default cache
    return GetFileBlockCache("");
}

const FileBlockCachePtr& FileBlockCacheContainer::GetAvailableFileCache(const string& fileSystemIdentifier,
                                                                        const string& loadConfigName) const
{
    autil::ScopedLock lock(_lock);
    auto iter = _localFileBlockCacheMap.find(make_pair(fileSystemIdentifier, loadConfigName));
    if (iter != _localFileBlockCacheMap.end()) {
        return iter->second.first;
    }
    static FileBlockCachePtr nullFileCache;
    return nullFileCache;
}

const FileBlockCachePtr&
FileBlockCacheContainer::RegisterLocalCache(const string& fileSystemIdentifier, const std::string& loadConfigName,
                                            const util::BlockCacheOption& option,
                                            const util::SimpleMemoryQuotaControllerPtr& quotaController)
{
    autil::ScopedLock lock(_lock);
    auto iter = _localFileBlockCacheMap.find(make_pair(fileSystemIdentifier, loadConfigName));
    if (iter != _localFileBlockCacheMap.end()) {
        ++iter->second.second;
        return iter->second.first;
    }
    FileBlockCachePtr cache(new FileBlockCache());
    std::map<std::string, std::string> tags;
    tags["identifier"] = fileSystemIdentifier;
    tags["load_config_name"] = loadConfigName;
    if (!cache->Init(option, quotaController, tags, _taskScheduler, _metricProvider)) {
        static FileBlockCachePtr nullFileCache;
        return nullFileCache;
    }
    _localFileBlockCacheMap[make_pair(fileSystemIdentifier, loadConfigName)] = make_pair(cache, 1);
    return _localFileBlockCacheMap[make_pair(fileSystemIdentifier, loadConfigName)].first;
}

void FileBlockCacheContainer::UnregisterLocalCache(const std::string& fileSystemIdentifier,
                                                   const std::string& loadConfigName)
{
    autil::ScopedLock lock(_lock);
    auto iter = _localFileBlockCacheMap.find(make_pair(fileSystemIdentifier, loadConfigName));
    assert(iter != _localFileBlockCacheMap.end());
    if (--iter->second.second == 0) {
        _localFileBlockCacheMap.erase(iter);
    }
}

util::CacheResourceInfo FileBlockCacheContainer::GetResourceInfo() const
{
    autil::ScopedLock lock(_lock);
    util::CacheResourceInfo ret;
    auto iter = _globalFileBlockCacheMap.begin();
    for (; iter != _globalFileBlockCacheMap.end(); iter++) {
        util::CacheResourceInfo currentUsage = iter->second->GetResourceInfo();
        ret.maxMemoryUse += currentUsage.maxMemoryUse;
        ret.memoryUse += currentUsage.memoryUse;
        ret.maxDiskUse += currentUsage.maxDiskUse;
        ret.diskUse += currentUsage.diskUse;
    }
    return ret;
}

}} // namespace indexlib::file_system
