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

#include <memory>

#include "autil/Lock.h"
#include "indexlib/file_system/FileBlockCache.h"

namespace indexlib { namespace file_system {

class FileBlockCacheContainer
{
public:
    FileBlockCacheContainer();
    ~FileBlockCacheContainer();

    FileBlockCacheContainer(const FileBlockCacheContainer&) = delete;
    FileBlockCacheContainer& operator=(const FileBlockCacheContainer&) = delete;
    FileBlockCacheContainer(FileBlockCacheContainer&&) = delete;
    FileBlockCacheContainer& operator=(FileBlockCacheContainer&&) = delete;

public:
    // for global block cache
    bool Init(const std::string& configStr, const util::MemoryQuotaControllerPtr& globalMemoryQuotaController,
              const std::shared_ptr<util::TaskScheduler>& taskScheduler = std::shared_ptr<util::TaskScheduler>(),
              const util::MetricProviderPtr& metricProvider = util::MetricProviderPtr());

    util::CacheResourceInfo GetResourceInfo() const;

    const FileBlockCachePtr& GetAvailableFileCache(const std::string& lifeCycle) const;
    const FileBlockCachePtr& GetAvailableFileCache(const std::string& fileSystemIdentifier,
                                                   const std::string& loadConfigName) const;

    const FileBlockCachePtr& RegisterLocalCache(const std::string& fileSystemIdentifier,
                                                const std::string& loadConfigName, const util::BlockCacheOption& option,
                                                const util::SimpleMemoryQuotaControllerPtr& quotaController);
    void UnregisterLocalCache(const std::string& fileSystemIdentifier, const std::string& loadConfigName);

private:
    const FileBlockCachePtr& GetFileBlockCache(const std::string& lifeCycle) const;

private:
    std::shared_ptr<util::TaskScheduler> _taskScheduler;
    util::MetricProviderPtr _metricProvider;
    std::map<std::string, FileBlockCachePtr> _globalFileBlockCacheMap;
    std::map<std::pair<std::string, std::string>, std::pair<FileBlockCachePtr, int>> _localFileBlockCacheMap;
    mutable autil::RecursiveThreadMutex _lock;

private:
    friend class FileBlockCacheContainerTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileBlockCacheContainer> FileBlockCacheContainerPtr;
}} // namespace indexlib::file_system
