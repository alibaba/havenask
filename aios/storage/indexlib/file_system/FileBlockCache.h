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

#include <atomic>
#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/util/cache/BlockCacheOption.h"
#include "indexlib/util/cache/CacheResourceInfo.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace util {
class TaskScheduler;
class BlockCache;
template <typename T>
class SimpleMemoryQuotaControllerType;
typedef SimpleMemoryQuotaControllerType<std::atomic<int64_t>> SimpleMemoryQuotaController;
typedef std::shared_ptr<SimpleMemoryQuotaController> SimpleMemoryQuotaControllerPtr;
}} // namespace indexlib::util

namespace indexlib { namespace file_system {

class FileBlockCache
{
public:
    FileBlockCache();
    ~FileBlockCache();

public:
    // for global block cache
    bool Init(const std::string& configStr, const util::MemoryQuotaControllerPtr& globalMemoryQuotaController,
              const std::shared_ptr<util::TaskScheduler>& taskScheduler = std::shared_ptr<util::TaskScheduler>(),
              const util::MetricProviderPtr& metricProvider = util::MetricProviderPtr());

public:
    // for local block cache
    bool Init(const util::BlockCacheOption& option, const util::SimpleMemoryQuotaControllerPtr& quotaController,
              const std::map<std::string, std::string>& metricsTags = std::map<std::string, std::string>(),
              const std::shared_ptr<util::TaskScheduler>& taskScheduler = std::shared_ptr<util::TaskScheduler>(),
              const util::MetricProviderPtr& metricProvider = util::MetricProviderPtr());

public:
    // common
    const std::shared_ptr<util::BlockCache>& GetBlockCache() const noexcept { return _blockCache; }

    util::CacheResourceInfo GetResourceInfo() const;

    static uint64_t GetFileId(const std::string& fileName);

    void ReportMetrics();

    const std::string& GetLifeCycle() const { return _lifeCycle; }

private:
    bool RegisterMetricsReporter(const util::MetricProviderPtr& metricProvider, bool isGlobal,
                                 const kmonitor::MetricsTags& metricsTags);

private:
    static constexpr const char* _CONFIG_SEPERATOR = ";";
    static constexpr const char* _CONFIG_KV_SEPERATOR = "=";
    // TODO (yiping.typ) replace @_CONFIG_CACHE_SIZE_NAME with @_CONFIG_MEMORY_SIZE_NAME, cache size is just for legacy
    static constexpr const char* _CONFIG_MEMORY_SIZE_NAME = "memory_size_in_mb";
    static constexpr const char* _CONFIG_DISK_SIZE_NAME = "disk_size_in_gb";
    static constexpr const char* _CONFIG_CACHE_SIZE_NAME = "cache_size";
    static constexpr const char* _CONFIG_BLOCK_SIZE_NAME = "block_size";
    static constexpr const char* _CONFIG_CACHE_TYPE_NAME = "cache_type";
    static constexpr const char* _CONFIG_TARGET_LIFE_CYCLE = "life_cycle";
    static constexpr const char* _CONFIG_BATCH_SIZE_NAME = "io_batch_size";
    static const size_t _DEFAULT_IO_BATCH_SIZE = 4; // 4

private:
    std::shared_ptr<util::BlockCache> _blockCache;
    util::SimpleMemoryQuotaControllerPtr _cacheMemController;
    util::MemoryQuotaControllerPtr _globalMemoryQuotaController;
    std::shared_ptr<util::TaskScheduler> _taskScheduler;
    int32_t _reportMetricsTaskId;
    std::string _lifeCycle;

private:
    friend class BlockFileNodeCreatorTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileBlockCache> FileBlockCachePtr;
}} // namespace indexlib::file_system
