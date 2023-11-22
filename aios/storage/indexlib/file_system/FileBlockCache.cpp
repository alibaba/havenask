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
#include "indexlib/file_system/FileBlockCache.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/MurmurHash.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/file_system/FileBlockCacheTaskItem.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/TaskItem.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/cache/BlockCacheCreator.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileBlockCache);

FileBlockCache::FileBlockCache() : _reportMetricsTaskId(TaskScheduler::INVALID_TASK_ID) {}

FileBlockCache::~FileBlockCache()
{
    if (_globalMemoryQuotaController && _blockCache) {
        _globalMemoryQuotaController->Free(GetResourceInfo().maxMemoryUse);
    }
    if (_taskScheduler) {
        _taskScheduler->DeleteTask(_reportMetricsTaskId);
    }
}

bool FileBlockCache::Init(const std::string& configStr,
                          const util::MemoryQuotaControllerPtr& globalMemoryQuotaController,
                          const std::shared_ptr<util::TaskScheduler>& taskScheduler,
                          const util::MetricProviderPtr& metricProvider)
{
    _globalMemoryQuotaController = globalMemoryQuotaController;
    _taskScheduler = taskScheduler;

    vector<vector<string>> paramVec;
    StringUtil::fromString(configStr, paramVec, _CONFIG_KV_SEPERATOR, _CONFIG_SEPERATOR);

    BlockCacheOption option;
    option.memorySize /= (1024 * 1024); // byte -> mb
    option.ioBatchSize = _DEFAULT_IO_BATCH_SIZE;

    option.diskSize /= (1024UL * 1024 * 1024); // byte -> gb
    _lifeCycle = "";

    set<string> builtinParamKeys = {_CONFIG_CACHE_SIZE_NAME, _CONFIG_MEMORY_SIZE_NAME, _CONFIG_BLOCK_SIZE_NAME,
                                    _CONFIG_CACHE_TYPE_NAME, _CONFIG_TARGET_LIFE_CYCLE};
    for (const auto& param : paramVec) {
        if (param.size() != 2) {
            AUTIL_LOG(ERROR, "parse block cache param[%s] failed", configStr.c_str());
            return false;
        }
        if (param[0] == _CONFIG_CACHE_SIZE_NAME && StringUtil::fromString(param[1], option.memorySize)) {
            continue;
        } else if (param[0] == _CONFIG_MEMORY_SIZE_NAME && StringUtil::fromString(param[1], option.memorySize)) {
            continue;
        } else if (param[0] == _CONFIG_DISK_SIZE_NAME && StringUtil::fromString(param[1], option.diskSize)) {
            continue;
        } else if (param[0] == _CONFIG_CACHE_TYPE_NAME) {
            option.cacheType = param[1];
            continue;
        } else if (param[0] == _CONFIG_BLOCK_SIZE_NAME && StringUtil::fromString(param[1], option.blockSize)) {
            continue;
        } else if (param[0] == _CONFIG_BATCH_SIZE_NAME && StringUtil::fromString(param[1], option.ioBatchSize)) {
            continue;
        } else if (param[0] == _CONFIG_TARGET_LIFE_CYCLE) {
            _lifeCycle = param[1];
            continue;
        } else if (builtinParamKeys.find(param[0]) == builtinParamKeys.end()) {
            // other key we just see them as detail key about the detail block type
            option.cacheParams[param[0]] = param[1];
            continue;
        }

        AUTIL_LOG(ERROR, "parse block cache param[%s] failed, unexpected param key[%s] and value[%s]",
                  configStr.c_str(), param[0].c_str(), param[1].c_str());
        return false;
    }
    option.memorySize *= (1024 * 1024);        // mb -> byte
    option.diskSize *= (1024UL * 1024 * 1024); // gb -> byte

    _blockCache.reset(BlockCacheCreator::Create(option));
    if (!_blockCache) {
        AUTIL_LOG(ERROR, "create block cache failed, with config[%s]", configStr.c_str());
        return false;
    }

    if (_globalMemoryQuotaController) {
        _globalMemoryQuotaController->Allocate(GetResourceInfo().maxMemoryUse);
    }

    AUTIL_LOG(INFO,
              "init file block cache with[%s]: "
              "memorySize[%zu], blockSize[%zu], ioBatchSize[%zu], cacheType[%s], param [%s]",
              configStr.c_str(), option.memorySize, option.blockSize, option.ioBatchSize, option.cacheType.c_str(),
              autil::legacy::ToJsonString(option.cacheParams, true).c_str());

    kmonitor::MetricsTags tags;
    string cycle = _lifeCycle.empty() ? "default" : _lifeCycle;
    tags.AddTag("life_cycle", cycle);
    return RegisterMetricsReporter(metricProvider, true, tags);
}

bool FileBlockCache::Init(const util::BlockCacheOption& option,
                          const util::SimpleMemoryQuotaControllerPtr& quotaController,
                          const std::map<std::string, std::string>& metricsTags,
                          const std::shared_ptr<util::TaskScheduler>& taskScheduler,
                          const util::MetricProviderPtr& metricProvider)
{
    _taskScheduler = taskScheduler;
    if (option.memorySize != 0 && option.memorySize < option.blockSize) {
        AUTIL_LOG(ERROR, "cache size [%lu] lees than block size [%lu] ", option.memorySize, option.blockSize);
        return false;
    }

    _blockCache.reset(BlockCacheCreator::Create(option));
    if (!_blockCache) {
        AUTIL_LOG(ERROR, "create block cache failed");
        return false;
    }

    _cacheMemController = quotaController;
    if (_cacheMemController) {
        _cacheMemController->Allocate(GetResourceInfo().maxMemoryUse);
    }
    kmonitor::MetricsTags tags;
    for (auto iter = metricsTags.begin(); iter != metricsTags.end(); ++iter) {
        tags.AddTag(iter->first, iter->second);
    }
    return RegisterMetricsReporter(metricProvider, false, tags);
}

bool FileBlockCache::RegisterMetricsReporter(const util::MetricProviderPtr& metricProvider, bool isGlobal,
                                             const kmonitor::MetricsTags& metricsTags)
{
    if (!_taskScheduler || !metricProvider) {
        return true;
    }

    bool testMode = autil::EnvUtil::getEnv("TEST_QUICK_EXIT", false);
    int32_t sleepTime = indexlibv2::REPORT_METRICS_INTERVAL;
    if (testMode) {
        sleepTime /= 1000;
    }
    if (!_taskScheduler->DeclareTaskGroup("report_metrics", sleepTime)) {
        AUTIL_LOG(ERROR, "global file block cache declare report metrics task failed!");
        return false;
    }
    TaskItemPtr reportMetricsTask(new FileBlockCacheTaskItem(_blockCache.get(), metricProvider, isGlobal, metricsTags));
    _reportMetricsTaskId = _taskScheduler->AddTask("report_metrics", reportMetricsTask);
    if (_reportMetricsTaskId == TaskScheduler::INVALID_TASK_ID) {
        AUTIL_LOG(ERROR, "global file block cache add report metrics task failed!");
        return false;
    }
    std::string prefix = "global";
    if (!isGlobal) {
        prefix = "local";
    }
    _blockCache->RegisterMetrics(metricProvider, prefix, metricsTags);
    return true;
}

uint64_t FileBlockCache::GetFileId(const string& fileName)
{
    // assert fileName is not a logical path (in bazel ut physical path may start with '.')
    assert(!fileName.empty());
    // assert(fileName.find("://") != std::string::npos || fileName[0] == '.' || fileName[0] == '/');
    return MurmurHash::MurmurHash64A(fileName.c_str(), fileName.length(), 545609244UL);
}

CacheResourceInfo FileBlockCache::GetResourceInfo() const
{
    CacheResourceInfo info;
    if (_blockCache) {
        info = _blockCache->GetResourceInfo();
    }
    return info;
}

void FileBlockCache::ReportMetrics() { return _blockCache->ReportMetrics(); }
}} // namespace indexlib::file_system
