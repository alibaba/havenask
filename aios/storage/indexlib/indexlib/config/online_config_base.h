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

#include "autil/Log.h"
#include "indexlib/config/build_config.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/kv_online_config.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class OnlineConfigBase : public autil::legacy::Jsonizable
{
public:
    static const int64_t DEFAULT_MAX_REALTIME_MEMORY_SIZE = 8 * 1024; // 8G
    static const uint32_t DEFAULT_MAX_REALTIME_DUMP_INTERVAL = (uint32_t)-1;
    static const uint32_t DEFAULT_ONLINE_KEEP_VERSION_COUNT = 10;
    static const uint64_t DEFAULT_BUILD_TOTAL_MEMORY = 1 * 1024; // 1G
    static const uint32_t DEFAULT_REALTIME_BUILD_HASHMAP_INIT_SIZE = HASHMAP_INIT_SIZE;
    static const uint32_t DEFAULT_REALTIME_DUMP_THREAD_COUNT = 1;
    static const int64_t INVALID_MAX_REOPEN_MEMORY_USE = (int64_t)-1;
    static const bool DEFAULT_REALTIME_BUILD_USE_PACKAGE_FILE = false;
    static const int64_t DEFAULT_MAX_CUR_READER_RECLAIMABLE_MEM = 1024;
    static const int64_t DEFAULT_PRINT_METRICS_INTERVAL = 1200; // 20 min
    static const int64_t INVALID_MAX_SWAP_MMAP_FILE_SIZE = -1;
    static const uint32_t DEFAULT_LOAD_PATCH_THREAD_NUM = 1;

public:
    OnlineConfigBase();
    virtual ~OnlineConfigBase();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

    void SetMaxReopenMemoryUse(int64_t maxReopenMemUse) { maxReopenMemoryUse = maxReopenMemUse; }

    bool NeedReadRemoteIndex() const { return needReadRemoteIndex; }
    void SetNeedReadRemoteIndex(bool value) { needReadRemoteIndex = value; }

    void SetNeedDeployIndex(bool needDpIndex) { needDeployIndex = needDpIndex; }
    bool NeedDeployIndex() const { return needDeployIndex; }

    bool GetMaxReopenMemoryUse(int64_t& maxReopenMemUse) const
    {
        maxReopenMemUse = maxReopenMemoryUse;
        return maxReopenMemoryUse != INVALID_MAX_REOPEN_MEMORY_USE;
    }

    bool EnableIntervalDump() const { return maxRealtimeDumpInterval != DEFAULT_MAX_REALTIME_DUMP_INTERVAL; }

public:
    // attention: do not add new member to this class
    // U can add new member to OnlineConfigImpl

    size_t maxOperationQueueBlockSize;
    int64_t maxCurReaderReclaimableMem;
    int64_t maxRealtimeMemSize; // maxIndexSize
    uint32_t maxRealtimeDumpInterval;
    uint32_t onlineKeepVersionCount;
    int64_t ttl; // in second
    bool onDiskFlushRealtimeIndex;
    bool loadRemainFlushRealtimeIndex;
    bool isIncConsistentWithRealtime; // update doc consistency
    bool enableAsyncCleanResource;
    bool enableLoadSpeedControlForOpen;
    bool enableCompressOperationBlock;
    bool enableSearchCache;
    bool useHighPriorityCache;
    LoadConfigList loadConfigList;
    BuildConfig buildConfig;
    bool enableRecoverIndex;
    RecoverStrategyType recoverType;
    bool enableAsyncFlushIndex;
    int64_t printMetricsInterval;
    bool disableSsePforDeltaOptimize;
    bool useSwapMmapFile;
    bool warmUpSwapFileWhenDump;
    bool prohibitInMemDump;
    int64_t maxSwapMmapFileSize;
    bool enableAsyncDumpSegment;
    KVOnlineConfig kvOnlineConfig;
    bool enableForceOpen; // when forceReopen failed, this option will drop building realtime data and trigger open
    bool disableLoadCustomizedIndex;
    uint32_t loadPatchThreadNum;

private:
    int64_t maxReopenMemoryUse;
    bool needReadRemoteIndex;
    bool needDeployIndex;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<OnlineConfigBase> OnlineConfigBasePtr;
}} // namespace indexlib::config
