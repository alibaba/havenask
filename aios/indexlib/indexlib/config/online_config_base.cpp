#include "indexlib/config/online_config_base.h"
#include "indexlib/config/build_config_base.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, OnlineConfigBase);

OnlineConfigBase::OnlineConfigBase()
    : maxOperationQueueBlockSize(DEFAULT_MAX_OPERATION_BLOCK_SIZE)
    , maxCurReaderReclaimableMem(DEFAULT_MAX_CUR_READER_RECLAIMABLE_MEM)
    , maxRealtimeMemSize(DEFAULT_MAX_REALTIME_MEMORY_SIZE)
    , maxRealtimeDumpInterval(DEFAULT_MAX_REALTIME_DUMP_INTERVAL)
    , onlineKeepVersionCount(DEFAULT_ONLINE_KEEP_VERSION_COUNT)
    , ttl(INVALID_TTL)
    , onDiskFlushRealtimeIndex(false)
    , loadRemainFlushRealtimeIndex(false)
    , isIncConsistentWithRealtime(false)
    , enableAsyncCleanResource(true)
    , enableLoadSpeedControlForOpen(false)
    , enableCompressOperationBlock(true)
    , enableSearchCache(true)
    , enableRecoverIndex(false)
    , recoverType(RecoverStrategyType::RST_SEGMENT_LEVEL)
    , enableAsyncFlushIndex(true)
    , printMetricsInterval(DEFAULT_PRINT_METRICS_INTERVAL)
    , disableSsePforDeltaOptimize(false)
    , useSwapMmapFile(false)
    , warmUpSwapFileWhenDump(false)
    , prohibitInMemDump(false)
    , maxSwapMmapFileSize(INVALID_MAX_SWAP_MMAP_FILE_SIZE)
    , enableAsyncDumpSegment(false)
    , enableForceOpen(true)
    , disableLoadCustomizedIndex(false)
    , loadPatchThreadNum(DEFAULT_LOAD_PATCH_THREAD_NUM)
    , maxReopenMemoryUse(INVALID_MAX_REOPEN_MEMORY_USE)
    , needReadRemoteIndex(false)
    , needDeployIndex(true)
{
    int64_t envMem = 0;
    if (BuildConfig::GetEnvBuildTotalMemUse(envMem))
    {
        buildConfig.buildTotalMemory = envMem;
    }
    else
    {
        buildConfig.buildTotalMemory = DEFAULT_BUILD_TOTAL_MEMORY;        
    }
    buildConfig.hashMapInitSize = DEFAULT_REALTIME_BUILD_HASHMAP_INIT_SIZE;
    buildConfig.dumpThreadCount = DEFAULT_REALTIME_DUMP_THREAD_COUNT;
    buildConfig.enablePackageFile = DEFAULT_REALTIME_BUILD_USE_PACKAGE_FILE;
    buildConfig.buildPhase = BuildConfigBase::BuildPhase::BP_RT;
    buildConfig.usePathMetaCache = false;
}

OnlineConfigBase::~OnlineConfigBase() 
{
}

void OnlineConfigBase::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("max_operation_queue_block_size", maxOperationQueueBlockSize,
                 maxOperationQueueBlockSize);
    json.Jsonize("var_num_attribute_memory_reclaim_threshold",
                 maxCurReaderReclaimableMem, maxCurReaderReclaimableMem);
    json.Jsonize("max_realtime_memory_use", maxRealtimeMemSize, 
                 maxRealtimeMemSize);
    json.Jsonize("max_realtime_dump_interval", maxRealtimeDumpInterval,
                 maxRealtimeDumpInterval);
    json.Jsonize("online_keep_version_count", onlineKeepVersionCount,
                 onlineKeepVersionCount);
    json.Jsonize("time_to_live", ttl, ttl);
    json.Jsonize("on_disk_flush_realtime_index", onDiskFlushRealtimeIndex,
                 onDiskFlushRealtimeIndex);
    json.Jsonize("load_remain_flush_realtime_index", loadRemainFlushRealtimeIndex,
                 loadRemainFlushRealtimeIndex);
    json.Jsonize("enable_async_clean_resource", enableAsyncCleanResource, 
                 enableAsyncCleanResource);
    json.Jsonize("is_inc_consistent_with_realtime", isIncConsistentWithRealtime, 
                 isIncConsistentWithRealtime);
    json.Jsonize("enable_load_speed_control_for_open", enableLoadSpeedControlForOpen, 
                 enableLoadSpeedControlForOpen);
    json.Jsonize("enable_compress_operation_block", enableCompressOperationBlock,
                 enableCompressOperationBlock);
    json.Jsonize("enable_search_cache", enableSearchCache, enableSearchCache);
    json.Jsonize("enable_async_flush_index", enableAsyncFlushIndex, enableAsyncFlushIndex);    
    json.Jsonize("max_reopen_memory_use", maxReopenMemoryUse, maxReopenMemoryUse);
    json.Jsonize("need_read_remote_index", needReadRemoteIndex, needReadRemoteIndex);
    json.Jsonize("need_deploy_index", needDeployIndex, needDeployIndex);
    json.Jsonize("build_config", buildConfig, buildConfig);
    json.Jsonize("print_metrics_interval", printMetricsInterval, printMetricsInterval);
    json.Jsonize("disable_sse_pfor_delta_optimize",
                 disableSsePforDeltaOptimize, disableSsePforDeltaOptimize);
    json.Jsonize("use_swap_mmap_file", useSwapMmapFile, useSwapMmapFile);
    json.Jsonize("warm_up_swap_file_when_dump", warmUpSwapFileWhenDump, warmUpSwapFileWhenDump);
    json.Jsonize("prohibit_in_memory_dump", prohibitInMemDump, prohibitInMemDump);
    json.Jsonize("max_swap_mmap_file_size", maxSwapMmapFileSize, maxSwapMmapFileSize);
    json.Jsonize("enable_async_dump_segment", enableAsyncDumpSegment, enableAsyncDumpSegment);
    json.Jsonize("kv_online_config", kvOnlineConfig, kvOnlineConfig);
    json.Jsonize("enable_force_open", enableForceOpen, enableForceOpen);
    json.Jsonize("disable_load_customized_index", disableLoadCustomizedIndex, disableLoadCustomizedIndex);
    json.Jsonize("load_patch_thread_num", loadPatchThreadNum, loadPatchThreadNum);
    if (json.GetMode() == TO_JSON)
    {
        loadConfigList.Jsonize(json);
    }
    else
    {
        map<string, Any> jsonMap = json.GetMap();
        if (jsonMap.find("load_config") != jsonMap.end())
        {
            loadConfigList.Jsonize(json);
        }
        if (!needDeployIndex)
        {
            loadConfigList.SetLoadMode(LoadConfig::LoadMode::REMOTE_ONLY);
            if (!needReadRemoteIndex)
            {
                IE_LOG(WARN, "needDeployIndex is false, so needReadRemoteIndex must be true, rewrite it");
                needReadRemoteIndex = true;
            }
        }
        else
        {
            if (!needReadRemoteIndex)
            {
                loadConfigList.SetLoadMode(LoadConfig::LoadMode::LOCAL_ONLY);
            }
        }

        // Only for pe control, do NOT use in production.
        char *disableSeparationStr = getenv("INDEXLIB_DISABLE_SCS");
        if (disableSeparationStr)
        {
            bool disabled = false;
            if (autil::StringUtil::parseTrueFalse(disableSeparationStr, disabled))
            {
                if (disabled)
                {
                    IE_LOG(WARN, "disable s/c sepration by env, set cache load config as LOCAL_ONLY");
                    for (auto& loadConfig : loadConfigList.GetLoadConfigs())
                    {
                        if (READ_MODE_GLOBAL_CACHE == loadConfig.GetLoadStrategyName() ||
                            READ_MODE_CACHE == loadConfig.GetLoadStrategyName())
                        {
                            loadConfig.SetLoadMode(LoadConfig::LoadMode::LOCAL_ONLY);
                            IE_LOG(WARN, "set cache load config [%s] as LOCAL_ONLY", loadConfig.GetName().c_str());
                        }
                    }
                }
            }
        }

        loadConfigList.Check();
    }
}

void OnlineConfigBase::Check() const
{
    if (maxRealtimeMemSize < (int64_t)DEFAULT_CHUNK_SIZE * 2)
    {
        INDEXLIB_FATAL_ERROR(
                BadParameter,
                "max_realtime_memory_use at least twice as much as"
                " chunk_size(%luM).", DEFAULT_CHUNK_SIZE);
    }

    if (onlineKeepVersionCount == 0)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, 
                             "online_keep_version_count should be greater than 0.");
    }

    loadConfigList.Check();
    buildConfig.Check();

    if (loadRemainFlushRealtimeIndex && !onDiskFlushRealtimeIndex)
    {
        INDEXLIB_FATAL_ERROR(
                BadParameter,
                "load_remain_flush_realtime_index must be false when "
                "on_disk_flush_realtime_index is false");
    }

    if (maxRealtimeMemSize < buildConfig.buildTotalMemory)
    {
        INDEXLIB_FATAL_ERROR(
                BadParameter,
                "maxRealtimeMemSize[%ld] must be bigger than buildTotalMemory[%ld] in online_config!",
                maxRealtimeMemSize, buildConfig.buildTotalMemory);
    }

    if (!onDiskFlushRealtimeIndex && prohibitInMemDump)
    {
        INDEXLIB_FATAL_ERROR(
                BadParameter,
                "prohibit_in_memory_dump must be [false] when on_disk_flush_realtime_index == false");
    }
}

IE_NAMESPACE_END(config);

