#include <autil/StringUtil.h>
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/file_system/load_config/warmup_strategy.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/index_partition_options.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LoadConfigListCreator);

LoadConfigListCreator::LoadConfigListCreator() 
{
}

LoadConfigListCreator::~LoadConfigListCreator() 
{
}

LoadConfigList LoadConfigListCreator::CreateLoadConfigList(
        const string& strategyName)
{
    LoadConfigList loadConfigList;
    if (strategyName == READ_MODE_MMAP)
    {
        LoadConfig loadConfig = MakeMmapLoadConfig(false, false);
        loadConfigList.PushBack(loadConfig);
    }
    else if (strategyName == READ_MODE_CACHE)
    {
        LoadConfig loadConfig = MakeBlockLoadConfig(1024000, 1024, false, false);
        loadConfigList.PushBack(loadConfig);
    }
    else if (strategyName == READ_MODE_GLOBAL_CACHE)
    {
        LoadConfig loadConfig = MakeBlockLoadConfig(1024000, 1024, true, false);
        loadConfigList.PushBack(loadConfig);
    }

    return loadConfigList;
}

LoadConfig LoadConfigListCreator::MakeMmapLoadConfig(bool warmup, bool isLock, 
        bool adviseRandom, uint32_t lockSlice, uint32_t lockInterval,
        bool isPartialLock)
{
    LoadConfig loadConfig;
    MmapLoadStrategyPtr strategy(new MmapLoadStrategy(
                    isLock, isPartialLock, adviseRandom, lockSlice, lockInterval));
    loadConfig.SetLoadStrategyPtr(strategy);

    LoadConfig::FilePatternStringVector patterns;
    patterns.push_back(".*");
    loadConfig.SetFilePatternString(patterns);

    WarmupStrategy warmupStrategy;
    if (warmup)
    {
        warmupStrategy.SetWarmupType(WarmupStrategy::WARMUP_SEQUENTIAL);
    }
    else
    {
        warmupStrategy.SetWarmupType(WarmupStrategy::WARMUP_NONE);
    }
    loadConfig.SetWarmupStrategy(warmupStrategy);
    
    return loadConfig;
}

LoadConfig LoadConfigListCreator::MakeBlockLoadConfig(uint64_t cacheSize, 
        uint32_t blockSize, bool useGlobalCache, bool cacheDecompressFile)
{
    LoadConfig loadConfig;

    LoadConfig::FilePatternStringVector patterns;
    patterns.push_back(".*");
    loadConfig.SetFilePatternString(patterns);

    CacheLoadStrategyPtr loadStrategy(
            new CacheLoadStrategy(cacheSize, blockSize, false,
                    useGlobalCache, cacheDecompressFile));
    loadConfig.SetLoadStrategyPtr(loadStrategy);

    return loadConfig;
}

LoadConfigList LoadConfigListCreator::CreateLoadConfigListFromJsonString(
        const std::string& jsonString)
{
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, jsonString);
    loadConfigList.Check();
    return loadConfigList;
}

IE_NAMESPACE_END(file_system);

