#include "indexlib/file_system/test/LoadConfigListCreator.h"

#include <iosfwd>

#include "autil/CommonMacros.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/file_system/load_config/WarmupStrategy.h"


using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, LoadConfigListCreator);

LoadConfigListCreator::LoadConfigListCreator() {}

LoadConfigListCreator::~LoadConfigListCreator() {}

LoadConfigList LoadConfigListCreator::CreateLoadConfigList(const string& strategyName)
{
    size_t ioBatchSize = CacheLoadStrategy::DEFAULT_CACHE_IO_BATCH_SIZE;
    LoadConfigList loadConfigList;
    if (strategyName == READ_MODE_MMAP) {
        LoadConfig loadConfig = MakeMmapLoadConfig(false, false);
        loadConfigList.PushBack(loadConfig);
    } else if (strategyName == READ_MODE_CACHE) {
        LoadConfig loadConfig = MakeBlockLoadConfig("lru", 1024000, 1024, ioBatchSize, false, false);
        loadConfigList.PushBack(loadConfig);
    } else if (strategyName == READ_MODE_GLOBAL_CACHE) {
        LoadConfig loadConfig = MakeBlockLoadConfig("lru", 1024000, 1024, ioBatchSize, true, false);
        loadConfigList.PushBack(loadConfig);
    }

    return loadConfigList;
}

LoadConfig LoadConfigListCreator::MakeMmapLoadConfig(bool warmup, bool isLock, bool adviseRandom, uint32_t lockSlice,
                                                     uint32_t lockInterval)
{
    LoadConfig loadConfig;
    MmapLoadStrategyPtr strategy(new MmapLoadStrategy(isLock, adviseRandom, lockSlice, lockInterval));
    loadConfig.SetLoadStrategyPtr(strategy);

    LoadConfig::FilePatternStringVector patterns;
    patterns.push_back(".*");
    loadConfig.SetFilePatternString(patterns);

    WarmupStrategy warmupStrategy;
    if (warmup) {
        warmupStrategy.SetWarmupType(WarmupStrategy::WARMUP_SEQUENTIAL);
    } else {
        warmupStrategy.SetWarmupType(WarmupStrategy::WARMUP_NONE);
    }
    loadConfig.SetWarmupStrategy(warmupStrategy);

    return loadConfig;
}

LoadConfig LoadConfigListCreator::MakeBlockLoadConfig(const string& cacheType, uint64_t memorySize, uint32_t blockSize,
                                                      uint32_t ioBatchSize, bool useGlobalCache,
                                                      bool cacheDecompressFile, bool useHighPriority)
{
    LoadConfig loadConfig;

    LoadConfig::FilePatternStringVector patterns;
    patterns.push_back(".*");
    loadConfig.SetFilePatternString(patterns);

    assert(cacheType == "dadi" || cacheType == "lru");
    util::BlockCacheOption option = BlockCacheOption::LRU(memorySize, blockSize, ioBatchSize);

    CacheLoadStrategyPtr loadStrategy(
        new CacheLoadStrategy(option, false, useGlobalCache, cacheDecompressFile, useHighPriority));
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    return loadConfig;
}

LoadConfigList LoadConfigListCreator::CreateLoadConfigListFromJsonString(const std::string& jsonString)
{
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, jsonString);
    loadConfigList.Check();
    return loadConfigList;
}
}} // namespace indexlib::file_system
