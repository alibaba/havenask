#pragma once

#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"

namespace indexlib { namespace file_system {

class LoadConfigListCreator
{
public:
    LoadConfigListCreator();
    ~LoadConfigListCreator();

public:
    static LoadConfigList CreateLoadConfigList(const std::string& strategyName);
    static LoadConfigList CreateLoadConfigListFromJsonString(const std::string& jsonString);

    static LoadConfig MakeMmapLoadConfig(bool warmup, bool isLock, bool adviseRandom = false,
                                         uint32_t lockSlice = 4 * 1024 * 1024, uint32_t lockInterval = 0);
    static LoadConfig MakeBlockLoadConfig(const std::string& cahceType, uint64_t memorySize, uint32_t blockSize,
                                          uint32_t ioBatchSize, bool useGlobalCache = false,
                                          bool cacheDecompressFile = false, bool useHighPriority = false);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LoadConfigListCreator> LoadConfigListCreatorPtr;
}} // namespace indexlib::file_system
