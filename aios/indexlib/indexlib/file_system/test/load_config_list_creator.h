#ifndef __INDEXLIB_LOAD_CONFIG_LIST_CREATOR_H
#define __INDEXLIB_LOAD_CONFIG_LIST_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/index_define.h"

IE_NAMESPACE_BEGIN(file_system);

class LoadConfigListCreator
{
public:
    LoadConfigListCreator();
    ~LoadConfigListCreator();
public:
    static LoadConfigList CreateLoadConfigList(
            const std::string& strategyName);
    static LoadConfigList CreateLoadConfigListFromJsonString(
            const std::string& jsonString);

    static LoadConfig MakeMmapLoadConfig(bool warmup, bool isLock, 
            bool adviseRandom = false, uint32_t lockSlice = 4 * 1024 * 1024, 
            uint32_t lockInterval = 0, bool isPartialLock = false);
    static LoadConfig MakeBlockLoadConfig(uint64_t cacheSize, 
            uint32_t blockSize, bool useGlobalCache = false,
            bool cacheDecompressFile = false);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LoadConfigListCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LOAD_CONFIG_LIST_CREATOR_H
