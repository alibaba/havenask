#ifndef __INDEXLIB_FILE_SYSTEM_OPTIONS_H
#define __INDEXLIB_FILE_SYSTEM_OPTIONS_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/storage/raid_config.h"

DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(file_system, FileBlockCache);

IE_NAMESPACE_BEGIN(file_system);

class FileSystemOptions
{
public:
    FileSystemOptions()
        : metricPref(FSMP_ALL)
        , needFlush(true)
        , enableAsyncFlush(true)
        , useCache(true)
        , useRootLink(false)
        , rootLinkWithTs(true)
        , prohibitInMemDump(false)
        , enablePathMetaContainer(false)
        , isOffline(false)
    {}

public:
    LoadConfigList loadConfigList;
    FSMetricPreference metricPref;
    FileBlockCachePtr fileBlockCache;
    util::PartitionMemoryQuotaControllerPtr memoryQuotaController;
    storage::RaidConfigPtr raidConfig;
    bool needFlush;
    bool enableAsyncFlush;
    bool useCache;
    bool useRootLink;
    bool rootLinkWithTs;
    bool prohibitInMemDump;
    bool enablePathMetaContainer;
    bool isOffline;
};

DEFINE_SHARED_PTR(FileSystemOptions);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_SYSTEM_OPTIONS_H
