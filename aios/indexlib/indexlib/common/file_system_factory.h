#ifndef __INDEXLIB_FILE_SYSTEM_FACTORY_H
#define __INDEXLIB_FILE_SYSTEM_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/util/memory_control/partition_memory_quota_controller.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/storage/file_system_wrapper.h"

IE_NAMESPACE_BEGIN(common);

class FileSystemFactory
{
public:
    static file_system::IndexlibFileSystemPtr Create(const std::string& rootDir,
            const std::string& secondaryRootDir,
            const config::IndexPartitionOptions& options,
            misc::MetricProviderPtr metricProvider,
            const util::PartitionMemoryQuotaControllerPtr& controller,
            const file_system::FileBlockCachePtr& fileBlockCache)
    {
        file_system::FileSystemOptions fileSystemOptions = CreateFileSystemOptions(
                rootDir, options, controller, fileBlockCache);
        return file_system::IndexlibFileSystemCreator::Create(rootDir, secondaryRootDir,
                metricProvider, fileSystemOptions);
    }

public:
    //for test
    static file_system::IndexlibFileSystemPtr Create(const std::string& rootDir,
            const std::string& secondaryRootDir,
            const config::IndexPartitionOptions& options,
            misc::MetricProviderPtr metricProvider)
    {
        util::PartitionMemoryQuotaControllerPtr quotaController =
            util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
        return Create(rootDir, secondaryRootDir, options, metricProvider,
                      quotaController, file_system::FileBlockCachePtr());
    }

private:
    static file_system::FileSystemOptions CreateFileSystemOptions(
            const std::string& rootDir,
            const config::IndexPartitionOptions& options,
            const util::PartitionMemoryQuotaControllerPtr& controller,
            const file_system::FileBlockCachePtr& fileBlockCache)
    {
        file_system::FileSystemOptions fileSystemOptions;
        const config::OnlineConfig& onlineConfig = options.GetOnlineConfig();
        fileSystemOptions.loadConfigList = onlineConfig.loadConfigList;
    
        fileSystemOptions.needFlush = options.IsOffline() ||
                                      (options.IsOnline() && onlineConfig.onDiskFlushRealtimeIndex);
        fileSystemOptions.useRootLink = (options.IsOnline() && onlineConfig.onDiskFlushRealtimeIndex
                && !options.TEST_mReadOnly);

        fileSystemOptions.enableAsyncFlush = options.IsOffline() ||
                                             (options.IsOnline() && onlineConfig.enableAsyncFlushIndex);

        fileSystemOptions.metricPref = options.IsOffline() ?
                                       file_system::FSMP_OFFLINE : file_system::FSMP_ONLINE;
        fileSystemOptions.prohibitInMemDump = options.IsOnline() &&
                                              options.GetOnlineConfig().prohibitInMemDump;
        fileSystemOptions.enablePathMetaContainer = options.GetBuildConfig().usePathMetaCache;
        if (fileSystemOptions.useRootLink &&
            fslib::fs::FileSystem::getFsType(rootDir) != FSLIB_FS_LOCAL_FILESYSTEM_NAME)
        {
            INDEXLIB_THROW(misc::UnSupportedException, "dfs path [%s] not support useRootLink yet!",
                    rootDir.c_str());
        }
    
        if (options.IsOffline())
        {
            fileSystemOptions.isOffline = true;
            storage::FileSystemWrapper::SetMergeIOConfig(options.GetMergeConfig().mergeIOConfig);
            fileSystemOptions.raidConfig = options.GetOfflineConfig().GetRaidConfig();
        }

        fileSystemOptions.memoryQuotaController = controller;
        fileSystemOptions.fileBlockCache = fileBlockCache;
        return fileSystemOptions;
    }
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_FILE_SYSTEM_FACTORY_H
