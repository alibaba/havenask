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

#include "fslib/fs/FileSystem.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"

namespace indexlib { namespace common {

class FileSystemFactory
{
public:
    static file_system::FileSystemOptions
    CreateFileSystemOptions(const std::string& rootDir, const config::IndexPartitionOptions& options,
                            const util::PartitionMemoryQuotaControllerPtr& controller,
                            const file_system::FileBlockCacheContainerPtr& fileBlockCacheContainer,
                            const std::string& fileSystemIdentifier)
    {
        file_system::FileSystemOptions fileSystemOptions;
        const config::OnlineConfig& onlineConfig = options.GetOnlineConfig();
        fileSystemOptions.loadConfigList = onlineConfig.loadConfigList;

        fileSystemOptions.needFlush =
            options.IsOffline() || (options.IsOnline() && onlineConfig.onDiskFlushRealtimeIndex);
        fileSystemOptions.useRootLink =
            (options.IsOnline() && onlineConfig.onDiskFlushRealtimeIndex && !options.TEST_mReadOnly);

        fileSystemOptions.enableAsyncFlush =
            options.IsOffline() || (options.IsOnline() && onlineConfig.enableAsyncFlushIndex);

        fileSystemOptions.metricPref = options.IsOffline() ? file_system::FSMP_OFFLINE : file_system::FSMP_ONLINE;
        fileSystemOptions.prohibitInMemDump = options.IsOnline() && options.GetOnlineConfig().prohibitInMemDump;
        if (options.IsOffline()) {
            fileSystemOptions.isOffline = true;
            file_system::FslibWrapper::SetMergeIOConfig(options.GetMergeConfig().mergeIOConfig);
        }

        fileSystemOptions.memoryQuotaController = controller;
        fileSystemOptions.fileBlockCacheContainer = fileBlockCacheContainer;
        if (options.IsOnline()) {
            fileSystemOptions.memMetricGroupPaths.push_back(RT_INDEX_PARTITION_DIR_NAME);
            fileSystemOptions.memMetricGroupPaths.push_back(JOIN_INDEX_PARTITION_DIR_NAME);
        }
        fileSystemOptions.fileSystemIdentifier = fileSystemIdentifier;

        fileSystemOptions.TEST_useSessionFileCache = options.TEST_mReadOnly;
        return fileSystemOptions;
    }
};
}} // namespace indexlib::common
