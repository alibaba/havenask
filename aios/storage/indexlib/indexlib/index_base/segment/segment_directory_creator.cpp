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
#include "indexlib/index_base/segment/segment_directory_creator.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"
#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/parallel_offline_segment_directory.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SegmentDirectoryCreator);

SegmentDirectoryCreator::SegmentDirectoryCreator() {}

SegmentDirectoryCreator::~SegmentDirectoryCreator() {}

SegmentDirectoryPtr SegmentDirectoryCreator::Create(const config::IndexPartitionOptions& options,
                                                    const file_system::DirectoryPtr& directory,
                                                    const index_base::Version& version, bool hasSub)
{
    SegmentDirectoryPtr segDir = Create(&options);
    segDir->Init(directory, version, hasSub);
    return segDir;
}

SegmentDirectoryPtr SegmentDirectoryCreator::Create(const file_system::DirectoryPtr& dir, index_base::Version version,
                                                    const config::IndexPartitionOptions* options, bool hasSub)
{
    bool isParallelBuild = false;
    if (ParallelBuildInfo::IsExist(dir)) {
        isParallelBuild = true;
    }
    SegmentDirectoryPtr segDir = Create(options, isParallelBuild);
    segDir->Init(dir, version, hasSub);
    return segDir;
}

SegmentDirectoryPtr SegmentDirectoryCreator::Create(const config::IndexPartitionOptions* options, bool isParallelBuild)
{
    SegmentDirectoryPtr segDir;
    if (!options) {
        segDir.reset(new SegmentDirectory);
    } else if (options->IsOnline()) {
        segDir.reset(new OnlineSegmentDirectory(
            options->GetOnlineConfig().enableRecoverIndex,
            std::make_shared<LifecycleConfig>(options->GetOnlineConfig().GetLifecycleConfig())));
    } else if (isParallelBuild) {
        segDir.reset(new ParallelOfflineSegmentDirectory(options->GetOfflineConfig().enableRecoverIndex));
    } else {
        segDir.reset(new OfflineSegmentDirectory(options->GetOfflineConfig().enableRecoverIndex,
                                                 options->GetOfflineConfig().recoverType));
    }
    return segDir;
}

SegmentDirectoryPtr SegmentDirectoryCreator::Create(const file_system::DirectoryVector& directoryVec, bool hasSub)
{
    MultiPartSegmentDirectoryPtr multiSegDir(new MultiPartSegmentDirectory);
    multiSegDir->Init(directoryVec, hasSub);
    return multiSegDir;
}

SegmentDirectoryPtr SegmentDirectoryCreator::Create(const file_system::DirectoryVector& directoryVec,
                                                    const std::vector<index_base::Version>& versions, bool hasSub)
{
    MultiPartSegmentDirectoryPtr multiSegDir(new MultiPartSegmentDirectory);
    multiSegDir->Init(directoryVec, versions, hasSub);
    return multiSegDir;
}
}} // namespace indexlib::index_base
