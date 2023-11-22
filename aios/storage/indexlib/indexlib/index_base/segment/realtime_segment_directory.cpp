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
#include "indexlib/index_base/segment/realtime_segment_directory.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::index;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, RealtimeSegmentDirectory);

RealtimeSegmentDirectory::RealtimeSegmentDirectory(bool enableRecover) : mEnableRecover(enableRecover) {}

RealtimeSegmentDirectory::~RealtimeSegmentDirectory() {}

void RealtimeSegmentDirectory::DoInit(const file_system::DirectoryPtr& directory)
{
    if (mEnableRecover) {
        RemoveVersionFiles(directory);
        mVersion = index_base::Version(INVALID_VERSIONID);
        mVersion = OfflineRecoverStrategy::Recover(mVersion, directory);
        mVersion.SetVersionId(0);
        // realtime segment no need DumpDeployIndexMeta
        mVersion.Store(directory, false);
        directory->Sync(true);
    }
}

void RealtimeSegmentDirectory::RemoveVersionFiles(const file_system::DirectoryPtr& directory)
{
    fslib::FileList fileLists;
    VersionLoader::ListVersion(directory, fileLists);
    for (size_t i = 0; i < fileLists.size(); i++) {
        directory->RemoveFile(fileLists[i]);
    }
}

void RealtimeSegmentDirectory::CommitVersion(const config::IndexPartitionOptions& options)
{
    return DoCommitVersion(options, false);
}
}} // namespace indexlib::index_base
