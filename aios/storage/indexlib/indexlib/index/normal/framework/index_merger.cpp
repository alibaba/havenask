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
#include "indexlib/index/normal/framework/index_merger.h"

#include <unordered_set>

#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/util/PathUtil.h"
using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::file_system;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexMerger);

IndexMerger::IndexMerger() {}

IndexMerger::~IndexMerger() {}

void IndexMerger::Init(const config::IndexConfigPtr& indexConfig, const index_base::MergeItemHint& hint,
                       const index_base::MergeTaskResourceVector& taskResources, const config::MergeIOConfig& ioConfig,
                       legacy::TruncateIndexWriterCreator* truncateCreator,
                       legacy::AdaptiveBitmapIndexWriterCreator* adaptiveCreator)
{
    mIndexConfig = indexConfig;
    mMergeHint = hint;
    mTaskResources = taskResources;
    mIOConfig = ioConfig;
}

void IndexMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir) { mSegmentDirectory = segDir; }

DirectoryPtr IndexMerger::GetMergeDir(const DirectoryPtr& indexDirectory, bool needClear) const
{
    string mergePath = mIndexConfig->GetIndexName();
    string parallelInstDirName = mMergeHint.GetParallelInstanceDirName();
    if (!parallelInstDirName.empty()) {
        mergePath = util::PathUtil::JoinPath(mergePath, parallelInstDirName);
    }
    if (needClear) {
        file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
        indexDirectory->RemoveDirectory(mergePath, removeOption);
    }
    return indexDirectory->MakeDirectory(mergePath);
}

string IndexMerger::GetMergedDir(const string& rootDir) const
{
    string mergedDir = util::PathUtil::JoinPath(rootDir, mIndexConfig->GetIndexName());
    string parallelInstDirName = mMergeHint.GetParallelInstanceDirName();
    if (parallelInstDirName.empty()) {
        return mergedDir;
    }
    return util::PathUtil::JoinPath(mergedDir, parallelInstDirName);
}
}} // namespace indexlib::index
