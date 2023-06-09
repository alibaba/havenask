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
#include "indexlib/index/normal/source/source_group_merger.h"

#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/index/normal/source/source_define.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SourceGroupMerger);

string SourceGroupMerger::MERGE_TASK_NAME_PREFIX = "source_group_";

SourceGroupMerger::SourceGroupMerger(const config::SourceGroupConfigPtr& groupConfig)
    : GroupFieldDataMerger(groupConfig->GetParameter().GetFileCompressConfig(), SOURCE_OFFSET_FILE_NAME,
                           SOURCE_DATA_FILE_NAME)
    , mGroupConfig(groupConfig)
{
}

SourceGroupMerger::~SourceGroupMerger() {}

void SourceGroupMerger::EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                         int32_t totalParallelCount,
                                         const std::vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(UnSupported, "source not support parallel task merge!");
}

VarLenDataParam SourceGroupMerger::CreateVarLenDataParam() const
{
    return VarLenDataParamHelper::MakeParamForSourceData(mGroupConfig);
}

file_system::DirectoryPtr SourceGroupMerger::CreateInputDirectory(const index_base::SegmentData& segmentData) const
{
    file_system::DirectoryPtr sourceDir = segmentData.GetSourceDirectory(true);
    return sourceDir->GetDirectory(SourceDefine::GetDataDir(mGroupConfig->GetGroupId()), true);
}

file_system::DirectoryPtr
SourceGroupMerger::CreateOutputDirectory(const index_base::OutputSegmentMergeInfo& outputSegMergeInfo) const
{
    // remove old directory, not only offset and data files
    file_system::DirectoryPtr parentDir = outputSegMergeInfo.directory;
    string groupName = SourceDefine::GetDataDir(mGroupConfig->GetGroupId());
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    parentDir->RemoveDirectory(groupName, removeOption);
    return parentDir->MakeDirectory(groupName);
}
}} // namespace indexlib::index
