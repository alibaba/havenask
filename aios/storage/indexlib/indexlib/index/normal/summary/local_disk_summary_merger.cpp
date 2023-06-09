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
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"

#include "fslib/fslib.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, LocalDiskSummaryMerger);

LocalDiskSummaryMerger::LocalDiskSummaryMerger(const config::SummaryGroupConfigPtr& summaryGroupConfig,
                                               const index_base::MergeItemHint& hint,
                                               const index_base::MergeTaskResourceVector& taskResources)
    : SummaryMerger(summaryGroupConfig)
    , mHint(hint)
    , mTaskResources(taskResources)
{
    assert(mSummaryGroupConfig);
}

LocalDiskSummaryMerger::~LocalDiskSummaryMerger() {}

void LocalDiskSummaryMerger::EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                              int32_t totalParallelCount,
                                              const vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(UnSupported, "summary not support parallel task merge!");
}

VarLenDataParam LocalDiskSummaryMerger::CreateVarLenDataParam() const
{
    return VarLenDataParamHelper::MakeParamForSummary(mSummaryGroupConfig);
}

file_system::DirectoryPtr LocalDiskSummaryMerger::CreateInputDirectory(const index_base::SegmentData& segmentData) const
{
    DirectoryPtr directory = segmentData.GetSummaryDirectory(true);
    if (!mSummaryGroupConfig->IsDefaultGroup()) {
        const string& groupName = mSummaryGroupConfig->GetGroupName();
        directory = directory->GetDirectory(groupName, true);
    }
    return directory;
}

file_system::DirectoryPtr
LocalDiskSummaryMerger::CreateOutputDirectory(const index_base::OutputSegmentMergeInfo& outputSegMergeInfo) const
{
    file_system::DirectoryPtr directory = outputSegMergeInfo.directory;
    if (!mSummaryGroupConfig->IsDefaultGroup()) {
        const string& groupName = mSummaryGroupConfig->GetGroupName();
        directory = directory->MakeDirectory(groupName);
    }
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    directory->RemoveFile(SUMMARY_OFFSET_FILE_NAME, removeOption);
    directory->RemoveFile(SUMMARY_DATA_FILE_NAME, removeOption);
    directory->RemoveFile(string(SUMMARY_OFFSET_FILE_NAME) + string(COMPRESS_FILE_INFO_SUFFIX), removeOption);
    directory->RemoveFile(string(SUMMARY_OFFSET_FILE_NAME) + string(COMPRESS_FILE_META_SUFFIX), removeOption);
    directory->RemoveFile(string(SUMMARY_DATA_FILE_NAME) + string(COMPRESS_FILE_INFO_SUFFIX), removeOption);
    directory->RemoveFile(string(SUMMARY_DATA_FILE_NAME) + string(COMPRESS_FILE_META_SUFFIX), removeOption);
    return directory;
}
}} // namespace indexlib::index
