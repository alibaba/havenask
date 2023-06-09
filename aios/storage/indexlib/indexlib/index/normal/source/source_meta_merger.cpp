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
#include "indexlib/index/normal/source/source_meta_merger.h"

#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/index/normal/source/source_define.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SourceMetaMerger);

std::string SourceMetaMerger::MERGE_TASK_NAME = "source_meta";

SourceMetaMerger::SourceMetaMerger()
    : GroupFieldDataMerger(std::shared_ptr<config::FileCompressConfig>(), SOURCE_OFFSET_FILE_NAME,
                           SOURCE_DATA_FILE_NAME)
{
}

SourceMetaMerger::~SourceMetaMerger() {}

void SourceMetaMerger::EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                        int32_t totalParallelCount,
                                        const std::vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(UnSupported, "source not support parallel task merge!");
}

VarLenDataParam SourceMetaMerger::CreateVarLenDataParam() const
{
    return VarLenDataParamHelper::MakeParamForSourceMeta();
}

file_system::DirectoryPtr SourceMetaMerger::CreateInputDirectory(const index_base::SegmentData& segmentData) const
{
    file_system::DirectoryPtr sourceDir = segmentData.GetSourceDirectory(true);
    return sourceDir->GetDirectory(SOURCE_META_DIR, true);
}

file_system::DirectoryPtr
SourceMetaMerger::CreateOutputDirectory(const index_base::OutputSegmentMergeInfo& outputSegMergeInfo) const
{
    file_system::DirectoryPtr parentDir = outputSegMergeInfo.directory;
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    parentDir->RemoveDirectory(SOURCE_META_DIR, removeOption);
    return parentDir->MakeDirectory(SOURCE_META_DIR);
}
}} // namespace indexlib::index
