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
#ifndef __INDEXLIB_SOURCE_META_MERGER_H
#define __INDEXLIB_SOURCE_META_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/accessor/group_field_data_merger.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SourceMetaMerger : public GroupFieldDataMerger
{
public:
    SourceMetaMerger();
    ~SourceMetaMerger();

public:
    void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
                          const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) override;

private:
    virtual VarLenDataParam CreateVarLenDataParam() const override;
    virtual file_system::DirectoryPtr CreateInputDirectory(const index_base::SegmentData& segmentData) const override;
    virtual file_system::DirectoryPtr
    CreateOutputDirectory(const index_base::OutputSegmentMergeInfo& outputSegMergeInfo) const override;

public:
    static std::string MERGE_TASK_NAME;
    static const int DATA_BUFFER_SIZE = 1024 * 1024 * 2;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceMetaMerger);
}} // namespace indexlib::index

#endif //__INDEXLIB_SOURCE_META_MERGER_H
