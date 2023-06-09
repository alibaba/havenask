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
#ifndef __INDEXLIB_GROUP_FIELD_DATA_MERGER_H
#define __INDEXLIB_GROUP_FIELD_DATA_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/data_structure/var_len_data_merger.h"
#include "indexlib/index/normal/framework/merger_interface.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index/util/segment_output_mapper.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib { namespace index {

class GroupFieldDataMerger : public MergerInterface
{
public:
    GroupFieldDataMerger(const std::shared_ptr<config::FileCompressConfig>& config, const std::string& offsetFileName,
                         const std::string& dataFildName);
    virtual ~GroupFieldDataMerger() {}

public:
    static const int DATA_BUFFER_SIZE = 1024 * 1024 * 2;

    // virtual for test
public:
    virtual void BeginMerge(const SegmentDirectoryBasePtr& segDir) override;

    virtual void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                       const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    virtual void SortByWeightMerge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                                   const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    virtual int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                                      const index_base::SegmentMergeInfos& segMergeInfos,
                                      const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                      bool isSortedMerge) const override;

    virtual bool EnableMultiOutputSegmentParallel() const override { return true; }

    virtual std::vector<index_base::ParallelMergeItem>
    CreateParallelMergeItems(const SegmentDirectoryBasePtr& segDir,
                             const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
                             bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const override;

private:
    VarLenDataMergerPtr PrepareVarLenDataMerger(const MergerResource& resource,
                                                const index_base::SegmentMergeInfos& segMergeInfos,
                                                const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    VarLenDataReaderPtr CreateSegmentReader(const VarLenDataParam& param,
                                            const index_base::SegmentMergeInfo& segMergeInfo);

    VarLenDataWriterPtr CreateSegmentWriter(const VarLenDataParam& param,
                                            const index_base::OutputSegmentMergeInfo& outputSegMergeInfo);

private:
    virtual VarLenDataParam CreateVarLenDataParam() const = 0;

    virtual file_system::DirectoryPtr CreateInputDirectory(const index_base::SegmentData& segmentData) const = 0;

    // create a cleaned directory for merger
    virtual file_system::DirectoryPtr
    CreateOutputDirectory(const index_base::OutputSegmentMergeInfo& outputSegMergeInfo) const = 0;

protected:
    std::shared_ptr<config::FileCompressConfig> mFileCompressConfig;
    SegmentDirectoryBasePtr mSegmentDirectory;
    index_base::MergeItemHint mMergeHint;
    index_base::MergeTaskResourceVector mTaskResources;
    util::SimplePool mPool;
    std::string mOffsetFileName;
    std::string mDataFileName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(GroupFieldDataMerger);
}} // namespace indexlib::index

#endif //__INDEXLIB_GROUP_FIELD_DATA_MERGER_H
