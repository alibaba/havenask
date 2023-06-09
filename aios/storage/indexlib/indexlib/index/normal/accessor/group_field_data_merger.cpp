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
#include "indexlib/index/normal/accessor/group_field_data_merger.h"

#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/index/common/FSWriterParamDecider.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info_heap.h"
#include "indexlib/index/util/file_compress_param_helper.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, GroupFieldDataMerger);

GroupFieldDataMerger::GroupFieldDataMerger(const std::shared_ptr<config::FileCompressConfig>& config,
                                           const std::string& offsetFileName, const std::string& dataFileName)
    : mFileCompressConfig(config)
    , mOffsetFileName(offsetFileName)
    , mDataFileName(dataFileName)
{
}

void GroupFieldDataMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir) { mSegmentDirectory = segDir; }

void GroupFieldDataMerger::Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                                 const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    VarLenDataMergerPtr merger = PrepareVarLenDataMerger(resource, segMergeInfos, outputSegMergeInfos);
    merger->Merge();
}

void GroupFieldDataMerger::SortByWeightMerge(const MergerResource& resource,
                                             const index_base::SegmentMergeInfos& segMergeInfos,
                                             const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    VarLenDataMergerPtr merger = PrepareVarLenDataMerger(resource, segMergeInfos, outputSegMergeInfos);
    merger->Merge();
}

VarLenDataMergerPtr
GroupFieldDataMerger::PrepareVarLenDataMerger(const MergerResource& resource,
                                              const index_base::SegmentMergeInfos& segMergeInfos,
                                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    VarLenDataParam param = CreateVarLenDataParam();
    vector<VarLenDataMerger::InputData> inputDatas;
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        if (segMergeInfos[i].segmentInfo.docCount == 0) {
            continue;
        }

        VarLenDataMerger::InputData input;
        input.dataReader = CreateSegmentReader(param, segMergeInfos[i]);
        input.segMergeInfo = segMergeInfos[i];
        inputDatas.push_back(input);
    }

    vector<VarLenDataMerger::OutputData> outputDatas;
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i) {
        VarLenDataMerger::OutputData output;
        output.dataWriter = CreateSegmentWriter(param, outputSegMergeInfos[i]);
        output.targetSegmentIndex = outputSegMergeInfos[i].targetSegmentIndex;
        outputDatas.push_back(output);
    }
    VarLenDataMergerPtr merger(new VarLenDataMerger(param));
    merger->Init(resource, inputDatas, outputDatas);
    return merger;
}

VarLenDataReaderPtr GroupFieldDataMerger::CreateSegmentReader(const VarLenDataParam& param,
                                                              const index_base::SegmentMergeInfo& segMergeInfo)
{
    VarLenDataParam inputParam = param;
    if (mFileCompressConfig && inputParam.dataCompressorName.empty()) {
        // attention: here set any, in order to make var_len_data_reader support read compress file
        inputParam.dataCompressorName = "uncertain";
    }
    const index_base::PartitionDataPtr& partitionData = mSegmentDirectory->GetPartitionData();
    assert(partitionData);
    index_base::SegmentData segmentData = partitionData->GetSegmentData(segMergeInfo.segmentId);
    file_system::DirectoryPtr directory = CreateInputDirectory(segmentData);
    VarLenDataReaderPtr dataReader(new VarLenDataReader(inputParam, false));
    dataReader->Init(segmentData.GetSegmentInfo()->docCount, directory, mOffsetFileName, mDataFileName);
    return dataReader;
}

VarLenDataWriterPtr
GroupFieldDataMerger::CreateSegmentWriter(const VarLenDataParam& param,
                                          const index_base::OutputSegmentMergeInfo& outputSegMergeInfo)
{
    // directory must be clean (empty)
    file_system::DirectoryPtr directory = CreateOutputDirectory(outputSegMergeInfo);
    fslib::FileList fileList;

    VarLenDataParam outputParam = param;
    FileCompressParamHelper::SyncParam(mFileCompressConfig, outputSegMergeInfo.temperatureLayer, outputParam);

    VarLenDataWriterPtr dataWriter(new VarLenDataWriter(&mPool));
    dataWriter->Init(directory, mOffsetFileName, mDataFileName, FSWriterParamDeciderPtr(), outputParam);
    return dataWriter;
}

int64_t GroupFieldDataMerger::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                                                const index_base::SegmentMergeInfos& segMergeInfos,
                                                const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                                bool isSortedMerge) const
{
    int64_t size = 0;
    size += sizeof(*this);
    size += DATA_BUFFER_SIZE;

    VarLenDataMerger dataMerger(CreateVarLenDataParam());
    uint32_t totalDocCount = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        totalDocCount += segMergeInfos[i].segmentInfo.docCount;
    }
    size += dataMerger.EstimateMemoryUse(totalDocCount);
    return size;
}

vector<index_base::ParallelMergeItem> GroupFieldDataMerger::CreateParallelMergeItems(
    const SegmentDirectoryBasePtr& segDir, const index_base::SegmentMergeInfos& inPlanSegMergeInfos,
    uint32_t instanceCount, bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const
{
    return vector<index_base::ParallelMergeItem>();
}
}} // namespace indexlib::index
