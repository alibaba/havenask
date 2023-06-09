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
#include "indexlib/index/normal/attribute/accessor/fixed_value_attribute_merger.h"

#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"
#include "indexlib/index/normal/attribute/accessor/dedup_patch_file_merger.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_merger.h"
#include "indexlib/index/util/file_compress_param_helper.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, FixedValueAttributeMerger);

FixedValueAttributeMerger::FixedValueAttributeMerger(bool needMergePatch)
    : AttributeMerger(needMergePatch)
    , mRecordSize(0)
{
}

FixedValueAttributeMerger::~FixedValueAttributeMerger() { DestroyBuffers(); }

void FixedValueAttributeMerger::DestroyBuffers()
{
    for (auto& outputData : mSegOutputMapper.GetOutputs()) {
        delete outputData.formatter;
    }
    mSegOutputMapper.Clear();
}

void FixedValueAttributeMerger::CloseFiles()
{
    for (auto& outputData : mSegOutputMapper.GetOutputs()) {
        if (outputData.dataAppender) {
            outputData.dataAppender->CloseFile();
        }
    }
}

void FixedValueAttributeMerger::Merge(const MergerResource& resource, const SegmentMergeInfos& segMergeInfos,
                                      const OutputSegmentMergeInfos& outputSegMergeInfos)

{
    IE_LOG(INFO, "Start merging attr : %s", mAttributeConfig->GetAttrName().c_str());
    MergeData(resource, segMergeInfos, outputSegMergeInfos);
    MergePatches(resource, segMergeInfos, outputSegMergeInfos);
    IE_LOG(INFO, "Finish merging attr : %s", mAttributeConfig->GetAttrName().c_str());
}

void FixedValueAttributeMerger::MergeData(const MergerResource& resource, const SegmentMergeInfos& segMergeInfos,
                                          const OutputSegmentMergeInfos& outputSegMergeInfos)
{
    IE_LOG(INFO, "Start merging data for attribute : %s", mAttributeConfig->GetAttrName().c_str());
    PrepareOutputDatas(resource.reclaimMap, outputSegMergeInfos);

    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
        if (segMergeInfo.segmentInfo.docCount == 0) {
            continue;
        }
        MergeSegment(resource, segMergeInfo, outputSegMergeInfos, mAttributeConfig);
    }

    for (auto& output : mSegOutputMapper.GetOutputs()) {
        FlushDataBuffer(output);
    }
    if (AttributeCompressInfo::NeedCompressData(mAttributeConfig)) {
        DumpCompressDataBuffer();
    }

    CloseFiles();
    DestroyBuffers();
    IE_LOG(INFO, "Finish merging data for attribute : %s", mAttributeConfig->GetAttrName().c_str());
}

void FixedValueAttributeMerger::MergePatches(const MergerResource& resource, const SegmentMergeInfos& segMergeInfos,
                                             const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    DoMergePatches<DedupPatchFileMerger>(resource, segMergeInfos, outputSegMergeInfos, mAttributeConfig);
}

FileWriterPtr FixedValueAttributeMerger::CreateDataFileWriter(const DirectoryPtr& attributeDir,
                                                              const string& temperatureLayer)
{
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    attributeDir->RemoveDirectory(mAttributeConfig->GetAttrName(), removeOption);
    DirectoryPtr directory = attributeDir->MakeDirectory(mAttributeConfig->GetAttrName());

    file_system::WriterOption option;
    FileCompressParamHelper::SyncParam(mAttributeConfig->GetFileCompressConfig(), temperatureLayer, option);
    return directory->CreateFileWriter(ATTRIBUTE_DATA_FILE_NAME, option);
}

void FixedValueAttributeMerger::FlushDataBuffer(OutputData& outputData)
{
    if (!outputData.dataAppender || outputData.dataAppender->GetInBufferCount() == 0) {
        return;
    }

    if (AttributeCompressInfo::NeedCompressData(mAttributeConfig)) {
        FlushCompressDataBuffer(outputData);
        return;
    }
    outputData.dataAppender->Flush();
}
}} // namespace indexlib::index
