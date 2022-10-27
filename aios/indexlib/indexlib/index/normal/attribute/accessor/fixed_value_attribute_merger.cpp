#include "indexlib/index/normal/attribute/accessor/fixed_value_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"
#include "indexlib/index/normal/attribute/accessor/dedup_patch_file_merger.h"
#include "indexlib/util/class_typed_factory.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/file_system/directory_creator.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, FixedValueAttributeMerger);

FixedValueAttributeMerger::FixedValueAttributeMerger(
        bool needMergePatch)
    : AttributeMerger(needMergePatch)
    , mRecordSize(0)
{
}

FixedValueAttributeMerger::~FixedValueAttributeMerger() 
{
    DestroyBuffers();
}

void FixedValueAttributeMerger::PrepareOutputDatas(
    const ReclaimMapPtr& reclaimMap, const OutputSegmentMergeInfos& outputSegMergeInfos)
{
    auto createFunc
        = [this](const index_base::OutputSegmentMergeInfo& outputInfo, size_t outputIdx) {
              OutputData output;
              output.outputIdx = outputIdx;
              output.targetSegmentIndex = outputInfo.targetSegmentIndex;
              output.bufferSize = DEFAULT_RECORD_COUNT * mRecordSize;
              output.dataFileWriter = CreateDataFileWriter(outputInfo.directory);
              output.dataBuffer = new uint8_t[output.bufferSize];
              IE_LOG(INFO, "create output data for dir [%s]",
                     GetAttributePath(outputInfo.directory->GetPath()).c_str());
              return output;
          };

    mSegOutputMapper.Init(reclaimMap, outputSegMergeInfos, createFunc);
    if (AttributeCompressInfo::NeedCompressData(mAttributeConfig))
    {
        PrepareCompressOutputData(outputSegMergeInfos);
    }
}

void FixedValueAttributeMerger::DestroyBuffers()
{
    for (auto& outputData : mSegOutputMapper.GetOutputs())
    {
        if (outputData.dataBuffer)
        {
            delete[] outputData.dataBuffer;
            outputData.dataBuffer = nullptr;
            outputData.bufferSize = 0;
        }
    }
    mSegOutputMapper.Clear();
}

void FixedValueAttributeMerger::CloseFiles()
{
    for (auto& outputData : mSegOutputMapper.GetOutputs())
    {
        if (outputData.dataBuffer)
        {
            outputData.dataFileWriter->Close();
            outputData.dataFileWriter.reset();
        }
    }    
}

void FixedValueAttributeMerger::Merge(const MergerResource& resource,
    const SegmentMergeInfos& segMergeInfos, const OutputSegmentMergeInfos& outputSegMergeInfos)

{
    IE_LOG(INFO, "Start merging attr : %s", mAttributeConfig->GetAttrName().c_str());
    MergeData(resource, segMergeInfos, outputSegMergeInfos);
    MergePatches(resource, segMergeInfos, outputSegMergeInfos);
    IE_LOG(INFO, "Finish merging attr : %s", mAttributeConfig->GetAttrName().c_str());
}

void FixedValueAttributeMerger::MergeData(
    const MergerResource& resource,    
    const SegmentMergeInfos& segMergeInfos,
    const OutputSegmentMergeInfos& outputSegMergeInfos)
{
    IE_LOG(INFO, "Start merging data for attribute : %s", 
           mAttributeConfig->GetAttrName().c_str());
    PrepareOutputDatas(resource.reclaimMap, outputSegMergeInfos);

    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
        if (segMergeInfo.segmentInfo.docCount == 0)
        {
            continue;
        }
        MergeSegment(resource, segMergeInfo, outputSegMergeInfos, mAttributeConfig);
    }

    if (AttributeCompressInfo::NeedCompressData(mAttributeConfig))
    {
        DumpCompressDataBuffer();
    }

    CloseFiles();
    DestroyBuffers();
    IE_LOG(INFO, "Finish merging data for attribute : %s", 
           mAttributeConfig->GetAttrName().c_str());
}

void FixedValueAttributeMerger::MergePatches(const MergerResource& resource,
    const SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    DoMergePatches<DedupPatchFileMerger>(resource, segMergeInfos, outputSegMergeInfos, mAttributeConfig);
}

FileWriterPtr FixedValueAttributeMerger::CreateDataFileWriter(const DirectoryPtr& attributeDir)
{
    attributeDir->RemoveDirectory(mAttributeConfig->GetAttrName(), true);
    DirectoryPtr directory = attributeDir->MakeDirectory(mAttributeConfig->GetAttrName());
    return directory->CreateFileWriter(ATTRIBUTE_DATA_FILE_NAME);
}

void FixedValueAttributeMerger::FlushDataBuffer(OutputData& outputData)
{
    if (outputData.bufferCursor == 0)
    {
        return;
    }

    if (AttributeCompressInfo::NeedCompressData(mAttributeConfig))
    {
        FlushCompressDataBuffer(outputData);
    }
    else
    {
        outputData.dataFileWriter->Write(outputData.dataBuffer, outputData.bufferCursor);
        outputData.bufferCursor = 0;
    }
}

IE_NAMESPACE_END(index);

