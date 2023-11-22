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
#pragma once

#include <memory>

#include "autil/EnvUtil.h"
#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"
#include "indexlib/index/data_structure/equal_value_compress_dumper.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/index/data_structure/var_len_data_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_creator.h"
#include "indexlib/index/normal/attribute/accessor/dedup_patch_file_merger.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info_heap.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_merger.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index/util/file_compress_param_helper.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index/util/segment_output_mapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class VarNumAttributeMerger : public AttributeMerger
{
public:
    using SegmentReader = MultiValueAttributeSegmentReader<T>;
    using SegmentReaderPtr = std::shared_ptr<SegmentReader>;
    using SegmentReaderWithCtx = MultiValueAttributeSegmentReaderWithCtx<T>;

protected:
    struct OutputData {
        size_t outputIdx = 0;
        file_system::FileWriterPtr dataInfoFile;
        VarLenDataWriterPtr dataWriter;
        OutputData() = default;
    };

public:
    VarNumAttributeMerger(bool needMergePatch = false);
    ~VarNumAttributeMerger();

public:
    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(var_num);

public:
    void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void SortByWeightMerge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

public:
    // for test
    void SetOffsetThreshold(uint64_t threshold);

protected:
    void DoMerge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                 const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    virtual void MergeData(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    virtual void MergeData(const std::string& dir, const index_base::SegmentMergeInfos& segMergeInfos,
                           const ReclaimMapPtr& reclaimMap)
    {
        assert(false);
    }

    virtual void ReserveMemBuffers(const std::vector<AttributePatchReaderPtr>& patchReaders,
                                   const std::vector<SegmentReaderWithCtx>& segReaders);

    virtual void ReleaseMemBuffers();

    std::vector<typename VarNumAttributeMerger<T>::SegmentReaderWithCtx>
    CreateSegmentReaders(const index_base::SegmentMergeInfos& segMergeInfos);
    virtual typename VarNumAttributeMerger<T>::SegmentReaderWithCtx
    CreateSegmentReader(const index_base::SegmentMergeInfo& segMergeInfo);

    virtual void PrepareOutputDatas(const ReclaimMapPtr& reclaimMap,
                                    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);
    virtual void DestroyOutputDatas();
    virtual void CloseFiles();
    virtual uint32_t ReadData(docid_t docId, const SegmentReaderWithCtx& segReader,
                              const AttributePatchReaderPtr& patchReader, uint8_t* dataBuf, uint32_t dataBufLen);
    void ClearSegReaders(std::vector<SegmentReaderWithCtx>& segReaders);

    void DumpDataInfoFile()
    {
        for (auto& output : mSegOutputMapper.GetOutputs()) {
            AttributeDataInfo dataInfo(output.dataWriter->GetDataItemCount(), output.dataWriter->GetMaxItemLength());
            std::string content = dataInfo.ToString();
            output.dataInfoFile->Write(content.c_str(), content.length()).GetOrThrow();
        }
    }

protected:
    virtual void MergePatches(const std::string& dir, const index_base::SegmentMergeInfos& segMergeInfos)
    {
        assert(false);
    }
    virtual void MergePatches(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    virtual AttributePatchReaderPtr CreatePatchReaderForSegment(segmentid_t segmentId);

    std::vector<AttributePatchReaderPtr> CreatePatchReaders(const index_base::SegmentMergeInfos& segMergeInfos);

    virtual AttributePatchReaderPtr CreatePatchReader();

    uint32_t GetMaxDataItemLen(const SegmentDirectoryBasePtr& segDir,
                               const index_base::SegmentMergeInfos& segMergeInfos) const;

    file_system::DirectoryPtr GetAttributeDirectory(const SegmentDirectoryBasePtr& segDir,
                                                    const index_base::SegmentMergeInfo& segMergeInfo) const;
    void EnsureReadCtx();

protected:
    util::MemBuffer mDataBuf;
    std::vector<SegmentReaderWithCtx> mSegReaders;
    util::SimplePool mPool;
    autil::mem_pool::UnsafePool mReadPool;
    SegmentOutputMapper<OutputData> mSegOutputMapper;
    size_t mSwitchLimit;

private:
    friend class UpdatableVarNumAttributeMergerTest;
    friend class VarNumAttributeMergerInteTest;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumAttributeMerger);
///////////////////////////////////////////////

template <typename T>
VarNumAttributeMerger<T>::VarNumAttributeMerger(bool needMergePatch) : AttributeMerger(needMergePatch)
{
    mSwitchLimit = autil::EnvUtil::getEnv("MERGE_SWITCH_MEMORY_LIMIT", 100 * 1024 * 1024);
    // default 100M
}

template <typename T>
VarNumAttributeMerger<T>::~VarNumAttributeMerger()
{
    mSegReaders.clear();
    // must release dumper before pool released: mSegOutputMapper shuold declared after mPool
}

template <typename T>
inline void VarNumAttributeMerger<T>::EnsureReadCtx()
{
    if (mReadPool.getUsedBytes() >= mSwitchLimit) {
        IE_LOG(WARN, "switch ctx, may be need expend chunk memory size (if this log often happened, set env "
                     "MERGE_SWITCH_MEMORY_LIMIT)");
        for (auto& segReader : mSegReaders) {
            segReader.ctx.reset();
        }
        mReadPool.release();
    }
    for (auto& segReader : mSegReaders) {
        if (segReader.reader) {
            if (segReader.ctx) {
                break;
            }
            segReader.ctx = segReader.reader->CreateReadContextPtr(&mReadPool);
        }
    }
}

template <typename T>
inline void VarNumAttributeMerger<T>::Merge(const MergerResource& resource,
                                            const index_base::SegmentMergeInfos& segMergeInfos,
                                            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    std::stringstream ss;
    std::for_each(outputSegMergeInfos.begin(), outputSegMergeInfos.end(),
                  [this, &ss, delim = ""](index_base::OutputSegmentMergeInfo segInfo) {
                      ss << delim << GetAttributePath(segInfo.path);
                  });
    IE_LOG(DEBUG, "Start merging to dirs : %s", ss.str().c_str());
    DoMerge(resource, segMergeInfos, outputSegMergeInfos);
    IE_LOG(DEBUG, "Finish merging to dir : %s", ss.str().c_str());
}

template <typename T>
inline void VarNumAttributeMerger<T>::SortByWeightMerge(const MergerResource& resource,
                                                        const index_base::SegmentMergeInfos& segMergeInfos,
                                                        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    std::stringstream ss;
    std::for_each(outputSegMergeInfos.begin(), outputSegMergeInfos.end(),
                  [this, &ss, delim = ""](index_base::OutputSegmentMergeInfo segInfo) {
                      ss << delim << GetAttributePath(segInfo.path);
                  });
    IE_LOG(DEBUG, "Start sort by weight merging to dir : %s", ss.str().c_str());
    DoMerge(resource, segMergeInfos, outputSegMergeInfos);
    IE_LOG(DEBUG, "Finish sort by weight merging to dir : %s", ss.str().c_str());
}

template <typename T>
inline void VarNumAttributeMerger<T>::DoMerge(const MergerResource& resource,
                                              const index_base::SegmentMergeInfos& segMergeInfos,
                                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    PrepareOutputDatas(resource.reclaimMap, outputSegMergeInfos);

    mSegReaders = CreateSegmentReaders(segMergeInfos);
    MergeData(resource, segMergeInfos, outputSegMergeInfos);

    // Attention mSegReaders clear can throw exception, trigger double delete
    ClearSegReaders(mSegReaders);
    CloseFiles();
    DestroyOutputDatas();
    ReleaseMemBuffers();
    MergePatches(resource, segMergeInfos, outputSegMergeInfos);
}

template <typename T>
inline void VarNumAttributeMerger<T>::ReserveMemBuffers(const std::vector<AttributePatchReaderPtr>& patchReaders,
                                                        const std::vector<SegmentReaderWithCtx>& segReaders)
{
    uint32_t maxItemLen = 0;
    for (size_t i = 0; i < patchReaders.size(); i++) {
        maxItemLen = std::max(patchReaders[i]->GetMaxPatchItemLen(), maxItemLen);
    }

    for (size_t i = 0; i < segReaders.size(); i++) {
        auto segReader = segReaders[i].reader;
        if (segReader) {
            maxItemLen = std::max(segReader->GetMaxDataItemLen(), maxItemLen);
        }
    }

    mDataBuf.Reserve(maxItemLen);
}

template <typename T>
inline void VarNumAttributeMerger<T>::ReleaseMemBuffers()
{
    mDataBuf.Release();
}

template <typename T>
inline void VarNumAttributeMerger<T>::ClearSegReaders(std::vector<SegmentReaderWithCtx>& segReaders)
{
    while (!segReaders.empty()) {
        auto segReader = segReaders.back();
        segReaders.pop_back();
    }
}

template <typename T>
inline void VarNumAttributeMerger<T>::MergeData(const MergerResource& resource,
                                                const index_base::SegmentMergeInfos& segMergeInfos,
                                                const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    std::vector<AttributePatchReaderPtr> patchReaders = CreatePatchReaders(segMergeInfos);
    DocumentMergeInfoHeap heap;
    auto reclaimMap = resource.reclaimMap;
    heap.Init(segMergeInfos, reclaimMap);

    ReserveMemBuffers(patchReaders, mSegReaders);
    DocumentMergeInfo info;

    while (heap.GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        docid_t currentLocalId = info.oldDocId;
        auto output = mSegOutputMapper.GetOutput(info.newDocId);
        if (!output) {
            continue;
        }
        uint32_t dataLen = ReadData(currentLocalId, mSegReaders[segIdx], patchReaders[segIdx],
                                    (uint8_t*)mDataBuf.GetBuffer(), mDataBuf.GetBufferSize());
        autil::StringView dataValue((const char*)mDataBuf.GetBuffer(), dataLen);
        output->dataWriter->AppendValue(dataValue);
    }
}

template <typename T>
uint32_t VarNumAttributeMerger<T>::ReadData(docid_t docId, const SegmentReaderWithCtx& segReader,
                                            const AttributePatchReaderPtr& patchReader, uint8_t* dataBuf,
                                            uint32_t dataBufLen)
{
    uint32_t dataLen = 0;
    EnsureReadCtx();
    if (!segReader.reader->ReadDataAndLen(docId, segReader.ctx, dataBuf, dataBufLen, dataLen)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "read attribute data for doc [%d] failed.", docId);
    }
    return dataLen;
}

template <typename T>
inline std::vector<typename VarNumAttributeMerger<T>::SegmentReaderWithCtx>
VarNumAttributeMerger<T>::CreateSegmentReaders(const index_base::SegmentMergeInfos& segMergeInfos)
{
    std::vector<SegmentReaderWithCtx> segReaderPairs;
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        if (segMergeInfos[i].segmentInfo.docCount == 0) {
            segReaderPairs.push_back({SegmentReaderPtr(), nullptr});
            continue;
        }
        segReaderPairs.push_back(CreateSegmentReader(segMergeInfos[i]));
    }
    return segReaderPairs;
}

template <typename T>
inline file_system::DirectoryPtr
VarNumAttributeMerger<T>::GetAttributeDirectory(const SegmentDirectoryBasePtr& segDir,
                                                const index_base::SegmentMergeInfo& segMergeInfo) const
{
    index_base::PartitionDataPtr partData = segDir->GetPartitionData();
    assert(partData);

    index_base::SegmentData segData = partData->GetSegmentData(segMergeInfo.segmentId);

    const std::string& attrName = mAttributeConfig->GetAttrName();
    file_system::DirectoryPtr directory;
    config::AttributeConfig::ConfigType configType = mAttributeConfig->GetConfigType();
    if (configType == config::AttributeConfig::ct_section) {
        std::string indexName = config::SectionAttributeConfig::SectionAttributeNameToIndexName(attrName);
        directory = segData.GetSectionAttributeDirectory(indexName, true);
    } else {
        directory = segData.GetAttributeDirectory(attrName, true);
    }
    return directory;
}

template <typename T>
inline typename VarNumAttributeMerger<T>::SegmentReaderWithCtx
VarNumAttributeMerger<T>::CreateSegmentReader(const index_base::SegmentMergeInfo& segMergeInfo)
{
    file_system::DirectoryPtr directory = GetAttributeDirectory(mSegmentDirectory, segMergeInfo);
    assert(directory);

    SegmentReaderPtr reader(new SegmentReader(mAttributeConfig));
    index_base::PartitionDataPtr partData = mSegmentDirectory->GetPartitionData();
    index_base::SegmentData segmentData = partData->GetSegmentData(segMergeInfo.segmentId);
    reader->Open(segmentData, PatchApplyOption::OnRead(CreatePatchReaderForSegment(segMergeInfo.segmentId)), directory,
                 nullptr, true);
    assert(reader->GetBaseAddress() == nullptr);
    return {reader, reader->CreateReadContextPtr(&mReadPool)};
}

template <typename T>
inline void VarNumAttributeMerger<T>::PrepareOutputDatas(const ReclaimMapPtr& reclaimMap,
                                                         const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    auto createFunc = [this](const index_base::OutputSegmentMergeInfo& outputInfo, size_t outputIdx) {
        OutputData output;
        output.outputIdx = outputIdx;

        const file_system::DirectoryPtr& attributeDir = outputInfo.directory;
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        attributeDir->RemoveDirectory(mAttributeConfig->GetAttrName(), removeOption);
        file_system::DirectoryPtr directory = attributeDir->MakeDirectory(mAttributeConfig->GetAttrName());
        output.dataInfoFile = directory->CreateFileWriter(ATTRIBUTE_DATA_INFO_FILE_NAME);
        output.dataWriter.reset(new VarLenDataWriter(&mPool));
        auto param = VarLenDataParamHelper::MakeParamForAttribute(mAttributeConfig);
        FileCompressParamHelper::SyncParam(mAttributeConfig->GetFileCompressConfig(), outputInfo.temperatureLayer,
                                           param);
        output.dataWriter->Init(directory, ATTRIBUTE_OFFSET_FILE_NAME, ATTRIBUTE_DATA_FILE_NAME,
                                FSWriterParamDeciderPtr(), param);
        return output;
    };
    mSegOutputMapper.Init(reclaimMap, outputSegMergeInfos, createFunc);
}

template <typename T>
inline void VarNumAttributeMerger<T>::DestroyOutputDatas()
{
    mSegOutputMapper.Clear();
    mDataBuf.Release();
}

template <typename T>
inline void VarNumAttributeMerger<T>::CloseFiles()
{
    DumpDataInfoFile();
    for (auto& output : mSegOutputMapper.GetOutputs()) {
        output.dataWriter->Close();
        output.dataInfoFile->Close().GetOrThrow();
    }
}

template <typename T>
std::vector<AttributePatchReaderPtr>
VarNumAttributeMerger<T>::CreatePatchReaders(const index_base::SegmentMergeInfos& segMergeInfos)
{
    std::vector<AttributePatchReaderPtr> patchReaders;
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        AttributePatchReaderPtr patchReader;
        patchReader = CreatePatchReaderForSegment(segMergeInfos[i].segmentId);
        patchReaders.push_back(patchReader);
    }
    return patchReaders;
}

template <typename T>
AttributePatchReaderPtr VarNumAttributeMerger<T>::CreatePatchReaderForSegment(segmentid_t segmentId)
{
    index_base::PatchFileFinderPtr patchFinder =
        index_base::PatchFileFinderCreator::Create(mSegmentDirectory->GetPartitionData().get());

    index_base::PatchFileInfoVec patchVec;
    patchFinder->FindAttrPatchFilesForTargetSegment(mAttributeConfig, segmentId, &patchVec);

    AttributePatchReaderPtr patchReader(CreatePatchReader());
    for (size_t i = 0; i < patchVec.size(); i++) {
        patchReader->AddPatchFile(patchVec[i].patchDirectory, patchVec[i].patchFileName, patchVec[i].srcSegment);
    }

    return patchReader;
}

template <typename T>
AttributePatchReaderPtr VarNumAttributeMerger<T>::CreatePatchReader()
{
    return AttributePatchReaderPtr(new VarNumAttributePatchReader<T>(mAttributeConfig));
}

template <typename T>
void VarNumAttributeMerger<T>::MergePatches(const MergerResource& resource,
                                            const index_base::SegmentMergeInfos& segMergeInfos,
                                            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    DoMergePatches<DedupPatchFileMerger>(resource, segMergeInfos, outputSegMergeInfos, mAttributeConfig);
}

template <typename T>
inline uint32_t VarNumAttributeMerger<T>::GetMaxDataItemLen(const SegmentDirectoryBasePtr& segDir,
                                                            const index_base::SegmentMergeInfos& segMergeInfos) const
{
    uint32_t maxItemLen = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        uint32_t docCount = segMergeInfos[i].segmentInfo.docCount;
        if (docCount == 0) {
            continue;
        }

        file_system::DirectoryPtr attrDir = GetAttributeDirectory(segDir, segMergeInfos[i]);
        assert(attrDir);
        std::string dataInfoStr;
        attrDir->Load(ATTRIBUTE_DATA_INFO_FILE_NAME, dataInfoStr, true);
        AttributeDataInfo dataInfo;
        dataInfo.FromString(dataInfoStr);
        maxItemLen = std::max(maxItemLen, dataInfo.maxItemLen);
    }
    return maxItemLen;
}

template <typename T>
int64_t VarNumAttributeMerger<T>::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
                                                    const MergerResource& resource,
                                                    const index_base::SegmentMergeInfos& segMergeInfos,
                                                    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                                    bool isSortedMerge) const
{
    int64_t size = sizeof(*this);
    size += sizeof(VarLenDataWriter) * outputSegMergeInfos.size();
    size += GetMaxDataItemLen(segDir, segMergeInfos); // dataBufferSize

    uint32_t totalDocCount = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        uint32_t docCount = segMergeInfos[i].segmentInfo.docCount;
        if (docCount == 0) {
            continue;
        }
        auto dir = GetAttributeDirectory(segDir, segMergeInfos[i]);
        size += dir->EstimateFileMemoryUse(ATTRIBUTE_DATA_FILE_NAME, file_system::FSOT_LOAD_CONFIG);
        size += dir->EstimateFileMemoryUse(ATTRIBUTE_OFFSET_FILE_NAME, file_system::FSOT_LOAD_CONFIG);
        totalDocCount += docCount;
    }
    size += totalDocCount * sizeof(int64_t); // offset vector, worst estimate
    // multiple output segment will share totalDocCount
    return size;
}

typedef VarNumAttributeMerger<int8_t> Int8MultiValueAttributeMerger;
DEFINE_SHARED_PTR(Int8MultiValueAttributeMerger);

typedef VarNumAttributeMerger<uint8_t> UInt8MultiValueAttributeMerger;
DEFINE_SHARED_PTR(UInt8MultiValueAttributeMerger);

typedef VarNumAttributeMerger<int16_t> Int16MultiValueAttributeMerger;
DEFINE_SHARED_PTR(Int16MultiValueAttributeMerger);

typedef VarNumAttributeMerger<uint16_t> UInt16MultiValueAttributeMerger;
DEFINE_SHARED_PTR(UInt16MultiValueAttributeMerger);

typedef VarNumAttributeMerger<int32_t> Int32MultiValueAttributeMerger;
DEFINE_SHARED_PTR(Int32MultiValueAttributeMerger);

typedef VarNumAttributeMerger<uint32_t> UInt32MultiValueAttributeMerger;
DEFINE_SHARED_PTR(UInt32MultiValueAttributeMerger);

typedef VarNumAttributeMerger<int64_t> Int64MultiValueAttributeMerger;
DEFINE_SHARED_PTR(Int64MultiValueAttributeMerger);

typedef VarNumAttributeMerger<uint64_t> UInt64MultiValueAttributeMerger;
DEFINE_SHARED_PTR(UInt64MultiValueAttributeMerger);

typedef VarNumAttributeMerger<float> FloatMultiValueAttributeMerger;
DEFINE_SHARED_PTR(FloatMultiValueAttributeMerger);

typedef VarNumAttributeMerger<double> DoubleMultiValueAttributeMerger;
DEFINE_SHARED_PTR(DoubleMultiValueAttributeMerger);

typedef VarNumAttributeMerger<autil::MultiChar> MultiStringAttributeMerger;
DEFINE_SHARED_PTR(MultiStringAttributeMerger);
}} // namespace indexlib::index
