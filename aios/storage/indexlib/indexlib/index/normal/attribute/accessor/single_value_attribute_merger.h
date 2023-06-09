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
#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_MERGER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_MERGER_H

#include <memory>

#include "fslib/fslib.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_creator.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info_heap.h"
#include "indexlib/index/normal/attribute/accessor/fixed_value_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueAttributeMerger : public FixedValueAttributeMerger
{
public:
    typedef index::EqualValueCompressDumper<T> EqualValueCompressDumper;
    DEFINE_SHARED_PTR(EqualValueCompressDumper);

    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(single);
    class Creator : public AttributeMergerCreator
    {
    public:
        FieldType GetAttributeType() const { return common::TypeInfo<T>::GetFieldType(); }
        AttributeMerger* Create(bool isUniqEncoded, bool isUpdatable, bool needMergePatch) const
        {
            return new SingleValueAttributeMerger<T>(needMergePatch);
        }
    };

public:
    SingleValueAttributeMerger(bool needMergePatch) : FixedValueAttributeMerger(needMergePatch)
    {
        mRecordSize = sizeof(T);
    }
    ~SingleValueAttributeMerger()
    {
        mCompressDumpers.clear();
        mPool.reset();
    }

public:
    void SortByWeightMerge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void Init(const config::AttributeConfigPtr& attrConfig) override { AttributeMerger::Init(attrConfig); }

    void PrepareOutputDatas(const ReclaimMapPtr& reclaimMap,
                            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        auto createFunc = [this, reclaimMap](const index_base::OutputSegmentMergeInfo& outputInfo, size_t outputIdx) {
            // TODO: temperature layer
            OutputData output;
            output.outputIdx = outputIdx;
            output.targetSegmentIndex = outputInfo.targetSegmentIndex;
            output.targetSegmentBaseDocId = reclaimMap->GetTargetBaseDocId(outputInfo.targetSegmentIndex);

            output.formatter = new SingleValueAttributeFormatter<T>();
            output.formatter->Init(mAttributeConfig->GetCompressType(),
                                   mAttributeConfig->GetFieldConfig()->IsEnableNullField());
            output.dataAppender.reset(new SingleValueDataAppender(output.formatter));
            output.dataAppender->Init(DEFAULT_RECORD_COUNT,
                                      CreateDataFileWriter(outputInfo.directory, outputInfo.temperatureLayer));

            IE_LOG(INFO, "create output data for dir [%s]",
                   outputInfo.directory->DebugString(mAttributeConfig->GetAttrName()).c_str());
            return output;
        };

        mSegOutputMapper.Init(reclaimMap, outputSegMergeInfos, createFunc);
        if (AttributeCompressInfo::NeedCompressData(mAttributeConfig)) {
            PrepareCompressOutputData(outputSegMergeInfos);
        }
    }

    void PrepareCompressOutputData(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        assert(mCompressDumpers.empty());
        assert(!mPool.get());
        mPool.reset(new util::SimplePool);
        while (mCompressDumpers.size() < outputSegMergeInfos.size()) {
            mCompressDumpers.emplace_back(new EqualValueCompressDumper(false, mPool.get()));
        }
    }

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

protected:
    virtual index::AttributeSegmentReaderWithCtx
    OpenSingleValueAttributeReader(const config::AttributeConfigPtr& attrConfig,
                                   const index_base::SegmentMergeInfo& segMergeInfo);
    virtual void CreateSegmentReaders(const index_base::SegmentMergeInfos& segMergeInfos,
                                      std::vector<AttributeSegmentReaderWithCtx>& segReaders);

    void FlushCompressDataBuffer(OutputData& outputData) override;
    void DumpCompressDataBuffer() override;
    AttributePatchReaderPtr CreatePatchReader(const index_base::PartitionDataPtr& partData, segmentid_t segmentId,
                                              const config::AttributeConfigPtr& attrConfig);
    void MergeSegment(const MergerResource& resource, const index_base::SegmentMergeInfo& segMergeInfo,
                      const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                      const config::AttributeConfigPtr& attrConfig) override;

private:
    std::vector<EqualValueCompressDumperPtr> mCompressDumpers;
    std::shared_ptr<autil::mem_pool::PoolBase> mPool;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeMerger);

//////////////////////////////////////////////////////////////
// inline functions
template <typename T>
index::AttributeSegmentReaderWithCtx
SingleValueAttributeMerger<T>::OpenSingleValueAttributeReader(const config::AttributeConfigPtr& attrConfig,
                                                              const index_base::SegmentMergeInfo& segMergeInfo)
{
    std::shared_ptr<SingleValueAttributeSegmentReader<T>> segReader(
        new SingleValueAttributeSegmentReader<T>(attrConfig));
    index_base::PartitionDataPtr partData = this->mSegmentDirectory->GetPartitionData();
    index_base::SegmentData segmentData = partData->GetSegmentData(segMergeInfo.segmentId);
    segReader->Open(segmentData,
                    PatchApplyOption::OnRead(CreatePatchReader(partData, segMergeInfo.segmentId, attrConfig)),
                    segmentData.GetAttributeDirectory(attrConfig->GetAttrName(), true), true);
    return {segReader, segReader->CreateReadContextPtr(nullptr)};
}

template <typename T>
inline AttributePatchReaderPtr
SingleValueAttributeMerger<T>::CreatePatchReader(const index_base::PartitionDataPtr& partData, segmentid_t segmentId,
                                                 const config::AttributeConfigPtr& attrConfig)
{
    assert(partData);
    index_base::PatchFileFinderPtr patchFinder = index_base::PatchFileFinderCreator::Create(partData.get());

    index_base::PatchFileInfoVec patchVec;
    patchFinder->FindAttrPatchFilesForTargetSegment(this->mAttributeConfig, segmentId, &patchVec);

    std::shared_ptr<SingleValueAttributePatchReader<T>> patchReader(new SingleValueAttributePatchReader<T>(attrConfig));
    for (size_t i = 0; i < patchVec.size(); i++) {
        patchReader->AddPatchFile(patchVec[i].patchDirectory, patchVec[i].patchFileName, patchVec[i].srcSegment);
    }
    return patchReader;
}

template <typename T>
inline void SingleValueAttributeMerger<T>::MergeSegment(const MergerResource& resource,
                                                        const index_base::SegmentMergeInfo& segMergeInfo,
                                                        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                                        const config::AttributeConfigPtr& attrConfig)
{
    auto segReader = OpenSingleValueAttributeReader(attrConfig, segMergeInfo);
    auto reclaimMap = resource.reclaimMap;
    uint32_t docCount = segMergeInfo.segmentInfo.docCount;
    for (docid_t localId = 0; localId < (docid_t)docCount; ++localId) {
        // this is global docId
        docid_t newId = reclaimMap->GetNewId(localId + segMergeInfo.baseDocId);
        if (newId < 0) // is deleted
        {
            continue;
        }
        auto output = mSegOutputMapper.GetOutput(newId);
        if (!output) {
            continue;
        }

        T value {};
        bool isNull = false;
        segReader.reader->Read(localId, segReader.ctx, (uint8_t*)&value, sizeof(value), isNull);
        output->Set(newId, value, isNull);
        if (output->BufferFull()) {
            FlushDataBuffer(*output);
        }
    }
}

template <typename T>
inline void
SingleValueAttributeMerger<T>::SortByWeightMerge(const MergerResource& resource,
                                                 const index_base::SegmentMergeInfos& segMergeInfos,
                                                 const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)

{
    std::stringstream ss;
    std::for_each(outputSegMergeInfos.begin(), outputSegMergeInfos.end(),
                  [this, &ss, delim = ""](index_base::OutputSegmentMergeInfo segInfo) {
                      ss << delim << GetAttributePath(segInfo.path);
                  });
    IE_LOG(DEBUG, "Start sort by weight merging to dir : %s", ss.str().c_str());
    std::vector<index::AttributeSegmentReaderWithCtx> segReaders;
    CreateSegmentReaders(segMergeInfos, segReaders);

    auto reclaimMap = resource.reclaimMap;

    PrepareOutputDatas(resource.reclaimMap, outputSegMergeInfos);

    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, reclaimMap);

    DocumentMergeInfo info;
    while (heap.GetNext(info)) {
        auto output = mSegOutputMapper.GetOutput(info.newDocId);
        if (output) {
            T value {};
            int32_t segmentIndex = info.segmentIndex;
            bool isNull = false;
            segReaders[segmentIndex].reader->Read(info.oldDocId, segReaders[segmentIndex].ctx, (uint8_t*)&value,
                                                  sizeof(value), isNull);
            output->Set(info.newDocId, value, isNull);
            if (output->BufferFull()) {
                FlushDataBuffer(*output);
            }
        }
    }

    for (auto& outputData : mSegOutputMapper.GetOutputs()) {
        FlushDataBuffer(outputData);
    }

    if (AttributeCompressInfo::NeedCompressData(mAttributeConfig)) {
        DumpCompressDataBuffer();
    }

    CloseFiles();
    DestroyBuffers();

    MergePatches(resource, segMergeInfos, outputSegMergeInfos);
    IE_LOG(DEBUG, "Finish sort by weight merging to dir : %s", ss.str().c_str());
}

template <typename T>
inline void
SingleValueAttributeMerger<T>::CreateSegmentReaders(const index_base::SegmentMergeInfos& segMergeInfos,
                                                    std::vector<index::AttributeSegmentReaderWithCtx>& segReaders)
{
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        if (segMergeInfos[i].segmentInfo.docCount == 0) {
            segReaders.push_back({AttributeSegmentReaderPtr(), nullptr});
            continue;
        }
        segReaders.push_back(OpenSingleValueAttributeReader(mAttributeConfig, segMergeInfos[i]));
    }
}

template <typename T>
inline void SingleValueAttributeMerger<T>::FlushCompressDataBuffer(OutputData& outputData)
{
    assert(mCompressDumpers.size() == mSegOutputMapper.GetOutputs().size());
    auto compressDumper = mCompressDumpers[outputData.outputIdx];
    assert(compressDumper.get());
    outputData.dataAppender->FlushCompressBuffer(compressDumper.get());
}

template <typename T>
inline void SingleValueAttributeMerger<T>::DumpCompressDataBuffer()
{
    assert(mCompressDumpers.size() == mSegOutputMapper.GetOutputs().size());
    for (size_t i = 0; i < mCompressDumpers.size(); ++i) {
        auto& compressDumper = mCompressDumpers[i];
        if (compressDumper) {
            compressDumper->Dump(mSegOutputMapper.GetOutputs()[i].dataAppender->GetDataFileWriter());
            compressDumper->Reset();
        }
    }
}

template <typename T>
int64_t SingleValueAttributeMerger<T>::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
                                                         const MergerResource& resource,
                                                         const index_base::SegmentMergeInfos& segMergeInfos,
                                                         const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                                         bool isSortedMerge) const
{
    int64_t size = sizeof(*this);

    bool compressData = AttributeCompressInfo::NeedCompressData(this->mAttributeConfig);

    size += mRecordSize * DEFAULT_RECORD_COUNT * outputSegMergeInfos.size(); // write buffer
    if (compressData) {
        // TODO: check logic
        // assert(mCompressDumper.get() != NULL);
        // size += sizeof(*(mCompressDumper.get()));
        size += sizeof(EqualValueCompressDumper) * outputSegMergeInfos.size();
        size += mRecordSize * DEFAULT_RECORD_COUNT * outputSegMergeInfos.size(); // buffer for compress
    }

    int64_t segmentReaderTotalMemUse = 0;
    int64_t segmentReaderMaxMemUse = 0;

    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        uint32_t docCount = segMergeInfos[i].segmentInfo.docCount;
        if (docCount == 0) {
            continue;
        }
        // TODO(xiaohao.yxh) add test
        index_base::PartitionDataPtr partData = segDir->GetPartitionData();
        index_base::SegmentData segmentData = partData->GetSegmentData(segMergeInfos[i].segmentId);
        auto dir = segmentData.GetAttributeDirectory(this->mAttributeConfig->GetAttrName(), true);
        int64_t memUse = dir->EstimateFileMemoryUse(ATTRIBUTE_DATA_FILE_NAME, file_system::FSOT_LOAD_CONFIG);
        segmentReaderTotalMemUse += memUse;
        segmentReaderMaxMemUse = std::max(segmentReaderMaxMemUse, memUse);
    }

    if (isSortedMerge) {
        size += segmentReaderTotalMemUse;
    } else {
        size += segmentReaderMaxMemUse;
    }
    return size;
}

typedef SingleValueAttributeMerger<float> FloatAttributeMerger;
DEFINE_SHARED_PTR(FloatAttributeMerger);

typedef SingleValueAttributeMerger<double> DoubleAttributeMerger;
DEFINE_SHARED_PTR(DoubleAttributeMerger);

typedef SingleValueAttributeMerger<int64_t> Int64AttributeMerger;
DEFINE_SHARED_PTR(Int64AttributeMerger);

typedef SingleValueAttributeMerger<uint64_t> UInt64AttributeMerger;
DEFINE_SHARED_PTR(UInt64AttributeMerger);

typedef SingleValueAttributeMerger<uint64_t> Hash64AttributeMerger;
DEFINE_SHARED_PTR(Hash64AttributeMerger);

typedef SingleValueAttributeMerger<autil::uint128_t> UInt128AttributeMerger;
DEFINE_SHARED_PTR(UInt128AttributeMerger);

typedef SingleValueAttributeMerger<autil::uint128_t> Hash128AttributeMerger;
DEFINE_SHARED_PTR(Hash128AttributeMerger);

typedef SingleValueAttributeMerger<int32_t> Int32AttributeMerger;
DEFINE_SHARED_PTR(Int32AttributeMerger);

typedef SingleValueAttributeMerger<uint32_t> UInt32AttributeMerger;
DEFINE_SHARED_PTR(UInt32AttributeMerger);

typedef SingleValueAttributeMerger<int16_t> Int16AttributeMerger;
DEFINE_SHARED_PTR(Int16AttributeMerger);

typedef SingleValueAttributeMerger<uint16_t> UInt16AttributeMerger;
DEFINE_SHARED_PTR(UInt16AttributeMerger);

typedef SingleValueAttributeMerger<int8_t> Int8AttributeMerger;
DEFINE_SHARED_PTR(Int8AttributeMerger);

typedef SingleValueAttributeMerger<uint8_t> UInt8AttributeMerger;
DEFINE_SHARED_PTR(UInt8AttributeMerger);
}} // namespace indexlib::index

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_MERGER_H
