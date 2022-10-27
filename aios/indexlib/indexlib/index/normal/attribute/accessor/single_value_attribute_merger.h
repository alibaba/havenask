#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_MERGER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/simple_pool.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info_heap.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_creator.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/index/normal/attribute/accessor/single_attribute_segment_reader_for_merge.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_merger.h"
#include "indexlib/index/normal/attribute/equal_value_compress_dumper.h"
#include "indexlib/index/normal/attribute/accessor/fixed_value_attribute_merger.h"
#include "indexlib/util/profiling.h"
#include "indexlib/file_system/directory_creator.h"
#include <fslib/fslib.h>

IE_NAMESPACE_BEGIN(index);

template<typename T>
class SingleValueAttributeMerger : public FixedValueAttributeMerger
{
public:
    typedef index::EqualValueCompressDumper<T> EqualValueCompressDumper;
    DEFINE_SHARED_PTR(EqualValueCompressDumper);

    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(single);
    class Creator : public AttributeMergerCreator
    {
    public:
        FieldType GetAttributeType() const
        {
            return common::TypeInfo<T>::GetFieldType();
        }
        AttributeMerger* Create(bool isUniqEncoded,
                                bool isUpdatable,
                                bool needMergePatch) const
        {
            return new SingleValueAttributeMerger<T>(needMergePatch);
        }
    };

public:
    SingleValueAttributeMerger(bool needMergePatch)
        : FixedValueAttributeMerger(needMergePatch)
    {
        mRecordSize = sizeof(T);
    }
    ~SingleValueAttributeMerger()
    {
        mCompressDumpers.clear();
        mPool.reset();
    }

public:
    void SortByWeightMerge(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void Init(const config::AttributeConfigPtr& attrConfig) override
    {
        AttributeMerger::Init(attrConfig);
    }
    void PrepareCompressOutputData(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        assert(mCompressDumpers.empty());
        assert(!mPool.get());
        mPool.reset(new util::SimplePool);
        while (mCompressDumpers.size() < outputSegMergeInfos.size())
        {
            mCompressDumpers.emplace_back(
                new EqualValueCompressDumper(false, mPool.get()));
        }
    }

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge) const override;

protected:
    void FlushCompressDataBuffer(OutputData& outputData) override;
    void DumpCompressDataBuffer() override;
    void MergeSegment(
        const MergerResource& resource,        
        const index_base::SegmentMergeInfo& segMergeInfo,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,        
        const config::AttributeConfigPtr& attrConfig) override;

    void CreateSegmentReaders(const index_base::SegmentMergeInfos& segMergeInfos, 
                              std::vector<AttributeSegmentReaderPtr>& segReaders) override;
    
    virtual void OpenSingleValueAttributeReader(
            SingleAttributeSegmentReaderForMerge<T>& reader,
            const index_base::SegmentInfo& segInfo, segmentid_t segmentId);

private:
    std::vector<EqualValueCompressDumperPtr> mCompressDumpers;
    std::tr1::shared_ptr<autil::mem_pool::PoolBase> mPool;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeMerger);

//////////////////////////////////////////////////////////////
// inline functions

template<typename T> 
inline void SingleValueAttributeMerger<T>::MergeSegment(
        const MergerResource& resource,    
        const index_base::SegmentMergeInfo& segMergeInfo,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,        
        const config::AttributeConfigPtr& attrConfig)

{
    std::tr1::shared_ptr<SingleAttributeSegmentReaderForMerge<T> > onDiskReader;
    onDiskReader.reset(new SingleAttributeSegmentReaderForMerge<T>(attrConfig));
    OpenSingleValueAttributeReader(
        *onDiskReader, segMergeInfo.segmentInfo, segMergeInfo.segmentId);
    auto reclaimMap = resource.reclaimMap;
    uint32_t docCount = segMergeInfo.segmentInfo.docCount;
    for (docid_t localId = 0; localId < (docid_t)docCount; ++localId)
    {
        docid_t newId = reclaimMap->GetNewId(localId + segMergeInfo.baseDocId);
        if (newId >= 0) // is not deleted
        {
            auto output = mSegOutputMapper.GetOutput(newId);
            if (!output)
            {
                continue;
            }

            T value;
            onDiskReader->Read(localId, (uint8_t*)&value, sizeof(value));
            *(T*)(&output->dataBuffer[output->bufferCursor]) = value;
            output->bufferCursor += sizeof(value);
            if (output->bufferCursor == output->bufferSize)
            {
                FlushDataBuffer(*output);
            }
        }
    }
    for (auto& output : mSegOutputMapper.GetOutputs())
    {
        if (output.dataBuffer)
        {
            FlushDataBuffer(output);
        }
    }
}

template <typename T>
inline void SingleValueAttributeMerger<T>::SortByWeightMerge(const MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)

{
    std::stringstream ss;
    std::for_each(
        outputSegMergeInfos.begin(), outputSegMergeInfos.end(),
        [ this, &ss, delim = "" ](index_base::OutputSegmentMergeInfo segInfo) {
            ss << delim << GetAttributePath(segInfo.path);
        });
    IE_LOG(DEBUG, "Start sort by weight merging to dir : %s", ss.str().c_str());
    std::vector<AttributeSegmentReaderPtr> segReaders;
    CreateSegmentReaders(segMergeInfos, segReaders);

    auto reclaimMap = resource.reclaimMap;

    PrepareOutputDatas(resource.reclaimMap, outputSegMergeInfos);

    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, reclaimMap);

    DocumentMergeInfo info;
    while (heap.GetNext(info))
    {
        auto output = mSegOutputMapper.GetOutput(info.newDocId);
        if (output)
        {
            T value;
            int32_t segmentIndex = info.segmentIndex;
            AttributeSegmentReaderPtr segReader = segReaders[segmentIndex];
            assert(segReader);
            segReader->Read(info.oldDocId, (uint8_t*)&value, sizeof(value));
            *(T*)(&output->dataBuffer[output->bufferCursor]) = value;            
            output->bufferCursor += sizeof(value);
            if (output->bufferCursor == output->bufferSize)
            {
                FlushDataBuffer(*output);
            }            
        }
    }

    for (auto& outputData : mSegOutputMapper.GetOutputs())
    {
        if (outputData.dataBuffer)
        {
            FlushDataBuffer(outputData);
        }
    }    

    if (AttributeCompressInfo::NeedCompressData(mAttributeConfig))
    {
        DumpCompressDataBuffer();
    }

    CloseFiles();
    DestroyBuffers();

    MergePatches(resource, segMergeInfos, outputSegMergeInfos);
    IE_LOG(DEBUG, "Finish sort by weight merging to dir : %s",
           ss.str().c_str());
}

template<typename T>
inline void SingleValueAttributeMerger<T>::CreateSegmentReaders(
        const index_base::SegmentMergeInfos& segMergeInfos, 
        std::vector<AttributeSegmentReaderPtr>& segReaders)
{
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        AttributeSegmentReaderPtr segReader;
        if (segMergeInfos[i].segmentInfo.docCount == 0)
        {
            segReaders.push_back(segReader);
            continue;
        }
        SingleAttributeSegmentReaderForMerge<T>* singleSegReader = 
            new SingleAttributeSegmentReaderForMerge<T>(mAttributeConfig);
        segReader.reset(singleSegReader);
        OpenSingleValueAttributeReader(*singleSegReader, 
                segMergeInfos[i].segmentInfo,
                segMergeInfos[i].segmentId);
        segReaders.push_back(segReader);
    }
}

template <typename T>
inline void SingleValueAttributeMerger<T>::OpenSingleValueAttributeReader(
        SingleAttributeSegmentReaderForMerge<T>& reader, 
        const index_base::SegmentInfo& segInfo, segmentid_t segmentId)
{
    reader.Open(mSegmentDirectory, segInfo, segmentId);
}

template <typename T>
inline void SingleValueAttributeMerger<T>::FlushCompressDataBuffer(OutputData& outputData)
{
    assert(mCompressDumpers.size() == mSegOutputMapper.GetOutputs().size());
    auto compressDumper = mCompressDumpers[outputData.outputIdx];
    assert(compressDumper.get());
    compressDumper->CompressData((T*)outputData.dataBuffer, outputData.bufferCursor / sizeof(T));
    outputData.bufferCursor = 0;
}

template <typename T>
inline void SingleValueAttributeMerger<T>::DumpCompressDataBuffer()
{
    assert(mCompressDumpers.size() == mSegOutputMapper.GetOutputs().size());
    for (size_t i = 0; i < mCompressDumpers.size(); ++i)
    {
        auto& compressDumper = mCompressDumpers[i];
        if (compressDumper)
        {
            compressDumper->Dump(mSegOutputMapper.GetOutputs()[i].dataFileWriter);
            compressDumper->Reset();
        }
    }
}

template <typename T>
int64_t SingleValueAttributeMerger<T>::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
    const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool isSortedMerge) const
{
    int64_t size = sizeof(*this);

    bool compressData = AttributeCompressInfo::NeedCompressData(this->mAttributeConfig);

    size += mRecordSize * DEFAULT_RECORD_COUNT * outputSegMergeInfos.size(); // write buffer
    if (compressData)
    {
        // TODO: check logic
        // assert(mCompressDumper.get() != NULL);
        // size += sizeof(*(mCompressDumper.get()));
        size += sizeof(EqualValueCompressDumper) * outputSegMergeInfos.size();
        size += mRecordSize * DEFAULT_RECORD_COUNT * outputSegMergeInfos.size(); // buffer for compress
    }

    int64_t segmentReaderTotalMemUse = 0;
    int64_t segmentReaderMaxMemUse = 0;

    
    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        uint32_t docCount = segMergeInfos[i].segmentInfo.docCount;
        if (docCount == 0)
        {
            continue;
        }
        int64_t memUse = SingleAttributeSegmentReaderForMerge<T>::EstimateMemoryUse(
                compressData, docCount);
        segmentReaderTotalMemUse += memUse;
        segmentReaderMaxMemUse = std::max(segmentReaderMaxMemUse, memUse);
    }

    if (isSortedMerge)
    {
        size += segmentReaderTotalMemUse;
    }
    else
    {
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

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_MERGER_H
