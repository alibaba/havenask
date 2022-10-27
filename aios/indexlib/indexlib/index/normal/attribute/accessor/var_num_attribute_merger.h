#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_MERGER_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "fslib/fslib.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index/segment_output_mapper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info_heap.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_creator.h"
#include "indexlib/index/normal/attribute/accessor/dfs_var_num_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/adaptive_attribute_offset_dumper.h"
#include "indexlib/index/normal/attribute/equal_value_compress_dumper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_merger.h"
#include "indexlib/index/normal/attribute/accessor/dedup_patch_file_merger.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/index_meta/segment_info.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class VarNumAttributeMerger : public AttributeMerger
{
protected:
    struct OutputData
    {
        size_t outputIdx = 0;
        uint64_t fileOffset = 0;
        uint32_t dataItemCount = 0;
        uint32_t maxItemLen = 0;
        file_system::FileWriterPtr dataFileWriter;
        file_system::FileWriterPtr offsetFile;
        file_system::FileWriterPtr dataInfoFile;

        AdaptiveAttributeOffsetDumperPtr offsetDumper;
        
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

    void SortByWeightMerge(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;    

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge) const override;

public:
    // for test
    void SetOffsetThreshold(uint64_t threshold);

protected:
    void DoMerge(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    virtual void MergeData(
        const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);
    
    virtual void MergeData(const std::string& dir,
        const index_base::SegmentMergeInfos& segMergeInfos, const ReclaimMapPtr& reclaimMap)
    {
        assert(false);
    }

    virtual void ReserveMemBuffers(
            const std::vector<AttributePatchReaderPtr>& patchReaders,
            const std::vector<AttributeSegmentReaderPtr>& segReaders);

    virtual void ReleaseMemBuffers();

    std::vector<AttributeSegmentReaderPtr> CreateSegmentReaders(
        const index_base::SegmentMergeInfos& segMergeInfos);
    virtual AttributeSegmentReaderPtr CreateSegmentReader(
            const index_base::SegmentMergeInfo& segMergeInfo);
    
    virtual void PrepareOutputDatas(const ReclaimMapPtr& reclaimMap,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);
    virtual void DestroyOutputDatas();
    virtual void CloseFiles();
    virtual void DumpOffsetFile();   
    virtual uint32_t ReadData(
        docid_t docId, const AttributeSegmentReaderPtr &attributeSegmentReader,
        const AttributePatchReaderPtr &patchReader, 
        uint8_t* dataBuf, uint32_t dataBufLen);
    void ClearSegReaders(std::vector<AttributeSegmentReaderPtr>& segReaders);

    void DumpDataInfoFile()
    {
        for (auto& output : mSegOutputMapper.GetOutputs())
        {
            AttributeDataInfo dataInfo(output.dataItemCount, output.maxItemLen);
            std::string content = dataInfo.ToString();
            output.dataInfoFile->Write(content.c_str(), content.length());
        }
    }
    
protected:
    virtual void MergePatches(
        const std::string& dir, const index_base::SegmentMergeInfos& segMergeInfos)
    {
        assert(false);
    }
    virtual void MergePatches(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    virtual AttributePatchReaderPtr CreatePatchReaderForSegment(segmentid_t segmentId);

    std::vector<AttributePatchReaderPtr> CreatePatchReaders(
            const index_base::SegmentMergeInfos& segMergeInfos);

    virtual AttributePatchReaderPtr CreatePatchReader();

    uint32_t GetMaxDataItemLen(
            const SegmentDirectoryBasePtr& segDir,
            const index_base::SegmentMergeInfos& segMergeInfos) const;

    file_system::DirectoryPtr GetAttributeDirectory(
            const SegmentDirectoryBasePtr& segDir,
            const index_base::SegmentMergeInfo& segMergeInfo) const;

protected:
    util::MemBuffer mDataBuf;
    std::vector<AttributeSegmentReaderPtr> mSegReaders;
    util::SimplePool mPool;
    SegmentOutputMapper<OutputData> mSegOutputMapper;

private:
    friend class UpdatableVarNumAttributeMergerTest;
    friend class VarNumAttributeMergerInteTest;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumAttributeMerger);
///////////////////////////////////////////////

template <typename T>
VarNumAttributeMerger<T>::VarNumAttributeMerger(bool needMergePatch)
    : AttributeMerger(needMergePatch)
{
}

template <typename T>
VarNumAttributeMerger<T>::~VarNumAttributeMerger()
{
    // must release dumper before pool released: mSegOutputMapper shuold declared after mPool
}

template <typename T>
inline void VarNumAttributeMerger<T>::Merge(
    const MergerResource &resource,
    const index_base::SegmentMergeInfos &segMergeInfos,
    const index_base::OutputSegmentMergeInfos &outputSegMergeInfos) {
    std::stringstream ss;
    std::for_each(
        outputSegMergeInfos.begin(), outputSegMergeInfos.end(),
        [ this, &ss, delim = "" ](index_base::OutputSegmentMergeInfo segInfo) {
            ss << delim << GetAttributePath(segInfo.path);
        });
    IE_LOG(DEBUG, "Start merging to dirs : %s", ss.str().c_str());
    DoMerge(resource, segMergeInfos, outputSegMergeInfos);
    IE_LOG(DEBUG, "Finish merging to dir : %s", ss.str().c_str());
}

template <typename T>
inline void VarNumAttributeMerger<T>::SortByWeightMerge(
    const MergerResource &resource,
    const index_base::SegmentMergeInfos &segMergeInfos,
    const index_base::OutputSegmentMergeInfos &outputSegMergeInfos) {
    std::stringstream ss;
    std::for_each(
        outputSegMergeInfos.begin(), outputSegMergeInfos.end(),
        [ this, &ss, delim = "" ](index_base::OutputSegmentMergeInfo segInfo) {
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

    //Attention mSegReaders clear can throw exception, trigger double delete
    ClearSegReaders(mSegReaders);

    DumpDataInfoFile();
    DumpOffsetFile();
    DestroyOutputDatas();
    ReleaseMemBuffers();
    MergePatches(resource, segMergeInfos, outputSegMergeInfos);
}

template <typename T>
inline void VarNumAttributeMerger<T>::ReserveMemBuffers(
        const std::vector<AttributePatchReaderPtr>& patchReaders,
        const std::vector<AttributeSegmentReaderPtr>& segReaders)
{
    uint32_t maxItemLen = 0;
    for (size_t i = 0; i < patchReaders.size(); i++)
    {
        maxItemLen = std::max(patchReaders[i]->GetMaxPatchItemLen(), maxItemLen);
    }

    for (size_t i = 0; i < segReaders.size(); i++)
    {
        typedef DFSVarNumAttributeSegmentReader<T> DFSReader;
        typedef std::tr1::shared_ptr<DFSReader> DFSReaderPtr;
        DFSReaderPtr dfsSegReader = DYNAMIC_POINTER_CAST(DFSReader, segReaders[i]);
        if (dfsSegReader)
        {
            maxItemLen = std::max(dfsSegReader->GetMaxDataItemLen(), maxItemLen);
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
inline void VarNumAttributeMerger<T>::ClearSegReaders(
        std::vector<AttributeSegmentReaderPtr>& segReaders)
{
    while(!segReaders.empty())
    {
        AttributeSegmentReaderPtr segReader = segReaders.back();
        segReaders.pop_back();
    }
}

template <typename T>
inline void VarNumAttributeMerger<T>::MergeData(
    const MergerResource& resource,
    const index_base::SegmentMergeInfos &segMergeInfos,
    const index_base::OutputSegmentMergeInfos &outputSegMergeInfos)    
{
    std::vector<AttributePatchReaderPtr> patchReaders =
        CreatePatchReaders(segMergeInfos);
    DocumentMergeInfoHeap heap;
    auto reclaimMap = resource.reclaimMap;
    heap.Init(segMergeInfos, reclaimMap);

    ReserveMemBuffers(patchReaders, mSegReaders);
    DocumentMergeInfo info;

    while (heap.GetNext(info))
    {
        int32_t segIdx = info.segmentIndex;
        docid_t currentLocalId = info.oldDocId;
        auto output = mSegOutputMapper.GetOutput(info.newDocId);
        if (!output)
        {
            continue;
        }
        uint32_t dataLen = ReadData(currentLocalId, mSegReaders[segIdx], patchReaders[segIdx],
            (uint8_t*)mDataBuf.GetBuffer(), mDataBuf.GetBufferSize());
        output->offsetDumper->PushBack(output->fileOffset);
        output->fileOffset += dataLen;
        output->dataFileWriter->Write(mDataBuf.GetBuffer(), dataLen);
        ++output->dataItemCount;
        output->maxItemLen = std::max(output->maxItemLen, dataLen);
    }
}

template <typename T>
uint32_t VarNumAttributeMerger<T>::ReadData(
        docid_t docId,
        const AttributeSegmentReaderPtr &attributeSegmentReader,
        const AttributePatchReaderPtr &patchReader, 
        uint8_t* dataBuf, uint32_t dataBufLen)
{
    uint32_t dataLen = patchReader->Seek(docId, dataBuf, dataBufLen);
    if (dataLen > 0)
    {
        return dataLen;
    }
    
    if(!attributeSegmentReader->ReadDataAndLen(
           docId, dataBuf, dataBufLen, dataLen))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "read attribute data for doc [%d] failed.",
                             docId);    
    }
    return dataLen;
}

template<typename T>
inline std::vector<AttributeSegmentReaderPtr> VarNumAttributeMerger<T>::CreateSegmentReaders(
        const index_base::SegmentMergeInfos& segMergeInfos)
{
    std::vector<AttributeSegmentReaderPtr> segReaders;
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        AttributeSegmentReaderPtr segReader;
        if (segMergeInfos[i].segmentInfo.docCount == 0)
        {
            segReaders.push_back(segReader);
            continue;
        }
        segReader = CreateSegmentReader(segMergeInfos[i]);
        segReaders.push_back(segReader);
    }
    return segReaders;
}

template<typename T>
inline file_system::DirectoryPtr VarNumAttributeMerger<T>::GetAttributeDirectory(
        const SegmentDirectoryBasePtr& segDir, 
        const index_base::SegmentMergeInfo& segMergeInfo) const
{
    index_base::PartitionDataPtr partData = 
        segDir->GetPartitionData();
    assert(partData);

    index_base::SegmentData segData = 
        partData->GetSegmentData(segMergeInfo.segmentId);

    const std::string& attrName = mAttributeConfig->GetAttrName();
    file_system::DirectoryPtr directory;
    config::AttributeConfig::ConfigType configType = 
        mAttributeConfig->GetConfigType();
    if (configType == config::AttributeConfig::ct_section)
    {
        std::string indexName =
            config::SectionAttributeConfig::SectionAttributeNameToIndexName(attrName);
        directory = segData.GetSectionAttributeDirectory(indexName, true);
    }
    else
    {
        directory = segData.GetAttributeDirectory(attrName, true);
    }
    return directory;
}


template<typename T>
inline AttributeSegmentReaderPtr VarNumAttributeMerger<T>::CreateSegmentReader(
        const index_base::SegmentMergeInfo& segMergeInfo)
{
    file_system::DirectoryPtr directory = 
        GetAttributeDirectory(mSegmentDirectory, segMergeInfo);
    assert(directory);

    AttributeSegmentReaderPtr reader;
    std::unique_ptr<DFSVarNumAttributeSegmentReader<T> > onDiskReader(
            new DFSVarNumAttributeSegmentReader<T>(mAttributeConfig));
    onDiskReader->Open(directory, segMergeInfo.segmentInfo);
    reader.reset(onDiskReader.release());
    return reader;
}

template <typename T>
inline void VarNumAttributeMerger<T>::PrepareOutputDatas(const ReclaimMapPtr& reclaimMap,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    auto createFunc
        = [this](const index_base::OutputSegmentMergeInfo& outputInfo, size_t outputIdx) {
              OutputData output;
              output.outputIdx = outputIdx;
              output.fileOffset = 0;
              
              const file_system::DirectoryPtr& attributeDir = outputInfo.directory;
              attributeDir->RemoveDirectory(mAttributeConfig->GetAttrName(), true);
              file_system::DirectoryPtr directory = attributeDir->MakeDirectory(
                  mAttributeConfig->GetAttrName());
              output.dataFileWriter = directory->CreateFileWriter(ATTRIBUTE_DATA_FILE_NAME);
              output.offsetFile = directory->CreateFileWriter(ATTRIBUTE_OFFSET_FILE_NAME);
              output.dataInfoFile = directory->CreateFileWriter(ATTRIBUTE_DATA_INFO_FILE_NAME);

              output.offsetDumper.reset(new AdaptiveAttributeOffsetDumper(&mPool));
              output.offsetDumper->Init(mAttributeConfig);
              return output;
          };
    mSegOutputMapper.Init(reclaimMap, outputSegMergeInfos, createFunc);
}

template <typename T> inline void VarNumAttributeMerger<T>::DestroyOutputDatas() {
    for (auto& output : mSegOutputMapper.GetOutputs())
    {
        output.dataFileWriter->Close();
        output.offsetFile->Close();
        output.dataInfoFile->Close();
    }
    mSegOutputMapper.Clear();
    mDataBuf.Release();
}

template <typename T>
inline void VarNumAttributeMerger<T>::CloseFiles()
{
}

template<typename T>
inline void VarNumAttributeMerger<T>::DumpOffsetFile()
{
    for (auto& output : mSegOutputMapper.GetOutputs())
    {
        if (output.offsetDumper->Size() > 0)
        {
            output.offsetDumper->PushBack(output.fileOffset);
            output.offsetDumper->Dump(output.offsetFile);
            output.offsetDumper->Clear(); // release memory right now
        }
    }
}

template<typename T>
std::vector<AttributePatchReaderPtr>
VarNumAttributeMerger<T>::CreatePatchReaders(
        const index_base::SegmentMergeInfos& segMergeInfos)
{
    std::vector<AttributePatchReaderPtr> patchReaders;
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        AttributePatchReaderPtr patchReader;
        patchReader = CreatePatchReaderForSegment(segMergeInfos[i].segmentId);
        patchReaders.push_back(patchReader);
    }
    return patchReaders;
}

template<typename T>
AttributePatchReaderPtr
VarNumAttributeMerger<T>::CreatePatchReaderForSegment(segmentid_t segmentId)
{
    index_base::PatchFileFinderPtr patchFinder = 
        index_base::PatchFileFinderCreator::Create(
                mSegmentDirectory->GetPartitionData().get());

    index_base::AttrPatchFileInfoVec patchVec;
    patchFinder->FindAttrPatchFilesForSegment(
            mAttributeConfig, segmentId, patchVec);

    AttributePatchReaderPtr patchReader(CreatePatchReader());
    for (size_t i = 0; i < patchVec.size(); i++)
    {
        patchReader->AddPatchFile(patchVec[i].patchDirectory,
                patchVec[i].patchFileName, patchVec[i].srcSegment);
    }

    return patchReader;
}

template<typename T>
AttributePatchReaderPtr VarNumAttributeMerger<T>::CreatePatchReader()
{
    return AttributePatchReaderPtr(new VarNumAttributePatchReader<T>(mAttributeConfig));
}

template <typename T>
void VarNumAttributeMerger<T>::MergePatches(
    const MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    DoMergePatches<DedupPatchFileMerger>(resource, segMergeInfos, outputSegMergeInfos, mAttributeConfig);
}

template <typename T>
inline uint32_t VarNumAttributeMerger<T>::GetMaxDataItemLen(
        const SegmentDirectoryBasePtr& segDir,
        const index_base::SegmentMergeInfos& segMergeInfos) const
{
    uint32_t maxItemLen = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        uint32_t docCount = segMergeInfos[i].segmentInfo.docCount;
        if (docCount == 0)
        {
            continue;
        }

        file_system::DirectoryPtr attrDir = GetAttributeDirectory(
                segDir, segMergeInfos[i]);
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
    const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool isSortedMerge) const
{
    int64_t size = sizeof(*this);
    size += sizeof(AdaptiveAttributeOffsetDumper) * outputSegMergeInfos.size();
    size += GetMaxDataItemLen(segDir, segMergeInfos); // dataBufferSize

    uint32_t totalDocCount = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        uint32_t docCount = segMergeInfos[i].segmentInfo.docCount;
        if (docCount == 0)
        {
            continue;
        }
        size += DFSVarNumAttributeSegmentReader<T>::EstimateMemoryUse(docCount);
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


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_MERGER_H
