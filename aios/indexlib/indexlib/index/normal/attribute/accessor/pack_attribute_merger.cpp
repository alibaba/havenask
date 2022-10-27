#include "indexlib/index/normal/attribute/accessor/pack_attribute_merger.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_patch_reader.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/index/normal/attribute/accessor/dedup_pack_attribute_patch_file_merger.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PackAttributeMerger);

PackAttributeMerger::PackAttributeMerger(bool needMergePatch)
    : VarNumAttributeMerger<char>(needMergePatch)
{
}

PackAttributeMerger::~PackAttributeMerger() 
{}

void PackAttributeMerger::Init(const PackAttributeConfigPtr& packAttrConf,
                               const MergeItemHint& hint,
                               const MergeTaskResourceVector& taskResources)
{
    VarNumAttributeMerger<char>::Init(
            packAttrConf->CreateAttributeConfig(), hint, taskResources);
    mPackAttrConfig = packAttrConf;
    mPackAttrFormatter.reset(new PackAttributeFormatter());
    mPackAttrFormatter->Init(packAttrConf);
    
    AttributeConfigPtr packDataAttrConfig =
        mPackAttrConfig->CreateAttributeConfig();
    mDataConvertor.reset(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
            packDataAttrConfig->GetFieldConfig()));
    assert(mDataConvertor);
}
 
AttributePatchReaderPtr PackAttributeMerger::CreatePatchReaderForSegment(
    segmentid_t segmentId)
{
    PackAttributePatchReaderPtr patchReader(new PackAttributePatchReader(mPackAttrConfig));
    patchReader->Init(mSegmentDirectory->GetPartitionData(), segmentId);
    return patchReader;
}

int64_t PackAttributeMerger::EstimateMemoryUse(
const SegmentDirectoryBasePtr& segDir,
    const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool isSortedMerge) const
{
    DedupPackAttributePatchFileMerger patchMerger(mPackAttrConfig);
    
    // TODO: reserve buffer
    return VarNumAttributeMerger<char>::EstimateMemoryUse(
        segDir, resource, segMergeInfos, outputSegMergeInfos, isSortedMerge)
        + GetMaxDataItemLen(segDir, segMergeInfos) * 3 // mergerBuffer(2) + worst patchBuffer(1)
        + patchMerger.EstimateMemoryUse(
            segDir->GetPartitionData(), segMergeInfos);
}

uint32_t PackAttributeMerger::ReadData(
    docid_t docId,
    const AttributeSegmentReaderPtr &attributeSegmentReader,
    const AttributePatchReaderPtr &patchReader, 
    uint8_t* dataBuf, uint32_t dataBufLen)
{
    uint32_t patchDataLen, dataLen;
    patchDataLen = patchReader->Seek(docId, (uint8_t*)mPatchBuffer.GetBuffer(), 
            mPatchBuffer.GetBufferSize());
    if(!attributeSegmentReader->ReadDataAndLen(
                    docId, dataBuf, dataBufLen, dataLen))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "read attribute data for doc [%d] failed.", docId);
    }
    
    if (patchDataLen == 0)
    {
        return dataLen;
    }
    PackAttributeFormatter::PackAttributeFields patchFields;
    if(!PackAttributeFormatter::DecodePatchValues(
                    (uint8_t*)mPatchBuffer.GetBuffer(), 
                    patchDataLen, patchFields))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "decode patch value for doc [%d] failed.",
                             docId);
    }

    // skip count header(length)
    size_t encodeCountLen = 0;
    VarNumAttributeFormatter::DecodeCount((const char*)dataBuf, encodeCountLen);
    const char* pData = (const char*)dataBuf + encodeCountLen;
    ConstString mergedValue = mPackAttrFormatter->MergeAndFormatUpdateFields(
            (const char*)pData, patchFields, false, mMergeBuffer);
    if (mergedValue.empty())
    {
        IE_LOG(ERROR, "merge patch value for doc [%d] failed."
               "ReadData without patch.", docId);
        return dataLen;
    }
    
    // remove hashKey
    common::AttrValueMeta decodeMeta = mDataConvertor->Decode(mergedValue);
    ConstString decodeValue = decodeMeta.data;

    assert(decodeValue.length() <= dataBufLen);
    memcpy(dataBuf, decodeValue.data(), decodeValue.length());
    return decodeValue.length();
}

void PackAttributeMerger::MergePatches(
    const MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    DoMergePatches<DedupPackAttributePatchFileMerger>(
        resource, segMergeInfos, outputSegMergeInfos, mPackAttrConfig);
}

void PackAttributeMerger::ReserveMemBuffers(
        const vector<AttributePatchReaderPtr>& patchReaders,
        const vector<AttributeSegmentReaderPtr>& segReaders)
{
    uint32_t maxPatchItemLen = 0;
    for (size_t i = 0; i < patchReaders.size(); i++)
    {
        maxPatchItemLen = std::max(
                patchReaders[i]->GetMaxPatchItemLen(), maxPatchItemLen);
    }
    mPatchBuffer.Reserve(maxPatchItemLen);

    uint32_t maxDataItemLen = 0;
    for (size_t i = 0; i < segReaders.size(); i++)
    {
        typedef DFSVarNumAttributeSegmentReader<char> DFSReader;
        typedef std::tr1::shared_ptr<DFSReader> DFSReaderPtr;
        DFSReaderPtr dfsSegReader = DYNAMIC_POINTER_CAST(DFSReader, segReaders[i]);
        if (dfsSegReader)
        {
            maxDataItemLen = std::max(
                    dfsSegReader->GetMaxDataItemLen(), maxDataItemLen);
        }
    }
    mMergeBuffer.Reserve(maxDataItemLen * 2);
    mDataBuf.Reserve(maxPatchItemLen + maxDataItemLen);
    // copy mergeValue to dataBuffer
}

void PackAttributeMerger::ReleaseMemBuffers()
{
    mPatchBuffer.Release();
    mMergeBuffer.Release();
}

IE_NAMESPACE_END(index);

