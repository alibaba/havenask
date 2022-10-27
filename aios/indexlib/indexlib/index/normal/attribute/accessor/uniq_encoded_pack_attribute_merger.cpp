#include "indexlib/index/normal/attribute/accessor/uniq_encoded_pack_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/dedup_pack_attribute_patch_file_merger.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_patch_reader.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"

using namespace autil;
using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, UniqEncodedPackAttributeMerger);

UniqEncodedPackAttributeMerger::UniqEncodedPackAttributeMerger(
    bool needMergePatch, size_t patchBufferSize)
    : UniqEncodedVarNumAttributeMerger<char>(needMergePatch)
    , mPatchBuffer(patchBufferSize)
    , mBufferThreshold(0)
    , mPatchBufferSize(patchBufferSize)
{}

UniqEncodedPackAttributeMerger::~UniqEncodedPackAttributeMerger() 
{}

void UniqEncodedPackAttributeMerger::Init(
        const PackAttributeConfigPtr& packAttrConf,
        const index_base::MergeItemHint& hint,
        const index_base::MergeTaskResourceVector& taskResources)
{
    UniqEncodedVarNumAttributeMerger<char>::Init(
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


void UniqEncodedPackAttributeMerger::MergePatchesInMergingSegments(
    const SegmentMergeInfos &segMergeInfos,
    const ReclaimMapPtr &reclaimMap,
    DocIdSet &patchedGlobalDocIdSet)
{
     vector<AttributePatchReaderPtr> patchReaders =
        this->CreatePatchReaders(segMergeInfos);
    
    ReserveMemBuffers(patchReaders, mSegReaders);
    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, reclaimMap);
    while (!heap.IsEmpty())
    {
        PatchedDocInfoVec docInfoVec;
        GeneratePatchedDocInfos(segMergeInfos, heap, patchReaders,
                                patchedGlobalDocIdSet, docInfoVec, reclaimMap);

        // sort doc by segmentIdx and offset 
        sort(docInfoVec.begin(), docInfoVec.end(), PatchDocInfoComparator());
        MergePatchedDocInfoAndDump(segMergeInfos, docInfoVec, reclaimMap);
    }
}

void UniqEncodedPackAttributeMerger::GeneratePatchedDocInfos(
    const SegmentMergeInfos& segMergeInfos, DocumentMergeInfoHeap& heap,
    const vector<AttributePatchReaderPtr>& patchReaders, DocIdSet& patchedGlobalDocIdSet,
    PatchedDocInfoVec& docInfoVec, const ReclaimMapPtr& reclaimMap)
{
    IE_LOG(INFO, "begin generate patch for one batch");
    DocumentMergeInfo info;
    size_t cur = 0;
    while (!heap.IsEmpty())
    {
        PatchedDocInfo* patchDocInfo = AllocatePatchDocInfo(cur);
        if (!patchDocInfo)
        {
            // buffer is not enough
            break;
        }
        heap.GetNext(info);
        auto output = mSegOutputMapper.GetOutput(info.newDocId);
        if (!output)
        {
            continue;
        }
        auto& offsetDumper = output->offsetDumper;
        
        if (FillPatchDocInfo((docid_t)offsetDumper->Size(), info, patchReaders, patchDocInfo))
        {
            // doc has patch
            cur += (sizeof(PatchedDocInfo) + patchDocInfo->dataLen);
            docInfoVec.push_back(patchDocInfo);
            docid_t globalDocId =
                segMergeInfos[patchDocInfo->segIdx].baseDocId + info.oldDocId;
            patchedGlobalDocIdSet.insert(globalDocId);
        }
        // push back place-holder
        offsetDumper->PushBack(0);
    }
    IE_LOG(INFO, "end generate patch for one batch");
}

UniqEncodedPackAttributeMerger::PatchedDocInfo*
UniqEncodedPackAttributeMerger::AllocatePatchDocInfo(size_t cur)
{
    if (cur + mBufferThreshold > mPatchBuffer.GetBufferSize())
    {
        return NULL;
    }
    return (PatchedDocInfo*)(mPatchBuffer.GetBuffer() + cur);
}

bool UniqEncodedPackAttributeMerger::FillPatchDocInfo(
    docid_t docIdInNewSegment,
    const DocumentMergeInfo& docMergeInfo,
    const vector<AttributePatchReaderPtr>& patchReaders,
    PatchedDocInfo* const patchDocInfo)
{
    const AttributePatchReaderPtr& patchReader =
        patchReaders[docMergeInfo.segmentIndex];
    assert(patchReader);
    
    uint32_t dataLen = patchReader->Seek(docMergeInfo.oldDocId,
            patchDocInfo->data, mBufferThreshold - sizeof(PatchedDocInfo));
    if (dataLen == 0)
    {
        // doc has no patch
        return false;
    }
    const AttributeSegmentReaderPtr& segReader =
        mSegReaders[docMergeInfo.segmentIndex];
    assert(segReader);
    
    patchDocInfo->newDocId = docIdInNewSegment;
    patchDocInfo->oldDocId = docMergeInfo.oldDocId;;
    patchDocInfo->segIdx = docMergeInfo.segmentIndex;
    patchDocInfo->offset = segReader->GetOffset(docMergeInfo.oldDocId);
    patchDocInfo->dataLen = dataLen;
    return true;
}

void UniqEncodedPackAttributeMerger::MergePatchedDocInfoAndDump(
    const SegmentMergeInfos& segMergeInfos, const PatchedDocInfoVec& docInfoVec,
    const ReclaimMapPtr& reclaimMap)
{
    IE_LOG(INFO, "begin merge and dump patch for one batch");
    int32_t lastSegIdx = -1;
    docid_t lastDocId = INVALID_DOCID;
    uint32_t dataLen;
    for (size_t i = 0; i < docInfoVec.size(); ++i)
    {
        PackAttributeFormatter::PackAttributeFields patchFields;
        if(!PackAttributeFormatter::DecodePatchValues(
               docInfoVec[i]->data, docInfoVec[i]->dataLen, patchFields))
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                                 "decode patch value for new doc [%d] failed.",
                                 docInfoVec[i]->newDocId);
        }

        auto oldGlobalId
            = segMergeInfos[docInfoVec[i]->segIdx].baseDocId + docInfoVec[i]->oldDocId;
        auto newLocalInfo = reclaimMap->GetNewLocalId(oldGlobalId);
        auto output = mSegOutputMapper.GetOutputBySegIdx(newLocalInfo.first);
        assert(output);

        if (docInfoVec[i]->segIdx != lastSegIdx
            || docInfoVec[i]->oldDocId != lastDocId)
        {
            const AttributeSegmentReaderPtr& segReader = mSegReaders[docInfoVec[i]->segIdx];

            if (!segReader->ReadDataAndLen(docInfoVec[i]->oldDocId, (uint8_t*)mDataBuf.GetBuffer(),
                    mDataBuf.GetBufferSize(), dataLen))
            {
                INDEXLIB_FATAL_ERROR(IndexCollapsed,
                    "read attribute data for new doc [%d] failed.", docInfoVec[i]->newDocId);
            }
            lastSegIdx = docInfoVec[i]->segIdx;
            lastDocId = docInfoVec[i]->oldDocId;
        }
        MergePatchAndDumpData(patchFields, dataLen, newLocalInfo.second, *output);
    }
    IE_LOG(INFO, "end merge and dump patch for one batch");
}

void UniqEncodedPackAttributeMerger::MergePatchAndDumpData(
    const PackAttributeFormatter::PackAttributeFields& patchFields,
    uint32_t dataLen, docid_t docIdInNewSegment, OutputData& output)
{
    // skip count header(length)
    size_t encodeCountLen = 0;
    VarNumAttributeFormatter::DecodeCount(mDataBuf.GetBuffer(), encodeCountLen);
    const char* pData = (const char*)mDataBuf.GetBuffer() + encodeCountLen;

    ConstString mergedValue =
        mPackAttrFormatter->MergeAndFormatUpdateFields(
                (const char*)pData, patchFields, false, mMergeBuffer);
    
    if (mergedValue.empty())
    {
        IE_LOG(ERROR, "merge patch value for new doc [%d] failed, "
               "merge data without patch.", docIdInNewSegment);
        uint64_t newOffset = FindOrGenerateNewOffset(
            (uint8_t*)mDataBuf.GetBuffer(), dataLen, output);
        output.offsetDumper->SetOffset(docIdInNewSegment, newOffset);
        return;
    }
    
    // remove hashKey
    // todo xxx: hash key has been caculated twice. 
    common::AttrValueMeta decodeMeta = mDataConvertor->Decode(mergedValue);
    ConstString decodeValue = decodeMeta.data;
    uint64_t newOffset = FindOrGenerateNewOffset(
        (uint8_t*)decodeValue.data(), decodeValue.length(), output);
    output.offsetDumper->SetOffset(docIdInNewSegment, newOffset);    
}

AttributePatchReaderPtr UniqEncodedPackAttributeMerger::CreatePatchReaderForSegment(
    segmentid_t segmentId)
{
    PackAttributePatchReaderPtr patchReader(new PackAttributePatchReader(mPackAttrConfig));
    patchReader->Init(mSegmentDirectory->GetPartitionData(), segmentId);
    return patchReader;
}

void UniqEncodedPackAttributeMerger::MergePatches(const MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    DoMergePatches<DedupPackAttributePatchFileMerger>(
        resource, segMergeInfos, outputSegMergeInfos, mPackAttrConfig);
}

int64_t UniqEncodedPackAttributeMerger::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
    const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool isSortedMerge) const
{
    DedupPackAttributePatchFileMerger patchMerger(mPackAttrConfig);
    return UniqEncodedVarNumAttributeMerger<char>::EstimateMemoryUse(
        segDir, resource, segMergeInfos, outputSegMergeInfos, isSortedMerge)
        + mPatchBufferSize
        + GetMaxDataItemLen(segDir, segMergeInfos) * 3 // mergeBuffer(2) + worst patchBuffer(1) 
        + patchMerger.EstimateMemoryUse(
            segDir->GetPartitionData(), segMergeInfos);
}

void UniqEncodedPackAttributeMerger::ReserveMemBuffers(
        const vector<AttributePatchReaderPtr>& patchReaders,
        const vector<AttributeSegmentReaderPtr>& segReaders)
{
    uint32_t maxPatchItemLen = 0;
    for (size_t i = 0; i < patchReaders.size(); i++)
    {
        maxPatchItemLen = std::max(
                patchReaders[i]->GetMaxPatchItemLen(), maxPatchItemLen);
    }

    mBufferThreshold = maxPatchItemLen + sizeof(PatchedDocInfo);
    mPatchBuffer.Reserve(mPatchBuffer.GetBufferSize() + mBufferThreshold);

    uint32_t maxDataItemLen = 0;
    for (size_t i = 0; i < segReaders.size(); i++)
    {
        typedef DFSVarNumAttributeSegmentReader<char> DFSReader;
        typedef std::tr1::shared_ptr<DFSReader> DFSReaderPtr;
        DFSReaderPtr dfsSegReader = DYNAMIC_POINTER_CAST(DFSReader, segReaders[i]);
        if (dfsSegReader)
        {
            maxDataItemLen = std::max(dfsSegReader->GetMaxDataItemLen(), maxDataItemLen);
        }
    }
    
    mMergeBuffer.Reserve(maxDataItemLen * 2);    
    // only used to read data, not copy mergevalue
    mDataBuf.Reserve(maxDataItemLen);
}

void UniqEncodedPackAttributeMerger::ReleaseMemBuffers()
{
    mPatchBuffer.Release();
    mMergeBuffer.Release();
    mDataBuf.Release();
}

IE_NAMESPACE_END(index);
