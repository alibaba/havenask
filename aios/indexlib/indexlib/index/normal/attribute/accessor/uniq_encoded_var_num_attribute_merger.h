#ifndef __INDEXLIB_UNIQ_ENCODED_VAR_NUM_ATTRIBUTE_MERGER_H
#define __INDEXLIB_UNIQ_ENCODED_VAR_NUM_ATTRIBUTE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <unordered_set>
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/util/hash_string.h"
IE_NAMESPACE_BEGIN(index);

template <typename T>
class UniqEncodedVarNumAttributeMerger : public VarNumAttributeMerger<T>
{
protected:
    using VarNumAttributeMerger<T>::mSegOutputMapper;
    using VarNumAttributeMerger<T>::mDataBuf;
    using OutputData = typename VarNumAttributeMerger<T>::OutputData;

public:
    struct OffsetPair {
        OffsetPair(uint64_t _oldOffset, uint64_t _newOffset, docid_t _oldDocId,
                 uint16_t _targetSegIdx)
            : oldOffset(_oldOffset), newOffset(_newOffset), oldDocId(_oldDocId),
              targetSegmentIndex(_targetSegIdx) {}

        ~OffsetPair() {}
        bool operator< (const OffsetPair &other) const {
            if (oldOffset == other.oldOffset) {
                return targetSegmentIndex < other.targetSegmentIndex;
            }
            return oldOffset < other.oldOffset;
        }
        bool operator== (const OffsetPair &other) const {
            return oldOffset == other.oldOffset && targetSegmentIndex == other.targetSegmentIndex;
        }
        bool operator!= (const OffsetPair &other) const {
            return !(*this == other);
        }
        uint64_t oldOffset;
        uint64_t newOffset;
        docid_t oldDocId;
        uint16_t targetSegmentIndex;
    };

public:
    typedef std::unordered_set<docid_t> DocIdSet;
    typedef util::HashMap<uint64_t, uint64_t> EncodeMap;
    typedef std::unique_ptr<EncodeMap> EncodeMapPtr;
    typedef std::vector<OffsetPair> SegmentOffsetMap; // ordered vector
    typedef std::vector<SegmentOffsetMap> OffsetMapVec; // each segment
public:
    UniqEncodedVarNumAttributeMerger(bool needMergePatch)
        : VarNumAttributeMerger<T>(needMergePatch)
    {
        
    }
    ~UniqEncodedVarNumAttributeMerger() {}
public:
    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(uniq_var_num);
public:
    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge) const override;

protected:
    void MergeData(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void CloseFiles() override;
    
    uint64_t FindOrGenerateNewOffset(uint8_t* dataBuf, uint32_t dataLen, OutputData& output);

private:
    virtual void MergePatchesInMergingSegments(
            const index_base::SegmentMergeInfos &segMergeInfos,
            const ReclaimMapPtr &reclaimMap,
            DocIdSet &patchedGlobalDocIdSet);
    
    void ConstuctSegmentOffsetMap(
            const index_base::SegmentMergeInfo& segMergeInfo,
            const ReclaimMapPtr &reclaimMap,
            const DocIdSet &patchedGlobalDocIdSet,
            const AttributeSegmentReaderPtr& segReader,
            SegmentOffsetMap &segmentOffsetMap);
    void MergeData(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        const DocIdSet& patchedGlobalDocIdSet,
        const std::vector<AttributeSegmentReaderPtr>& segReaders, OffsetMapVec& offsetMapVec);
    void MergeOneSegmentData(const AttributeSegmentReaderPtr& reader,
        SegmentOffsetMap& segmentOffsetMap, const ReclaimMapPtr& reclaimMap, docid_t baseDocId);

    void MergeNoPatchedDocOffsets(
            const index_base::SegmentMergeInfos &segMergeInfos,
            const ReclaimMapPtr &reclaimMap,
            const std::vector<AttributeSegmentReaderPtr> &segReaders,
            const OffsetMapVec &offsetMapVec,
            DocIdSet &patchedGlobalDocIdSet);

protected:
    std::vector<EncodeMapPtr> mEncodeMaps;

private:
    friend class UpdatableUniqEncodedVarNumAttributeMergerTest;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, UniqEncodedVarNumAttributeMerger);
/////////////////////////////////////////////////

template <typename T>
inline void UniqEncodedVarNumAttributeMerger<T>::MergeData(const MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    assert(mEncodeMaps.empty());
    mEncodeMaps.reserve(mSegOutputMapper.GetOutputs().size());
    for (size_t i = 0; i < mSegOutputMapper.GetOutputs().size(); ++i)
    {
        mEncodeMaps.emplace_back(new EncodeMap(HASHMAP_INIT_SIZE));
    } 
    
    auto reclaimMap = resource.reclaimMap;
    DocIdSet patchedGlobalDocIdSet;
    MergePatchesInMergingSegments(segMergeInfos, reclaimMap, patchedGlobalDocIdSet);

    OffsetMapVec offsetMapVec(segMergeInfos.size());
    MergeData(resource, segMergeInfos, outputSegMergeInfos, patchedGlobalDocIdSet,
              this->mSegReaders, offsetMapVec);
    for (auto& encodeMap : mEncodeMaps)
    {
        encodeMap->Clear();
    }
    MergeNoPatchedDocOffsets(segMergeInfos, reclaimMap, this->mSegReaders, 
                             offsetMapVec, patchedGlobalDocIdSet);
}

template <typename T>
inline void UniqEncodedVarNumAttributeMerger<T>::MergeData(const MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
    const DocIdSet& patchedGlobalDocIdSet,
    const std::vector<AttributeSegmentReaderPtr>& segReaders, OffsetMapVec& offsetMapVec)
{
    auto reclaimMap = resource.reclaimMap;
    for (size_t i = 0; i < segReaders.size(); ++i)
    {
        ConstuctSegmentOffsetMap(segMergeInfos[i], reclaimMap, 
                patchedGlobalDocIdSet, segReaders[i], offsetMapVec[i]);
        MergeOneSegmentData(segReaders[i], offsetMapVec[i], reclaimMap, segMergeInfos[i].baseDocId);
    }
}

template<typename T>
void UniqEncodedVarNumAttributeMerger<T>::CloseFiles()
{
    VarNumAttributeMerger<T>::CloseFiles();
}

template<typename T>
void UniqEncodedVarNumAttributeMerger<T>::ConstuctSegmentOffsetMap(
        const index_base::SegmentMergeInfo& segMergeInfo,
        const ReclaimMapPtr &reclaimMap,
        const DocIdSet &patchedGlobalDocIdSet,
        const AttributeSegmentReaderPtr& segReader,
        SegmentOffsetMap &segmentOffsetMap)
{
    uint32_t docCount = segMergeInfo.segmentInfo.docCount;
    segmentOffsetMap.reserve(docCount);
    docid_t baseDocId = segMergeInfo.baseDocId;
    for (docid_t oldDocId = 0; oldDocId < (int64_t)docCount; oldDocId++)
    {
        docid_t globalDocId = baseDocId + oldDocId;
        auto newDocId = reclaimMap->GetNewId(globalDocId);
        if (newDocId == INVALID_DOCID || patchedGlobalDocIdSet.count(globalDocId))
        {
            continue;
        }
        auto newLocalInfo = reclaimMap->GetLocalId(newDocId);
        auto output = mSegOutputMapper.GetOutputBySegIdx(newLocalInfo.first);
        if (!output)
        {
            continue;
        }
        uint64_t oldOffset = segReader->GetOffset(oldDocId);
        segmentOffsetMap.push_back(OffsetPair(oldOffset, uint64_t(-1), oldDocId, newLocalInfo.first));
    }
    std::sort(segmentOffsetMap.begin(), segmentOffsetMap.end());

    segmentOffsetMap.assign(segmentOffsetMap.begin(),
                            std::unique(segmentOffsetMap.begin(), segmentOffsetMap.end()));
}

template <typename T>
inline void UniqEncodedVarNumAttributeMerger<T>::MergeOneSegmentData(
        const AttributeSegmentReaderPtr &reader,
        SegmentOffsetMap &segmentOffsetMap,
        const ReclaimMapPtr& reclaimMap,
        docid_t oldBaseDocId)
{
    typename SegmentOffsetMap::iterator it = segmentOffsetMap.begin();
    
    for (; it != segmentOffsetMap.end(); it++)
    {
        auto newDocId = reclaimMap->GetNewId(oldBaseDocId + it->oldDocId);
        auto output = mSegOutputMapper.GetOutput(newDocId);
        assert(output);
        uint32_t dataLen = 0;
        bool ret = reader->ReadDataAndLen(it->oldDocId, (uint8_t*)mDataBuf.GetBuffer(),
            mDataBuf.GetBufferSize(), dataLen);
        assert(ret); (void)ret;
        it->newOffset = FindOrGenerateNewOffset(
            (uint8_t*)mDataBuf.GetBuffer(), dataLen, *output);
    }
}

template <typename T>
inline uint64_t UniqEncodedVarNumAttributeMerger<T>::FindOrGenerateNewOffset(
    uint8_t* dataBuf, uint32_t dataLen, OutputData& output)
{
    uint64_t newOffset;
    uint64_t hashValue = util::HashString::Hash((const char*)dataBuf, dataLen);
    assert(output.outputIdx < mEncodeMaps.size());
    auto& encodeMap = mEncodeMaps[output.outputIdx];
    uint64_t* retNewOffset = encodeMap->Find(hashValue);
    if (retNewOffset != NULL)
    {
        newOffset = *retNewOffset;
    }
    else
    {
        newOffset = output.fileOffset;
        encodeMap->Insert(hashValue, newOffset);
        output.fileOffset += dataLen;
        output.dataFileWriter->Write(dataBuf, dataLen);
        output.dataItemCount++;
        output.maxItemLen = std::max(output.maxItemLen, dataLen);
    }
    return newOffset;
}

template <typename T>
inline void UniqEncodedVarNumAttributeMerger<T>::MergeNoPatchedDocOffsets(
        const index_base::SegmentMergeInfos &segMergeInfos,
        const ReclaimMapPtr &reclaimMap,
        const std::vector<AttributeSegmentReaderPtr> &segReaders,
        const OffsetMapVec &offsetMapVec,
        DocIdSet &patchedGlobalDocIdSet)
{
    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, reclaimMap);
    DocumentMergeInfo info;
    while (heap.GetNext(info))
    {    
        int32_t segIdx = info.segmentIndex;
        docid_t oldDocId = info.oldDocId;
        docid_t globalDocId = segMergeInfos[segIdx].baseDocId + oldDocId;
        if (patchedGlobalDocIdSet.count(globalDocId) > 0)
        {
            continue;
        }
        docid_t newDocId = info.newDocId;
        auto newLocalInfo = reclaimMap->GetLocalId(newDocId);
        auto output = mSegOutputMapper.GetOutputBySegIdx(newLocalInfo.first);
        if (!output)
        {
            continue;
        }
        const AttributeSegmentReaderPtr& reader = segReaders[segIdx];
        uint64_t oldOffset = reader->GetOffset(oldDocId);
        const SegmentOffsetMap &offsetMap = offsetMapVec[segIdx];
        typename SegmentOffsetMap::const_iterator it =
            std::lower_bound(offsetMap.begin(), offsetMap.end(),
                             OffsetPair(oldOffset, uint64_t(-1), INVALID_DOCID,
                                        newLocalInfo.first));
        assert(it != offsetMap.end() &&
               *it == OffsetPair(oldOffset, uint64_t(-1), INVALID_DOCID,
                                 newLocalInfo.first));
        output->offsetDumper->SetOffset(newLocalInfo.second, it->newOffset);
    }
}

template <typename T>
void UniqEncodedVarNumAttributeMerger<T>::MergePatchesInMergingSegments(
    const index_base::SegmentMergeInfos& segMergeInfos,
    const ReclaimMapPtr &reclaimMap,    
    DocIdSet & patchedGlobalDocIdSet)
{
    std::vector<AttributePatchReaderPtr> patchReaders =
        this->CreatePatchReaders(segMergeInfos);

    this->ReserveMemBuffers(patchReaders, this->mSegReaders);
    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, reclaimMap);
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
        const AttributePatchReaderPtr& patchReader = patchReaders[segIdx];

        uint32_t dataLen = patchReader->Seek(
            currentLocalId, (uint8_t*)mDataBuf.GetBuffer(), mDataBuf.GetBufferSize());
        if (dataLen > 0)
        {
            // TODO: refactor args
            uint64_t newOffset = FindOrGenerateNewOffset(
                (uint8_t*)mDataBuf.GetBuffer(), dataLen, *output);
            output->offsetDumper->PushBack(newOffset);

            docid_t globalDocId = segMergeInfos[segIdx].baseDocId + currentLocalId;
            patchedGlobalDocIdSet.insert(globalDocId);
        }
        else
        {
            // delay merge data. Index: merge order, Value: [newDocId --> DocumentMergeInfo]
            output->offsetDumper->PushBack(0);
        }
    }
}

template <typename T>
int64_t UniqEncodedVarNumAttributeMerger<T>::EstimateMemoryUse(
    const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool isSortedMerge) const
{
    int64_t size = VarNumAttributeMerger<T>::EstimateMemoryUse(
        segDir, resource, segMergeInfos, outputSegMergeInfos, isSortedMerge);
    uint32_t totalDocCount = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        totalDocCount += segMergeInfos[i].segmentInfo.docCount;
    }
    int64_t encodeMapMem
        = EncodeMap::EstimateNeededMemory(totalDocCount / outputSegMergeInfos.size())
        * outputSegMergeInfos.size(); // EncodeMap memory use
    IE_LOG(INFO, "encodeMapMem: %ld MB", encodeMapMem/1024/1024);
    size += encodeMapMem;
    size += sizeof(OffsetPair) * totalDocCount; // OffsetMap
    return size;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_UNIQ_ENCODED_VAR_NUM_ATTRIBUTE_MERGER_H
