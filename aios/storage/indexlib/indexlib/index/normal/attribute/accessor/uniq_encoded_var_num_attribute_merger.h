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
#include <unordered_set>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/HashString.h"
namespace indexlib { namespace index {

template <typename T>
class UniqEncodedVarNumAttributeMerger : public VarNumAttributeMerger<T>
{
protected:
    using VarNumAttributeMerger<T>::mSegOutputMapper;
    using VarNumAttributeMerger<T>::mDataBuf;
    using OutputData = typename VarNumAttributeMerger<T>::OutputData;
    using typename VarNumAttributeMerger<T>::SegmentReaderWithCtx;

public:
    struct OffsetPair {
        OffsetPair(uint64_t _oldOffset, uint64_t _newOffset, docid_t _oldDocId, uint16_t _targetSegIdx)
            : oldOffset(_oldOffset)
            , newOffset(_newOffset)
            , oldDocId(_oldDocId)
            , targetSegmentIndex(_targetSegIdx)
        {
        }

        ~OffsetPair() {}
        bool operator<(const OffsetPair& other) const
        {
            if (oldOffset == other.oldOffset) {
                return targetSegmentIndex < other.targetSegmentIndex;
            }
            return oldOffset < other.oldOffset;
        }
        bool operator==(const OffsetPair& other) const
        {
            return oldOffset == other.oldOffset && targetSegmentIndex == other.targetSegmentIndex;
        }
        bool operator!=(const OffsetPair& other) const { return !(*this == other); }
        uint64_t oldOffset;
        uint64_t newOffset;
        docid_t oldDocId;
        uint16_t targetSegmentIndex;
    };

public:
    typedef std::unordered_set<docid_t> DocIdSet;
    typedef util::HashMap<uint64_t, uint64_t> EncodeMap;
    typedef std::vector<OffsetPair> SegmentOffsetMap;   // ordered vector
    typedef std::vector<SegmentOffsetMap> OffsetMapVec; // each segment
public:
    UniqEncodedVarNumAttributeMerger(bool needMergePatch) : VarNumAttributeMerger<T>(needMergePatch) {}
    ~UniqEncodedVarNumAttributeMerger() {}

public:
    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(uniq_var_num);

public:
    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

protected:
    void MergeData(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                   const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void CloseFiles() override;

    uint64_t FindOrGenerateNewOffset(uint8_t* dataBuf, uint32_t dataLen, OutputData& output);

private:
    virtual void MergePatchesInMergingSegments(const index_base::SegmentMergeInfos& segMergeInfos,
                                               const ReclaimMapPtr& reclaimMap, DocIdSet& patchedGlobalDocIdSet);

    void ConstuctSegmentOffsetMap(const index_base::SegmentMergeInfo& segMergeInfo, const ReclaimMapPtr& reclaimMap,
                                  const DocIdSet& patchedGlobalDocIdSet, const AttributeSegmentReaderPtr& segReader,
                                  SegmentOffsetMap& segmentOffsetMap);
    using VarNumAttributeMerger<T>::MergeData;
    void MergeData(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                   const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                   const DocIdSet& patchedGlobalDocIdSet, const std::vector<SegmentReaderWithCtx>& segReaders,
                   OffsetMapVec& offsetMapVec);
    void MergeOneSegmentData(const SegmentReaderWithCtx& reader, SegmentOffsetMap& segmentOffsetMap,
                             const ReclaimMapPtr& reclaimMap, docid_t baseDocId);

    void MergeNoPatchedDocOffsets(const index_base::SegmentMergeInfos& segMergeInfos, const ReclaimMapPtr& reclaimMap,
                                  const std::vector<SegmentReaderWithCtx>& segReaders, const OffsetMapVec& offsetMapVec,
                                  DocIdSet& patchedGlobalDocIdSet);
    typename VarNumAttributeMerger<T>::SegmentReaderWithCtx
    CreateSegmentReader(const index_base::SegmentMergeInfo& segMergeInfo) override;

private:
    friend class UpdatableUniqEncodedVarNumAttributeMergerTest;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, UniqEncodedVarNumAttributeMerger);
/////////////////////////////////////////////////

template <typename T>
inline typename VarNumAttributeMerger<T>::SegmentReaderWithCtx
UniqEncodedVarNumAttributeMerger<T>::CreateSegmentReader(const index_base::SegmentMergeInfo& segMergeInfo)
{
    file_system::DirectoryPtr directory = this->GetAttributeDirectory(this->mSegmentDirectory, segMergeInfo);
    assert(directory);
    typedef typename VarNumAttributeMerger<T>::SegmentReaderPtr SegmentReaderPtr;
    typedef typename VarNumAttributeMerger<T>::SegmentReader SegmentReader;
    SegmentReaderPtr reader(new SegmentReader(this->mAttributeConfig));
    index_base::PartitionDataPtr partData = this->mSegmentDirectory->GetPartitionData();
    index_base::SegmentData segmentData = partData->GetSegmentData(segMergeInfo.segmentId);
    reader->Open(segmentData, PatchApplyOption::NoPatch(), directory, nullptr, true);
    assert(reader->GetBaseAddress() == nullptr);
    return {reader, reader->CreateReadContextPtr(&this->mReadPool)};
}

template <typename T>
inline void
UniqEncodedVarNumAttributeMerger<T>::MergeData(const MergerResource& resource,
                                               const index_base::SegmentMergeInfos& segMergeInfos,
                                               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    auto reclaimMap = resource.reclaimMap;
    DocIdSet patchedGlobalDocIdSet;
    MergePatchesInMergingSegments(segMergeInfos, reclaimMap, patchedGlobalDocIdSet);

    OffsetMapVec offsetMapVec(segMergeInfos.size());
    MergeData(resource, segMergeInfos, outputSegMergeInfos, patchedGlobalDocIdSet, this->mSegReaders, offsetMapVec);
    MergeNoPatchedDocOffsets(segMergeInfos, reclaimMap, this->mSegReaders, offsetMapVec, patchedGlobalDocIdSet);
}

template <typename T>
inline void UniqEncodedVarNumAttributeMerger<T>::MergeData(
    const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, const DocIdSet& patchedGlobalDocIdSet,
    const std::vector<SegmentReaderWithCtx>& segReaders, OffsetMapVec& offsetMapVec)
{
    auto reclaimMap = resource.reclaimMap;
    for (size_t i = 0; i < segReaders.size(); ++i) {
        ConstuctSegmentOffsetMap(segMergeInfos[i], reclaimMap, patchedGlobalDocIdSet, segReaders[i].reader,
                                 offsetMapVec[i]);
        MergeOneSegmentData(segReaders[i], offsetMapVec[i], reclaimMap, segMergeInfos[i].baseDocId);
    }
}

template <typename T>
void UniqEncodedVarNumAttributeMerger<T>::CloseFiles()
{
    VarNumAttributeMerger<T>::CloseFiles();
}

template <typename T>
void UniqEncodedVarNumAttributeMerger<T>::ConstuctSegmentOffsetMap(const index_base::SegmentMergeInfo& segMergeInfo,
                                                                   const ReclaimMapPtr& reclaimMap,
                                                                   const DocIdSet& patchedGlobalDocIdSet,
                                                                   const AttributeSegmentReaderPtr& segReader,
                                                                   SegmentOffsetMap& segmentOffsetMap)
{
    uint32_t docCount = segMergeInfo.segmentInfo.docCount;
    segmentOffsetMap.reserve(docCount);
    docid_t baseDocId = segMergeInfo.baseDocId;
    autil::mem_pool::Pool pool;
    AttributeSegmentReader::ReadContextBasePtr ctx;
    if (segReader) {
        ctx = segReader->CreateReadContextPtr(&pool);
    }
    for (docid_t oldDocId = 0; oldDocId < (int64_t)docCount; oldDocId++) {
        docid_t globalDocId = baseDocId + oldDocId;
        auto newDocId = reclaimMap->GetNewId(globalDocId);
        if (newDocId == INVALID_DOCID || patchedGlobalDocIdSet.count(globalDocId)) {
            continue;
        }
        auto newLocalInfo = reclaimMap->GetLocalId(newDocId);
        auto output = mSegOutputMapper.GetOutputBySegIdx(newLocalInfo.first);
        if (!output) {
            continue;
        }
        typedef typename MultiValueAttributeSegmentReader<T>::ReadContext ReadContext;
        ReadContext* typedCtx = (ReadContext*)ctx.get();
        uint64_t oldOffset = typedCtx->offsetReader.GetOffset(oldDocId);
        segmentOffsetMap.push_back(OffsetPair(oldOffset, uint64_t(-1), oldDocId, newLocalInfo.first));
    }
    ctx.reset();
    std::sort(segmentOffsetMap.begin(), segmentOffsetMap.end());

    segmentOffsetMap.assign(segmentOffsetMap.begin(), std::unique(segmentOffsetMap.begin(), segmentOffsetMap.end()));
}

template <typename T>
inline void UniqEncodedVarNumAttributeMerger<T>::MergeOneSegmentData(const SegmentReaderWithCtx& segReader,
                                                                     SegmentOffsetMap& segmentOffsetMap,
                                                                     const ReclaimMapPtr& reclaimMap,
                                                                     docid_t oldBaseDocId)
{
    typename SegmentOffsetMap::iterator it = segmentOffsetMap.begin();

    for (; it != segmentOffsetMap.end(); it++) {
        auto newDocId = reclaimMap->GetNewId(oldBaseDocId + it->oldDocId);
        auto output = mSegOutputMapper.GetOutput(newDocId);
        assert(output);
        uint32_t dataLen = 0;

        assert(segReader.reader->GetBaseAddress() == nullptr);
        typedef typename MultiValueAttributeSegmentReader<T>::ReadContext ReadContext;
        ReadContext* typedCtx = (ReadContext*)segReader.ctx.get();
        bool isNull = false;
        segReader.reader->FetchValueFromStreamNoCopy(it->oldOffset, typedCtx->fileStream,
                                                     (uint8_t*)mDataBuf.GetBuffer(), dataLen, isNull);
        it->newOffset = FindOrGenerateNewOffset((uint8_t*)mDataBuf.GetBuffer(), dataLen, *output);
    }
}

template <typename T>
inline uint64_t UniqEncodedVarNumAttributeMerger<T>::FindOrGenerateNewOffset(uint8_t* dataBuf, uint32_t dataLen,
                                                                             OutputData& output)
{
    autil::StringView value((const char*)dataBuf, dataLen);
    uint64_t hashValue = output.dataWriter->GetHashValue(value);
    return output.dataWriter->AppendValueWithoutOffset(value, hashValue);
}

template <typename T>
inline void UniqEncodedVarNumAttributeMerger<T>::MergeNoPatchedDocOffsets(
    const index_base::SegmentMergeInfos& segMergeInfos, const ReclaimMapPtr& reclaimMap,
    const std::vector<SegmentReaderWithCtx>& segReaders, const OffsetMapVec& offsetMapVec,
    DocIdSet& patchedGlobalDocIdSet)
{
    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, reclaimMap);
    DocumentMergeInfo info;
    while (heap.GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        docid_t oldDocId = info.oldDocId;
        docid_t globalDocId = segMergeInfos[segIdx].baseDocId + oldDocId;
        if (patchedGlobalDocIdSet.count(globalDocId) > 0) {
            continue;
        }
        docid_t newDocId = info.newDocId;
        auto newLocalInfo = reclaimMap->GetLocalId(newDocId);
        auto output = mSegOutputMapper.GetOutputBySegIdx(newLocalInfo.first);
        if (!output) {
            continue;
        }
        typedef typename MultiValueAttributeSegmentReader<T>::ReadContext ReadContext;
        ReadContext* typedCtx = (ReadContext*)segReaders[segIdx].ctx.get();
        uint64_t oldOffset = typedCtx->offsetReader.GetOffset(oldDocId);
        const SegmentOffsetMap& offsetMap = offsetMapVec[segIdx];
        typename SegmentOffsetMap::const_iterator it = std::lower_bound(
            offsetMap.begin(), offsetMap.end(), OffsetPair(oldOffset, uint64_t(-1), INVALID_DOCID, newLocalInfo.first));
        assert(it != offsetMap.end() && *it == OffsetPair(oldOffset, uint64_t(-1), INVALID_DOCID, newLocalInfo.first));
        output->dataWriter->SetOffset(newLocalInfo.second, it->newOffset);
    }
}

template <typename T>
void UniqEncodedVarNumAttributeMerger<T>::MergePatchesInMergingSegments(
    const index_base::SegmentMergeInfos& segMergeInfos, const ReclaimMapPtr& reclaimMap,
    DocIdSet& patchedGlobalDocIdSet)
{
    std::vector<AttributePatchReaderPtr> patchReaders = this->CreatePatchReaders(segMergeInfos);

    this->ReserveMemBuffers(patchReaders, this->mSegReaders);
    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, reclaimMap);
    DocumentMergeInfo info;
    while (heap.GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        docid_t currentLocalId = info.oldDocId;
        auto output = mSegOutputMapper.GetOutput(info.newDocId);
        if (!output) {
            continue;
        }
        const AttributePatchReaderPtr& patchReader = patchReaders[segIdx];
        uint32_t dataLen = patchReader->Seek(currentLocalId, (uint8_t*)mDataBuf.GetBuffer(), mDataBuf.GetBufferSize());
        if (dataLen > 0) {
            autil::StringView value((const char*)mDataBuf.GetBuffer(), dataLen);
            output->dataWriter->AppendValue(value);
            docid_t globalDocId = segMergeInfos[segIdx].baseDocId + currentLocalId;
            patchedGlobalDocIdSet.insert(globalDocId);
        } else {
            // delay merge data. Index: merge order, Value: [newDocId --> DocumentMergeInfo]
            output->dataWriter->AppendOffset(0);
        }
    }
}

template <typename T>
int64_t UniqEncodedVarNumAttributeMerger<T>::EstimateMemoryUse(
    const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos, const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
    bool isSortedMerge) const
{
    int64_t size = VarNumAttributeMerger<T>::EstimateMemoryUse(segDir, resource, segMergeInfos, outputSegMergeInfos,
                                                               isSortedMerge);
    uint32_t totalDocCount = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        totalDocCount += segMergeInfos[i].segmentInfo.docCount;
    }
    int64_t encodeMapMem = EncodeMap::EstimateNeededMemory(totalDocCount / outputSegMergeInfos.size()) *
                           outputSegMergeInfos.size(); // EncodeMap memory use
    IE_LOG(INFO, "encodeMapMem: %ld MB", encodeMapMem / 1024 / 1024);
    size += encodeMapMem;
    size += sizeof(OffsetPair) * totalDocCount; // OffsetMap
    return size;
}
}} // namespace indexlib::index
