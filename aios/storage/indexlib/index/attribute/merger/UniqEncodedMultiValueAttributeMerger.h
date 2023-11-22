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

#include "indexlib/index/attribute/MultiSliceAttributeDiskIndexer.h"
#include "indexlib/index/attribute/merger/MultiValueAttributeMerger.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/HashString.h"

namespace indexlibv2::index {

template <typename T>
class UniqEncodedMultiValueAttributeMerger : public MultiValueAttributeMerger<T>
{
protected:
    using MultiValueAttributeMerger<T>::_segOutputMapper;
    using MultiValueAttributeMerger<T>::_dataBuf;
    using MultiValueAttributeMerger<T>::_diskIndexers;
    using typename MultiValueAttributeMerger<T>::OutputData;
    using typename MultiValueAttributeMerger<T>::DiskIndexerWithCtx;
    using MultiValueAttributeMerger<T>::ReserveMemBuffers;

public:
    struct OffsetPair {
        OffsetPair(uint64_t _oldOffset, uint64_t _newOffset, docid_t _oldDocId, uint16_t _targetSegId)
            : oldOffset(_oldOffset)
            , newOffset(_newOffset)
            , oldDocId(_oldDocId)
            , targetSegmentId(_targetSegId)
        {
        }

        ~OffsetPair() {}
        bool operator<(const OffsetPair& other) const
        {
            if (oldOffset == other.oldOffset) {
                return targetSegmentId < other.targetSegmentId;
            }
            return oldOffset < other.oldOffset;
        }
        bool operator==(const OffsetPair& other) const
        {
            return oldOffset == other.oldOffset && targetSegmentId == other.targetSegmentId;
        }
        bool operator!=(const OffsetPair& other) const { return !(*this == other); }
        uint64_t oldOffset;
        uint64_t newOffset;
        docid_t oldDocId;
        segmentid_t targetSegmentId;
    };

    using DocIdSet = std::unordered_set<docid_t>;
    using EncodeMap = indexlib::util::HashMap<uint64_t, uint64_t>;
    using SegmentOffsetMap = std::vector<OffsetPair>;   // ordered vector
    using OffsetMapVec = std::vector<SegmentOffsetMap>; // each segment

public:
    UniqEncodedMultiValueAttributeMerger() : MultiValueAttributeMerger<T>() {}
    virtual ~UniqEncodedMultiValueAttributeMerger() {}

public:
    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(uniq_multi);

protected:
    Status MergeData(const std::shared_ptr<DocMapper>& docMapper,
                     const IIndexMerger::SegmentMergeInfos& segMergeInfos) override;
    Status CloseFiles() override;

    std::pair<Status, uint64_t> FindOrGenerateNewOffset(uint8_t* dataBuf, uint32_t dataLen, OutputData& output);

private:
    // virtual for uniq_encode_pack_attr_merger
    virtual Status MergePatchesInMergingSegments(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                                 const std::shared_ptr<DocMapper>& docMapper,
                                                 DocIdSet& patchedGlobalDocIdSet);

    Status ConstructSegmentOffsetMap(const IIndexMerger::SourceSegment& segMergeInfo,
                                     const std::shared_ptr<DocMapper>& docMapper, const DocIdSet& patchedGlobalDocIdSet,
                                     const std::shared_ptr<AttributeDiskIndexer>& diskIndexer,
                                     SegmentOffsetMap& segmentOffsetMap);
    using MultiValueAttributeMerger<T>::MergeData;
    Status MergeData(const std::shared_ptr<DocMapper>& docMapper, const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                     const DocIdSet& patchedGlobalDocIdSet, OffsetMapVec& offsetMapVec);
    Status MergeOneSegmentData(const DiskIndexerWithCtx& diskIndexer, SegmentOffsetMap& segmentOffsetMap,
                               const std::shared_ptr<DocMapper>& docMapper, docid_t baseDocId);

    Status MergeNoPatchedDocOffsets(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                    const std::shared_ptr<DocMapper>& docMapper, const OffsetMapVec& offsetMapVec,
                                    DocIdSet& patchedGlobalDocIdSet);
    Status GetPatchReaders(const std::vector<IIndexMerger::SourceSegment>& srcSegments,
                           std::vector<std::shared_ptr<MultiValueAttributePatchReader<T>>>& patchReaders);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, UniqEncodedMultiValueAttributeMerger, T);
/////////////////////////////////////////////////

template <typename T>
inline Status UniqEncodedMultiValueAttributeMerger<T>::MergeData(const std::shared_ptr<DocMapper>& docMapper,
                                                                 const IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    DocIdSet patchedGlobalDocIdSet;
    auto st = MergePatchesInMergingSegments(segMergeInfos, docMapper, patchedGlobalDocIdSet);
    RETURN_IF_STATUS_ERROR(st, "merge patches failed.");

    OffsetMapVec offsetMapVec(segMergeInfos.srcSegments.size());
    st = MergeData(docMapper, segMergeInfos, patchedGlobalDocIdSet, offsetMapVec);
    RETURN_IF_STATUS_ERROR(st, "merge data failed.");
    st = MergeNoPatchedDocOffsets(segMergeInfos, docMapper, offsetMapVec, patchedGlobalDocIdSet);
    RETURN_IF_STATUS_ERROR(st, "merge no patched doc offset failed.");
    return Status::OK();
}

template <typename T>
inline Status UniqEncodedMultiValueAttributeMerger<T>::MergeData(const std::shared_ptr<DocMapper>& docMapper,
                                                                 const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                                                 const DocIdSet& patchedGlobalDocIdSet,
                                                                 OffsetMapVec& offsetMapVec)
{
    const std::vector<DiskIndexerWithCtx>& diskIndexers = _diskIndexers;
    for (size_t i = 0; i < diskIndexers.size(); ++i) {
        auto st = ConstructSegmentOffsetMap(segMergeInfos.srcSegments[i], docMapper, patchedGlobalDocIdSet,
                                            diskIndexers[i].first, offsetMapVec[i]);
        RETURN_IF_STATUS_ERROR(st, "construct segment offset map failed.");
        st = MergeOneSegmentData(diskIndexers[i], offsetMapVec[i], docMapper, segMergeInfos.srcSegments[i].baseDocid);
        RETURN_IF_STATUS_ERROR(st, "merge one segment data failed, segIdx[%lu]", i);
    }
    return Status::OK();
}

template <typename T>
Status UniqEncodedMultiValueAttributeMerger<T>::CloseFiles()
{
    return MultiValueAttributeMerger<T>::CloseFiles();
}

template <typename T>
Status UniqEncodedMultiValueAttributeMerger<T>::ConstructSegmentOffsetMap(
    const IIndexMerger::SourceSegment& srcSegment, const std::shared_ptr<DocMapper>& docMapper,
    const DocIdSet& patchedGlobalDocIdSet, const std::shared_ptr<AttributeDiskIndexer>& diskIndexer,
    SegmentOffsetMap& segmentOffsetMap)
{
    uint32_t docCount = srcSegment.segment->GetSegmentInfo()->docCount;
    segmentOffsetMap.reserve(docCount);
    docid_t baseDocId = srcSegment.baseDocid;
    autil::mem_pool::Pool pool;
    std::shared_ptr<AttributeDiskIndexer::ReadContextBase> ctx;
    if (diskIndexer != nullptr) {
        ctx = diskIndexer->CreateReadContextPtr(&pool);
    }
    for (docid_t oldDocId = 0; oldDocId < (int64_t)docCount; ++oldDocId) {
        docid_t globalDocId = baseDocId + oldDocId;
        const auto& [targetSegmentId, _] = docMapper->Map(globalDocId);
        if (globalDocId == INVALID_DOCID || patchedGlobalDocIdSet.count(globalDocId)) {
            continue;
        }
        auto output = _segOutputMapper.GetOutputBySegId(targetSegmentId);
        if (!output) {
            continue;
        }
        typedef typename MultiValueAttributeDiskIndexer<T>::ReadContext ReadContext;
        auto multiSliceReadContext =
            std::dynamic_pointer_cast<MultiSliceAttributeDiskIndexer::MultiSliceReadContext>(ctx);
        assert(multiSliceReadContext);
        ReadContext* typedCtx = (ReadContext*)multiSliceReadContext->sliceReadContexts[0].get();
        assert(typedCtx != nullptr);
        auto [st, oldOffset] = typedCtx->offsetReader.GetOffset(oldDocId);
        RETURN_IF_STATUS_ERROR(st, "get patch offset failed, docId[%d]", oldDocId);
        segmentOffsetMap.emplace_back(OffsetPair(oldOffset, uint64_t(-1), oldDocId, targetSegmentId));
    }
    ctx.reset();
    std::sort(segmentOffsetMap.begin(), segmentOffsetMap.end());

    segmentOffsetMap.assign(segmentOffsetMap.begin(), std::unique(segmentOffsetMap.begin(), segmentOffsetMap.end()));
    return Status::OK();
}

template <typename T>
inline Status UniqEncodedMultiValueAttributeMerger<T>::MergeOneSegmentData(const DiskIndexerWithCtx& diskIndexerWithCtx,
                                                                           SegmentOffsetMap& segmentOffsetMap,
                                                                           const std::shared_ptr<DocMapper>& docMapper,
                                                                           docid_t oldBaseDocId)
{
    if (segmentOffsetMap.size() == 0) {
        return Status::OK();
    }
    const auto& [indexer, ctx] = diskIndexerWithCtx;
    auto multiSliceIndexer = std::dynamic_pointer_cast<MultiSliceAttributeDiskIndexer>(indexer);
    assert(multiSliceIndexer);
    assert(multiSliceIndexer->GetSliceCount() == 1);
    typedef MultiValueAttributeDiskIndexer<T> DiskIndexer;
    auto diskIndexer = multiSliceIndexer->template GetSliceIndexer<DiskIndexer>(0);
    assert(diskIndexer);
    typename SegmentOffsetMap::iterator it = segmentOffsetMap.begin();

    for (; it != segmentOffsetMap.end(); ++it) {
        auto output = _segOutputMapper.GetOutput(oldBaseDocId + it->oldDocId);
        assert(output);
        uint32_t dataLen = 0;

        typedef typename MultiValueAttributeDiskIndexer<T>::ReadContext ReadContext;
        auto multiSliceReadContext =
            std::dynamic_pointer_cast<MultiSliceAttributeDiskIndexer::MultiSliceReadContext>(ctx);
        ReadContext* typedCtx = (ReadContext*)multiSliceReadContext->sliceReadContexts[0].get();
        bool isNull = false;
        diskIndexer->FetchValueFromStreamNoCopy(it->oldOffset, typedCtx->fileStream, (uint8_t*)_dataBuf.GetBuffer(),
                                                dataLen, isNull);
        auto [st, offset] = FindOrGenerateNewOffset((uint8_t*)_dataBuf.GetBuffer(), dataLen, *output);
        RETURN_IF_STATUS_ERROR(st, "get new offset failed.");
        it->newOffset = offset;
    }
    return Status::OK();
}

template <typename T>
inline std::pair<Status, uint64_t>
UniqEncodedMultiValueAttributeMerger<T>::FindOrGenerateNewOffset(uint8_t* dataBuf, uint32_t dataLen, OutputData& output)
{
    autil::StringView value((const char*)dataBuf, dataLen);
    uint64_t hashValue = output.dataWriter->GetHashValue(value);
    auto [st, offset] = output.dataWriter->AppendValueWithoutOffset(value, hashValue);
    RETURN2_IF_STATUS_ERROR(st, 0, "append value failed in VarLenDataWriter. ");
    return std::make_pair(Status::OK(), offset);
}

template <typename T>
inline Status UniqEncodedMultiValueAttributeMerger<T>::MergeNoPatchedDocOffsets(
    const IIndexMerger::SegmentMergeInfos& segMergeInfos, const std::shared_ptr<DocMapper>& docMapper,
    const OffsetMapVec& offsetMapVec, DocIdSet& patchedGlobalDocIdSet)
{
    const std::vector<DiskIndexerWithCtx>& diskIndexers = _diskIndexers;
    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, docMapper);
    DocumentMergeInfo info;
    while (heap.GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        docid_t oldDocId = info.oldDocId;
        if (patchedGlobalDocIdSet.count(oldDocId) > 0) {
            continue;
        }
        const auto& [targetSegmentId, targetDocId] = docMapper->Map(oldDocId);
        auto output = _segOutputMapper.GetOutputBySegId(targetSegmentId);
        if (!output) {
            continue;
        }
        typedef typename MultiValueAttributeDiskIndexer<T>::ReadContext ReadContext;
        auto multiSliceReadContext = std::dynamic_pointer_cast<MultiSliceAttributeDiskIndexer::MultiSliceReadContext>(
            diskIndexers[segIdx].second);
        ReadContext* typedCtx = (ReadContext*)multiSliceReadContext->sliceReadContexts[0].get();
        auto [st, oldOffset] = typedCtx->offsetReader.GetOffset(oldDocId - segMergeInfos.srcSegments[segIdx].baseDocid);
        RETURN_IF_STATUS_ERROR(st, "get patch offset failed, docId[%d]", oldDocId);
        const SegmentOffsetMap& offsetMap = offsetMapVec[segIdx];
        typename SegmentOffsetMap::const_iterator it = std::lower_bound(
            offsetMap.begin(), offsetMap.end(), OffsetPair(oldOffset, uint64_t(-1), INVALID_DOCID, targetSegmentId));
        assert(it != offsetMap.end() && *it == OffsetPair(oldOffset, uint64_t(-1), INVALID_DOCID, targetSegmentId));
        output->dataWriter->SetOffset(targetDocId, it->newOffset);
    }
    return Status::OK();
}
template <typename T>
Status UniqEncodedMultiValueAttributeMerger<T>::GetPatchReaders(
    const std::vector<IIndexMerger::SourceSegment>& srcSegments,
    std::vector<std::shared_ptr<MultiValueAttributePatchReader<T>>>& patchReaders)
{
    std::map<segmentid_t, std::shared_ptr<AttributePatchReader>> segId2PatchReader;
    auto status = AttributeMerger::CreatePatchReader(srcSegments, AttributeMerger::_allPatchInfos,
                                                     AttributeMerger::_attributeConfig, segId2PatchReader);
    RETURN_IF_STATUS_ERROR(status, "load patch reader failed.");
    for (const auto& [_, segment] : srcSegments) {
        auto segId = segment->GetSegmentId();
        auto it = segId2PatchReader.find(segId);
        if (it == segId2PatchReader.end()) {
            patchReaders.emplace_back(nullptr);
        } else {
            auto reader = std::dynamic_pointer_cast<MultiValueAttributePatchReader<T>>(it->second);
            assert(reader != nullptr);
            patchReaders.emplace_back(std::move(reader));
        }
    }
    return Status::OK();
}

template <typename T>
Status UniqEncodedMultiValueAttributeMerger<T>::MergePatchesInMergingSegments(
    const IIndexMerger::SegmentMergeInfos& segMergeInfos, const std::shared_ptr<DocMapper>& docMapper,
    DocIdSet& patchedGlobalDocIdSet)
{
    std::vector<std::shared_ptr<MultiValueAttributePatchReader<T>>> patchReaders;
    auto status = GetPatchReaders(segMergeInfos.srcSegments, patchReaders);
    RETURN_IF_STATUS_ERROR(status, "get patch reader failed.");

    ReserveMemBuffers(_diskIndexers);
    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, docMapper);
    DocumentMergeInfo info;
    while (heap.GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        auto output = _segOutputMapper.GetOutput(info.oldDocId);
        if (!output) {
            continue;
        }
        assert(segIdx < patchReaders.size());
        const auto patchReader = patchReaders[segIdx];
        if (patchReader == nullptr) {
            // delay merge data. Index: merge order, Value: [newDocId --> DocumentMergeInfo]
            output->dataWriter->AppendOffset(0);
            continue;
        }
        docid_t oldLocalDocId = info.oldDocId - segMergeInfos.srcSegments[segIdx].baseDocid;
        auto [st, dataLen] = patchReader->Seek(oldLocalDocId, (uint8_t*)_dataBuf.GetBuffer(), _dataBuf.GetBufferSize());
        RETURN_IF_STATUS_ERROR(st, "patch reader seek failed, docId[%d]", oldLocalDocId);
        if (dataLen > 0) {
            autil::StringView value((const char*)_dataBuf.GetBuffer(), dataLen);
            st = output->dataWriter->AppendValue(value);
            RETURN_IF_STATUS_ERROR(st, "append value failed in VarLenDataWriter. ");
            patchedGlobalDocIdSet.insert(info.oldDocId);
        } else {
            // delay merge data. Index: merge order, Value: [newDocId --> DocumentMergeInfo]
            output->dataWriter->AppendOffset(0);
        }
    }
    return Status::OK();
}
} // namespace indexlibv2::index
