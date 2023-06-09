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
#include "indexlib/index/data_structure/var_len_data_merger.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarLenDataMerger);

VarLenDataMerger::VarLenDataMerger(const VarLenDataParam& param) : mParam(param), mPool(DEFAULT_POOL_BUFFER_SIZE) {}

VarLenDataMerger::~VarLenDataMerger() {}

void VarLenDataMerger::Init(const MergerResource& resource, const vector<InputData>& inputReaders,
                            const vector<OutputData>& outputWriters)
{
    auto reclaimMap = resource.reclaimMap;
    SegmentMergeInfos segMergeInfos;
    segMergeInfos.reserve(inputReaders.size());
    for (auto input : inputReaders) {
        segMergeInfos.push_back(input.segMergeInfo);
    }
    mHeap.Init(segMergeInfos, reclaimMap);

    vector<pair<OutputData, segmentindex_t>> outputInfos;
    outputInfos.reserve(outputWriters.size());
    for (auto output : outputWriters) {
        outputInfos.push_back(make_pair(output, output.targetSegmentIndex));
    }
    mSegOutputMapper.Init(reclaimMap, outputInfos);
    mInputDatas = inputReaders;
    mOutputDatas = outputWriters;
}

size_t VarLenDataMerger::EstimateMemoryUse(uint32_t docCount)
{
    size_t size = docCount * sizeof(uint64_t)                       // offset
                  + file_system::ReaderOption::DEFAULT_BUFFER_SIZE; // data buffer
    if (mParam.dataItemUniqEncode) {
        size += docCount * sizeof(OffsetPair);
        size += docCount * sizeof(uint64_t); // offsetMap
    }
    return size;
}

void VarLenDataMerger::Merge()
{
    if (mParam.dataItemUniqEncode) {
        UniqMerge();
    } else {
        NormalMerge();
    }
    CloseOutputDatas();
}

void VarLenDataMerger::NormalMerge()
{
    vector<char> patchValueBuffer;
    auto reclaimMap = mHeap.GetReclaimMap();
    DocumentMergeInfo info;
    while (mHeap.GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        docid_t currentLocalId = info.oldDocId;
        auto output = mSegOutputMapper.GetOutput(info.newDocId);
        if (!output) {
            continue;
        }

        autil::StringView data;
        if (mInputDatas[segIdx].segPatchReader) {
            uint32_t maxPatchItemLen = mInputDatas[segIdx].segPatchReader->GetMaxPatchItemLen();
            if (patchValueBuffer.size() < maxPatchItemLen) {
                patchValueBuffer.resize(maxPatchItemLen);
            }
            uint32_t dataLen = mInputDatas[segIdx].segPatchReader->Seek(
                currentLocalId, (uint8_t*)patchValueBuffer.data(), patchValueBuffer.size());
            if (dataLen > 0) {
                data = StringView(patchValueBuffer.data(), dataLen);
            }
        }

        if (data.length() == 0 && !mInputDatas[segIdx].dataReader->GetValue(currentLocalId, data, &mPool)) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "read data for doc [%d] failed.", currentLocalId);
        }
        output->dataWriter->AppendValue(data);
        ResetPoolBuffer();
    }
}

void VarLenDataMerger::UniqMerge()
{
    ReserveMergedOffsets();
    auto reclaimMap = mHeap.GetReclaimMap();
    OffsetMapVec offsetMapVec(mInputDatas.size());
    for (size_t i = 0; i < mInputDatas.size(); ++i) {
        ConstructSegmentOffsetMap(mInputDatas[i].segMergeInfo, reclaimMap, mInputDatas[i].dataReader, offsetMapVec[i]);
        MergeOneSegmentData(mInputDatas[i].dataReader, offsetMapVec[i], reclaimMap, mInputDatas[i].segMergeInfo);
    }
    MergeDocOffsets(reclaimMap, offsetMapVec);
}

void VarLenDataMerger::ConstructSegmentOffsetMap(const SegmentMergeInfo& segMergeInfo, const ReclaimMapPtr& reclaimMap,
                                                 const VarLenDataReaderPtr& segReader,
                                                 SegmentOffsetMap& segmentOffsetMap)
{
    uint32_t docCount = segMergeInfo.segmentInfo.docCount;
    segmentOffsetMap.reserve(docCount);
    docid_t baseDocId = segMergeInfo.baseDocId;
    for (docid_t oldDocId = 0; oldDocId < (int64_t)docCount; oldDocId++) {
        docid_t globalDocId = baseDocId + oldDocId;
        auto newDocId = reclaimMap->GetNewId(globalDocId);
        if (newDocId == INVALID_DOCID || mPatchDocIdSet.count(globalDocId)) {
            continue;
        }
        auto newLocalInfo = reclaimMap->GetLocalId(newDocId);
        auto output = mSegOutputMapper.GetOutputBySegIdx(newLocalInfo.first);
        if (!output) {
            continue;
        }
        uint64_t oldOffset = segReader->GetOffset(oldDocId);
        segmentOffsetMap.push_back(OffsetPair(oldOffset, uint64_t(-1), oldDocId, newLocalInfo.first));
    }
    std::sort(segmentOffsetMap.begin(), segmentOffsetMap.end());
    segmentOffsetMap.assign(segmentOffsetMap.begin(), std::unique(segmentOffsetMap.begin(), segmentOffsetMap.end()));
}

void VarLenDataMerger::MergeOneSegmentData(const VarLenDataReaderPtr& reader, SegmentOffsetMap& segmentOffsetMap,
                                           const ReclaimMapPtr& reclaimMap, const SegmentMergeInfo& mergeSegInfo)
{
    docid_t oldBaseDocId = mergeSegInfo.baseDocId;
    typename SegmentOffsetMap::iterator it = segmentOffsetMap.begin();
    for (; it != segmentOffsetMap.end(); it++) {
        auto newDocId = reclaimMap->GetNewId(oldBaseDocId + it->oldDocId);
        auto output = mSegOutputMapper.GetOutput(newDocId);
        assert(output);
        autil::StringView data;
        if (!reader->GetValue(it->oldDocId, data, &mPool)) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "read data for doc [%d] in segment [%d] failed.", it->oldDocId,
                                 mergeSegInfo.segmentId);
        }
        it->newOffset = FindOrGenerateNewOffset(data, *output);
        ResetPoolBuffer();
    }
}

void VarLenDataMerger::MergeDocOffsets(const ReclaimMapPtr& reclaimMap, const OffsetMapVec& offsetMapVec)
{
    DocumentMergeInfo info;
    while (mHeap.GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        docid_t oldDocId = info.oldDocId;
        docid_t globalDocId = mInputDatas[segIdx].segMergeInfo.baseDocId + oldDocId;
        if (mPatchDocIdSet.count(globalDocId) > 0) {
            continue;
        }

        docid_t newDocId = info.newDocId;
        auto newLocalInfo = reclaimMap->GetLocalId(newDocId);
        auto output = mSegOutputMapper.GetOutputBySegIdx(newLocalInfo.first);
        if (!output) {
            continue;
        }

        const VarLenDataReaderPtr& reader = mInputDatas[segIdx].dataReader;
        uint64_t oldOffset = reader->GetOffset(oldDocId);
        const SegmentOffsetMap& offsetMap = offsetMapVec[segIdx];
        typename SegmentOffsetMap::const_iterator it = std::lower_bound(
            offsetMap.begin(), offsetMap.end(), OffsetPair(oldOffset, uint64_t(-1), INVALID_DOCID, newLocalInfo.first));
        assert(it != offsetMap.end() && *it == OffsetPair(oldOffset, uint64_t(-1), INVALID_DOCID, newLocalInfo.first));
        output->dataWriter->SetOffset(newLocalInfo.second, it->newOffset);
    }
}

void VarLenDataMerger::ResetPoolBuffer()
{
    if (mPool.getTotalBytes() > DEFAULT_POOL_BUFFER_SIZE) {
        mPool.release();
    } else {
        mPool.reset();
    }
}

uint64_t VarLenDataMerger::FindOrGenerateNewOffset(const StringView& value, OutputData& output)
{
    uint64_t hashValue = output.dataWriter->GetHashValue(value);
    return output.dataWriter->AppendValueWithoutOffset(value, hashValue);
}

void VarLenDataMerger::ReserveMergedOffsets()
{
    vector<char> patchValueBuffer;
    DocumentMergeInfoHeapPtr heap(mHeap.Clone());
    DocumentMergeInfo info;
    while (heap->GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        docid_t currentLocalId = info.oldDocId;
        auto output = mSegOutputMapper.GetOutput(info.newDocId);
        if (!output) {
            continue;
        }

        const AttributePatchReaderPtr& patchReader = mInputDatas[segIdx].segPatchReader;
        if (patchReader) {
            uint32_t maxPatchItemLen = patchReader->GetMaxPatchItemLen();
            if (patchValueBuffer.size() < maxPatchItemLen) {
                patchValueBuffer.resize(maxPatchItemLen);
            }

            uint32_t dataLen =
                patchReader->Seek(currentLocalId, (uint8_t*)patchValueBuffer.data(), patchValueBuffer.size());
            if (dataLen > 0) {
                autil::StringView value((const char*)patchValueBuffer.data(), dataLen);
                output->dataWriter->AppendValue(value);
                docid_t globalDocId = mInputDatas[segIdx].segMergeInfo.baseDocId + currentLocalId;
                mPatchDocIdSet.insert(globalDocId);
                continue;
            }
        }
        // delay merge data. Index: merge order, Value: [newDocId --> DocumentMergeInfo]
        output->dataWriter->AppendOffset(0);
    }
}

void VarLenDataMerger::CloseOutputDatas()
{
    for (size_t i = 0; i < mOutputDatas.size(); i++) {
        mOutputDatas[i].dataWriter->Close();
    }
}
}} // namespace indexlib::index
