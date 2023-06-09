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
#include "indexlib/index/merger_util/reclaim_map/reclaim_map_creator.h"

#include "indexlib/config/merge_config.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
using namespace std;
using namespace indexlib::index_base;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, ReclaimMapCreator);

ReclaimMapCreator::State::State(const SegmentSplitHandler& segmentSplitHandler, uint32_t maxDocCount)
    : mSegmentSplitHandler(segmentSplitHandler)
    , mDeletedDocCount(0)
    , mNewDocId(0)
{
    mDocIdArray.resize(maxDocCount, INVALID_DOCID);
    if (NeedSplitSegment()) {
        mSegMapper.Init(maxDocCount);
    }
}

ReclaimMapCreator::State::State(bool multiTargetSegment, uint32_t maxDocCount)
    : mSegmentSplitHandler(nullptr)
    , mDeletedDocCount(0)
    , mNewDocId(0)
{
    mDocIdArray.resize(maxDocCount, INVALID_DOCID);
    if (multiTargetSegment) {
        mSegMapper.Init(maxDocCount, false);
    }
}

ReclaimMapCreator::State::~State() {}

void ReclaimMapCreator::State::ReclaimOneDoc(segmentid_t segId, docid_t baseDocId, docid_t localId,
                                             const DeletionMapReaderPtr& deletionMapReader)
{
    auto oldGlobalId = baseDocId + localId;
    if (deletionMapReader->IsDeleted(segId, localId)) {
        ++mDeletedDocCount;
        return;
    }

    if (NeedSplitSegment()) {
        auto segIndex = mSegmentSplitHandler(segId, localId);
        auto currentSegDocCount = mSegMapper.GetSegmentDocCount(segIndex);
        mDocIdArray[oldGlobalId] = static_cast<docid_t>(currentSegDocCount);
        mSegMapper.Collect(oldGlobalId, segIndex);
    } else {
        mDocIdArray[oldGlobalId] = mNewDocId++;
    }
}

void ReclaimMapCreator::State::ConstructDocOrder(const index_base::SegmentMergeInfos& segMergeInfos,
                                                 ReclaimMapPtr& reclaimMap, const SegmentMapper& mapper,
                                                 const DeletionMapReaderPtr& deletionMapReader)
{
    GlobalIdVectorPtr reverseGlobalId;
    reverseGlobalId = reclaimMap->GetGlobalidVector();
    if (reverseGlobalId == nullptr) {
        reclaimMap->InitReverseDocIdArray(segMergeInfos);
        reverseGlobalId = reclaimMap->GetGlobalidVector();
    }
    uint32_t docCount = mDocIdArray.size();
    mDocIdOrder.reset(new DocOrder());
    mDocIdOrder->resize(docCount);
    vector<docid_t> expectOldDocIds, oldSegmentBaseId, expectNewDocIds, newDocBaseDocIds;
    expectOldDocIds.resize(segMergeInfos.size());
    oldSegmentBaseId.resize(segMergeInfos.size());
    expectNewDocIds.resize(mapper.GetMaxSegmentIndex() + 1);
    newDocBaseDocIds.resize(mapper.GetMaxSegmentIndex() + 1);
    int beforeDocCount = 0;
    expectNewDocIds[0] = 0;
    map<segmentid_t, int64_t> oldSegIdToIndex;
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        oldSegmentBaseId[i] = segMergeInfos[i].baseDocId;
        expectOldDocIds[i] = 0;
        oldSegIdToIndex.insert(make_pair(segMergeInfos[i].segmentId, i));
    }
    for (size_t i = 1; i < expectNewDocIds.size(); i++) {
        expectNewDocIds[i] = mapper.GetSegmentDocCount(i - 1) + beforeDocCount;
        newDocBaseDocIds[i] = expectNewDocIds[i];
        beforeDocCount = expectNewDocIds[i];
    }
    int32_t orderTime = 0;
    while (true) {
        for (size_t i = 0; i < expectNewDocIds.size(); i++) {
            while (i < expectNewDocIds.size() &&
                   expectNewDocIds[i] == newDocBaseDocIds[i] + mapper.GetSegmentDocCount(i)) {
                i++;
            }
            docid_t expectNewDocId = expectNewDocIds[i];
            GlobalId gid = (*reverseGlobalId)[expectNewDocId];
            segmentid_t oldSegId = gid.first;
            int64_t idx = oldSegIdToIndex[oldSegId];
            if (expectOldDocIds[idx] == gid.second) {
                auto oldDocId = oldSegmentBaseId[idx] + gid.second;
                (*mDocIdOrder)[oldDocId] = orderTime;
                orderTime++;
                expectOldDocIds[idx]++;
                expectNewDocIds[i]++;
                break;
            } else {
                assert(expectOldDocIds[idx] < gid.second);
                for (docid_t temp = expectOldDocIds[idx]; temp < gid.second; temp++) {
                    if (deletionMapReader->IsDeleted(oldSegId, temp)) {
                        expectOldDocIds[idx]++;
                    } else {
                        break;
                    }
                }
                if (expectOldDocIds[idx] == gid.second) {
                    auto oldDocId = oldSegmentBaseId[idx] + gid.second;
                    (*mDocIdOrder)[oldDocId] = orderTime;
                    orderTime++;
                    expectOldDocIds[idx]++;
                    expectNewDocIds[i]++;
                    break;
                }
            }
        }
        bool allDone = true;
        for (size_t i = 0; i < expectNewDocIds.size(); i++) {
            if (expectNewDocIds[i] != newDocBaseDocIds[i] + mapper.GetSegmentDocCount(i)) {
                allDone = false;
                break;
            }
        }
        if (allDone) {
            break;
        }
    }
    reclaimMap->SetDocIdOrder(mDocIdOrder);
}

ReclaimMapPtr ReclaimMapCreator::State::ConstructReclaimMap(const index_base::SegmentMergeInfos& segMergeInfos,
                                                            bool needReserveMapping,
                                                            const DeletionMapReaderPtr& deletionMapReader, bool isSort)
{
    if (NeedSplitSegment()) {
        MakeTargetBaseDocIds(mSegMapper, mSegMapper.GetMaxSegmentIndex());
        RewriteDocIdArray(segMergeInfos, mSegMapper);
    }
    ReclaimMapPtr reclaimMap(new ReclaimMap(mDeletedDocCount, mNewDocId, std::move(mDocIdArray), std::vector<docid_t>(),
                                            std::move(mTargetBaseDocIds)));
    if (!reclaimMap) {
        INDEXLIB_FATAL_ERROR(OutOfMemory, "Allocate memory FAILED.");
    }
    if (needReserveMapping) {
        reclaimMap->InitReverseDocIdArray(segMergeInfos);
    }
    if (NeedSplitSegment() && isSort && reclaimMap->GetTargetSegmentCount() > 1) {
        ConstructDocOrder(segMergeInfos, reclaimMap, mSegMapper, deletionMapReader);
    }
    mSegMapper.Clear();
    return reclaimMap;
}

void ReclaimMapCreator::State::MakeTargetBaseDocIds(const SegmentMapper& segMapper, segmentindex_t maxSegIdx)
{
    size_t currentDocs = 0;
    mTargetBaseDocIds.clear();
    mTargetBaseDocIds.reserve(maxSegIdx + 1);
    for (segmentindex_t segIdx = 0; segIdx <= maxSegIdx; ++segIdx) {
        mTargetBaseDocIds.push_back(currentDocs);
        currentDocs += segMapper.GetSegmentDocCount(segIdx);
    }
}

void ReclaimMapCreator::State::RewriteDocIdArray(const SegmentMergeInfos& segMergeInfos, const SegmentMapper& segMapper)
{
    mNewDocId = 0;
    for (const auto& segMergeInfo : segMergeInfos) {
        for (size_t localId = 0; localId < segMergeInfo.segmentInfo.docCount; ++localId) {
            auto oldId = segMergeInfo.baseDocId + localId;
            if (mDocIdArray[oldId] == INVALID_DOCID) {
                continue;
            }
            auto segIdx = segMapper.GetSegmentIndex(oldId);
            mDocIdArray[oldId] += mTargetBaseDocIds[segIdx];
            ++mNewDocId;
        }
    }
}

ReclaimMapCreator::ReclaimMapCreator(bool hasTruncate) : mHasTruncate(hasTruncate) {}

ReclaimMapCreator::~ReclaimMapCreator() {}

ReclaimMapPtr ReclaimMapCreator::Create(const index_base::SegmentMergeInfos& segMergeInfos,
                                        const DeletionMapReaderPtr& delMapReader,
                                        const SegmentSplitHandler& segmentSplitHandler) const
{
    int64_t before = autil::TimeUtility::currentTimeInSeconds();
    uint32_t maxDocCount = segMergeInfos.rbegin()->baseDocId + segMergeInfos.rbegin()->segmentInfo.docCount;
    ReclaimMapCreator::State state(segmentSplitHandler, maxDocCount);
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
        docid_t baseDocId = segMergeInfo.baseDocId;
        uint32_t segDocCount = segMergeInfos[i].segmentInfo.docCount;
        uint32_t segId = segMergeInfos[i].segmentId;
        for (uint32_t j = 0; j < segDocCount; ++j) {
            state.ReclaimOneDoc(segId, baseDocId, j, delMapReader);
        }
    }
    ReclaimMapPtr reclaimMap = state.ConstructReclaimMap(segMergeInfos, NeedReverseMapping(), nullptr, false);
    int64_t after = autil::TimeUtility::currentTimeInSeconds();
    IE_LOG(INFO, "Init reclaim map use %ld seconds", (after - before));
    return reclaimMap;
}
}} // namespace indexlib::index
