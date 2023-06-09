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
#include "indexlib/index/partition_info.h"

#include "indexlib/common/term_hint_parser.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/temperature_doc_info.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"

using namespace std;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PartitionInfo);

PartitionInfo::PartitionInfo(const PartitionInfo& other)
    : mBaseDocIds(other.mBaseDocIds)
    , mSegIdToBaseDocId(other.mSegIdToBaseDocId)
    , mVersion(other.mVersion)
    , mPartInfoHint(other.mPartInfoHint)
    , mOrderRanges(other.mOrderRanges)
    , mUnorderRange(other.mUnorderRange)
    , mPartMeta(other.mPartMeta)
    , mDeletionMapReader(other.mDeletionMapReader)
    , mPartitionMetrics(other.mPartitionMetrics)
    , mTemperatureDocRange(other.mTemperatureDocRange)
{
    if (other.mSubPartitionInfo) {
        mSubPartitionInfo.reset(other.mSubPartitionInfo->Clone());
    }
}

void PartitionInfo::Init(const Version& version, const PartitionMetaPtr& partitionMeta,
                         const SegmentDataVector& segmentDatas, const std::vector<InMemorySegmentPtr>& dumpingSegments,
                         const DeletionMapReaderPtr& deletionMapReader)
{
    // kv/kkv table does not have deletionMapReader
    assert(version.GetSegmentCount() == segmentDatas.size());

    mVersion = version;
    mPartMeta = partitionMeta;
    mDeletionMapReader = deletionMapReader;
    InitVersion(dumpingSegments);
    InitBaseDocIds(segmentDatas, dumpingSegments);
    InitPartitionMetrics(segmentDatas, dumpingSegments);

    InitOrderedDocIdRanges(segmentDatas);
    InitUnorderedDocIdRange();
    InitPartitionInfoHint(segmentDatas, dumpingSegments);
    InitTemperatureMetaInfo(version, segmentDatas, dumpingSegments);
}

void PartitionInfo::InitTemperatureMetaInfo(const Version& version, const SegmentDataVector& segmentDatas,
                                            const std::vector<InMemorySegmentPtr>& dumpingSegments)
{
    if (IsKeyValueTable()) {
        return;
    }
    mTemperatureDocRange.reset(new TemperatureDocInfo());
    mTemperatureDocRange->Init(version, segmentDatas, dumpingSegments, mPartitionMetrics.totalDocCount);
}

bool PartitionInfo::GetTemperatureDocIdRanges(int32_t hintValues, DocIdRangeVector& ranges) const
{
    if (IsKeyValueTable()) {
        return false;
    }
    if (!mTemperatureDocRange) {
        return false;
    }
    return mTemperatureDocRange->GetTemperatureDocIdRanges(hintValues, ranges);
}

globalid_t PartitionInfo::GetGlobalId(docid_t docId) const
{
    if (IsKeyValueTable()) {
        return INVALID_GLOBALID;
    }
    if (mBaseDocIds.size() == 0 || docId >= (docid_t)mPartitionMetrics.totalDocCount) {
        return INVALID_GLOBALID;
    }

    globalid_t gid = INVALID_GLOBALID;
    for (int32_t i = (int32_t)mBaseDocIds.size() - 1; i >= 0; --i) {
        if (docId >= mBaseDocIds[i]) {
            docid_t localDocId = docId - mBaseDocIds[i];
            segmentid_t segId = mVersion[i];
            if (mDeletionMapReader->IsDeleted(docId)) {
                return INVALID_GLOBALID;
            }
            gid = localDocId;
            gid |= ((globalid_t)segId << 32);
            break;
        }
    }
    return gid;
}

docid_t PartitionInfo::GetSegmentDocCount(size_t idx) const
{
    if (IsKeyValueTable()) {
        return INVALID_DOCID;
    }
    if (idx == mBaseDocIds.size() - 1) {
        return mPartitionMetrics.totalDocCount - mBaseDocIds[idx];
    }
    return mBaseDocIds[idx + 1] - mBaseDocIds[idx];
}

docid_t PartitionInfo::GetDocId(globalid_t gid) const
{
    if (IsKeyValueTable()) {
        return INVALID_DOCID;
    }
    docid_t docId = INVALID_DOCID;
    int32_t segId = gid >> 32;
    for (size_t i = 0; i < mBaseDocIds.size(); ++i) {
        if (mVersion[i] == segId) {
            docid_t localDocId = gid & 0xFFFFFFFF;
            if (localDocId >= GetSegmentDocCount(i)) {
                return INVALID_GLOBALID;
            }
            docId = localDocId + mBaseDocIds[i];
            if (mDeletionMapReader->IsDeleted(docId)) {
                return INVALID_DOCID;
            }
            break;
        }
    }
    return docId;
}

segmentid_t PartitionInfo::GetSegmentId(docid_t docId) const
{
    if (IsKeyValueTable()) {
        return INVALID_DOCID;
    }
    if (docId == INVALID_DOCID || static_cast<size_t>(docId) >= GetTotalDocCount()) {
        return INVALID_SEGMENTID;
    }
    size_t pos = upper_bound(mBaseDocIds.begin(), mBaseDocIds.end(), docId) - mBaseDocIds.begin();
    if (pos == 0) {
        return INVALID_SEGMENTID;
    }
    return mVersion[pos - 1];
}

pair<segmentid_t, docid_t> PartitionInfo::GetLocalDocInfo(docid_t docId) const
{
    if (docId == INVALID_DOCID || docId >= static_cast<int64_t>(GetTotalDocCount())) {
        return make_pair(INVALID_SEGMENTID, INVALID_DOCID);
    }
    size_t pos = upper_bound(mBaseDocIds.begin(), mBaseDocIds.end(), docId) - mBaseDocIds.begin();
    if (pos == 0) {
        return make_pair(INVALID_SEGMENTID, INVALID_DOCID);
    }
    size_t idx = pos - 1;
    segmentid_t segId = mVersion[idx];
    return make_pair(segId, docId - mBaseDocIds[idx]);
}

bool PartitionInfo::GetDiffDocIdRanges(const PartitionInfoHint& infoHint, DocIdRangeVector& docIdRanges) const
{
    if (IsKeyValueTable()) {
        return false;
    }
    if (infoHint.lastIncSegmentId == INVALID_SEGMENTID && infoHint.lastRtSegmentId == INVALID_SEGMENTID) {
        return false;
    }

    docIdRanges.clear();

    DocIdRange range;
    if (GetIncDiffDocIdRange(infoHint.lastIncSegmentId, range)) {
        docIdRanges.push_back(range);
    }

    if (GetRtDiffDocIdRange(infoHint.lastRtSegmentId, infoHint.lastRtSegmentDocCount, range)) {
        docIdRanges.push_back(range);
    }

    return !docIdRanges.empty();
}

void PartitionInfo::InitVersion(const std::vector<InMemorySegmentPtr>& dumpingSegments)
{
    for (const InMemorySegmentPtr& inMemSegment : dumpingSegments) {
        mVersion.AddSegment(inMemSegment->GetSegmentId());
    }
}

void PartitionInfo::InitBaseDocIds(const SegmentDataVector& segmentDatas,
                                   const std::vector<InMemorySegmentPtr>& dumpingSegments)
{
    if (IsKeyValueTable()) {
        return;
    }
    uint32_t totalDocCount = 0;
    for (size_t i = 0; i < segmentDatas.size(); i++) {
        const SegmentData& segData = segmentDatas[i];
        mBaseDocIds.push_back(segData.GetBaseDocId());
        mSegIdToBaseDocId.insert(make_pair(segData.GetSegmentId(), segData.GetBaseDocId()));
        totalDocCount += segData.GetSegmentInfo()->docCount;
    }
    for (const InMemorySegmentPtr& inMemSegment : dumpingSegments) {
        mBaseDocIds.push_back(totalDocCount);
        mSegIdToBaseDocId.insert(make_pair(inMemSegment->GetSegmentId(), totalDocCount));
        totalDocCount += inMemSegment->GetSegmentInfo()->docCount;
    }
    // // for realtime building segment
    // if (segmentDatas.size() > 0)
    // {
    //     const SegmentData& lastSegData = segmentDatas.back();
    //     mBaseDocIds.push_back(lastSegData.GetBaseDocId() +
    //                           lastSegData.GetSegmentInfo()->docCount);
    // }
    // else
    // {
    //     mBaseDocIds.push_back(0);
    // }
}

docid_t PartitionInfo::GetBaseDocId(segmentid_t segId) const
{
    if (IsKeyValueTable()) {
        return INVALID_DOCID;
    }
    const auto it = mSegIdToBaseDocId.find(segId);
    if (it == mSegIdToBaseDocId.end()) {
        return INVALID_DOCID;
    }
    return it->second;
}

void PartitionInfo::InitPartitionMetrics(const SegmentDataVector& segmentDatas,
                                         const std::vector<InMemorySegmentPtr>& dumpingSegments)
{
    mPartitionMetrics.segmentCount = segmentDatas.size() + dumpingSegments.size();
    mPartitionMetrics.delDocCount = mDeletionMapReader ? mDeletionMapReader->GetDeletedDocCount() : 0;

    size_t totalDocCount = 0;
    size_t incDocCount = 0;
    for (size_t i = 0; i < segmentDatas.size(); ++i) {
        const SegmentData& segData = segmentDatas[i];
        totalDocCount += segData.GetSegmentInfo()->docCount;
        if (OnlineSegmentDirectory::IsIncSegmentId(segData.GetSegmentId())) {
            incDocCount += segData.GetSegmentInfo()->docCount;
        }
    }
    for (const InMemorySegmentPtr& inMemSegment : dumpingSegments) {
        totalDocCount += inMemSegment->GetSegmentInfo()->docCount;
    }

    mPartitionMetrics.totalDocCount = totalDocCount;
    mPartitionMetrics.incDocCount = incDocCount;
}

void PartitionInfo::InitOrderedDocIdRanges(const SegmentDataVector& segmentDatas)
{
    if (IsKeyValueTable()) {
        return;
    }
    mOrderRanges.clear();
    if (mPartMeta->Size() == 0) {
        return;
    }
    for (size_t i = 0; i < segmentDatas.size(); i++) {
        const SegmentData& segData = segmentDatas[i];
        if (!OnlineSegmentDirectory::IsIncSegmentId(segData.GetSegmentId())) {
            break;
        }
        docid_t begin = segData.GetBaseDocId();
        docid_t end = begin + segData.GetSegmentInfo()->docCount;
        mOrderRanges.push_back(DocIdRange(begin, end));
    }
}

bool PartitionInfo::GetOrderedDocIdRanges(DocIdRangeVector& ranges) const
{
    if (IsKeyValueTable()) {
        return false;
    }
    if (mOrderRanges.empty()) {
        return false;
    }

    ranges = mOrderRanges;
    return true;
}

bool PartitionInfo::GetUnorderedDocIdRange(DocIdRange& range) const
{
    if (IsKeyValueTable()) {
        return false;
    }
    if (mUnorderRange.first >= mUnorderRange.second) {
        return false;
    }

    range = mUnorderRange;
    return true;
}

void PartitionInfo::InitUnorderedDocIdRange()
{
    if (IsKeyValueTable()) {
        return;
    }
    if (mOrderRanges.size() == 0) {
        mUnorderRange.first = (docid_t)0;
        mUnorderRange.second = (docid_t)mPartitionMetrics.totalDocCount;
    } else {
        mUnorderRange.first = mOrderRanges[mOrderRanges.size() - 1].second;
        mUnorderRange.second = (docid_t)mPartitionMetrics.totalDocCount;
    }
}

void PartitionInfo::InitPartitionInfoHint(const SegmentDataVector& segmentDatas,
                                          const std::vector<InMemorySegmentPtr>& dumpingSegments)
{
    mPartInfoHint.lastRtSegmentId = INVALID_SEGMENTID;
    if (dumpingSegments.empty()) {
        for (int32_t i = segmentDatas.size() - 1; i >= 0; i--) {
            segmentid_t segId = segmentDatas[i].GetSegmentId();
            if (RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
                mPartInfoHint.lastRtSegmentId = segId;
                mPartInfoHint.lastRtSegmentDocCount = segmentDatas[i].GetSegmentInfo()->docCount;
                break;
            }
        }
    } else {
        mPartInfoHint.lastRtSegmentId = dumpingSegments.back()->GetSegmentId();
        mPartInfoHint.lastRtSegmentDocCount = dumpingSegments.back()->GetSegmentInfo()->docCount;
    }

    mPartInfoHint.lastIncSegmentId = INVALID_SEGMENTID;
    for (int32_t i = segmentDatas.size() - 1; i >= 0; i--) {
        segmentid_t segId = segmentDatas[i].GetSegmentId();
        if (OnlineSegmentDirectory::IsIncSegmentId(segId)) {
            mPartInfoHint.lastIncSegmentId = segId;
            break;
        }
    }
}

void PartitionInfo::AddInMemorySegment(const InMemorySegmentPtr& inMemSegment)
{
    assert(inMemSegment);
    if (!IsKeyValueTable()) {
        mBaseDocIds.push_back(mPartitionMetrics.totalDocCount);
    }
    segmentid_t segId = inMemSegment->GetSegmentId();
    mPartitionMetrics.segmentCount++;
    mPartInfoHint.lastRtSegmentId = segId;
    mPartInfoHint.lastRtSegmentDocCount = inMemSegment->GetSegmentInfo()->docCount;
    mVersion.AddSegment(segId);
    mPartitionMetrics.totalDocCount += inMemSegment->GetSegmentInfo()->docCount;
    if (!IsKeyValueTable()) {
        mUnorderRange.second = mPartitionMetrics.totalDocCount;
    }

    if (!IsKeyValueTable() && mTemperatureDocRange && !mTemperatureDocRange->IsEmptyInfo()) {
        mTemperatureDocRange->AddNewSegmentInfo(TemperatureProperty::HOT, segId, mPartitionMetrics.totalDocCount,
                                                MAX_DOCID);
    }

    if (mSubPartitionInfo) {
        const InMemorySegmentPtr& subInMemSegment = inMemSegment->GetSubInMemorySegment();
        mSubPartitionInfo->AddInMemorySegment(subInMemSegment);
    }
}

bool PartitionInfo::NeedUpdate(const index_base::InMemorySegmentPtr& inMemSegment) const
{
    assert(inMemSegment);
    assert(mPartInfoHint.lastRtSegmentId == inMemSegment->GetSegmentId());
    uint32_t lastRtSegmentDocCount = inMemSegment->GetSegmentInfo()->docCount;
    // assume that if main partition doc count is not changed,
    // sub partition doc count is not changed too.
    return (lastRtSegmentDocCount > mPartInfoHint.lastRtSegmentDocCount);
}

void PartitionInfo::UpdateInMemorySegment(const InMemorySegmentPtr& inMemSegment)
{
    assert(inMemSegment);
    assert(mPartInfoHint.lastRtSegmentId == inMemSegment->GetSegmentId());

    if (mDeletionMapReader) {
        mPartitionMetrics.delDocCount = mDeletionMapReader->GetDeletedDocCount();
    }
    const SegmentInfoPtr& segInfo = inMemSegment->GetSegmentInfo();
    size_t lastRtSegmentDocCount = segInfo->docCount;
    if (lastRtSegmentDocCount > mPartInfoHint.lastRtSegmentDocCount) {
        size_t increaseDocCount = lastRtSegmentDocCount - mPartInfoHint.lastRtSegmentDocCount;
        mPartInfoHint.lastRtSegmentDocCount = lastRtSegmentDocCount;
        mPartitionMetrics.totalDocCount += increaseDocCount;
        if (!IsKeyValueTable()) {
            mUnorderRange.second = mPartitionMetrics.totalDocCount;
        }
    }

    if (mSubPartitionInfo) {
        const InMemorySegmentPtr& subInMemSegment = inMemSegment->GetSubInMemorySegment();
        mSubPartitionInfo->UpdateInMemorySegment(subInMemSegment);
    }
}

PartitionInfo* PartitionInfo::Clone() { return new PartitionInfo(*this); }

bool PartitionInfo::GetIncDiffDocIdRange(segmentid_t lastIncSegId, DocIdRange& range) const
{
    if (IsKeyValueTable()) {
        return false;
    }
    size_t segmentCount = mVersion.GetSegmentCount();
    for (size_t i = 0; i < segmentCount; ++i) {
        if (!OnlineSegmentDirectory::IsIncSegmentId(mVersion[i]) || mVersion[i] <= lastIncSegId) {
            continue;
        }
        if (static_cast<size_t>(mBaseDocIds[i]) >= mPartitionMetrics.incDocCount) {
            return false;
        }
        range = DocIdRange(mBaseDocIds[i], mPartitionMetrics.incDocCount);
        return true;
    }
    return false;
}

bool PartitionInfo::GetRtDiffDocIdRange(segmentid_t lastRtSegmentId, size_t lastRtSegmentDocCount,
                                        DocIdRange& range) const
{
    if (IsKeyValueTable()) {
        return false;
    }
    if (lastRtSegmentId == mPartInfoHint.lastRtSegmentId &&
        lastRtSegmentDocCount == mPartInfoHint.lastRtSegmentDocCount) {
        return false;
    }
    size_t totalDocCount = mPartitionMetrics.totalDocCount;
    size_t segmentCount = mVersion.GetSegmentCount();
    for (size_t i = 0; i < segmentCount; ++i) {
        if (mVersion[i] == lastRtSegmentId) {
            range = DocIdRange(mBaseDocIds[i] + lastRtSegmentDocCount, totalDocCount);
            return range.first < range.second;
        }
    }

    for (size_t i = 0; i < segmentCount; ++i) {
        if (mVersion[i] > lastRtSegmentId && RealtimeSegmentDirectory::IsRtSegmentId(mVersion[i])) {
            range = DocIdRange(mBaseDocIds[i], totalDocCount);
            return range.first < range.second;
        }
    }
    return false;
}

void PartitionInfo::TEST_AssertEqual(const PartitionInfo& other)
{
    assert(mBaseDocIds == other.mBaseDocIds);
    assert(mSegIdToBaseDocId == other.mSegIdToBaseDocId);
    assert(mVersion == other.mVersion);
    assert(mPartInfoHint == other.mPartInfoHint);
    assert(mOrderRanges == other.mOrderRanges);
    assert(mUnorderRange == other.mUnorderRange);
    mPartMeta->AssertEqual(*other.mPartMeta.get());
    assert(mDeletionMapReader->GetDeletedDocCount() == other.mDeletionMapReader->GetDeletedDocCount());
    assert(mPartitionMetrics == other.mPartitionMetrics);
    if (mSubPartitionInfo != nullptr && other.mSubPartitionInfo != nullptr) {
        mSubPartitionInfo->TEST_AssertEqual(*other.mSubPartitionInfo.get());
    } else {
        assert(mSubPartitionInfo == nullptr && other.mSubPartitionInfo == nullptr);
    }
}
}} // namespace indexlib::index
