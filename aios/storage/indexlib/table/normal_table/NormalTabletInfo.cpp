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
#include "indexlib/table/normal_table/NormalTabletInfo.h"

#include "indexlib/framework/TabletData.h"
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"
#include "indexlib/table/normal_table/NormalTabletMeta.h"
#include "indexlib/table/normal_table/SegmentSortDecisionMaker.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletInfo);

NormalTabletInfo::NormalTabletInfo(const NormalTabletInfo& other)
    : _tabletMeta(other._tabletMeta)
    , _deletionMapReader(other._deletionMapReader)
    , _segmentIds(other._segmentIds)
    , _baseDocIds(other._baseDocIds)
    , _segmentIdSet(other._segmentIdSet)
    , _segmentCount(other._segmentCount)
    , _delDocCount(other._delDocCount)
    , _totalDocCount(other._totalDocCount)
    , _incDocCount(other._incDocCount)
    , _orderRanges(other._orderRanges)
    , _unorderRange(other._unorderRange)
    , _tabletInfoHint(other._tabletInfoHint)
{
}

Status NormalTabletInfo::Init(const std::shared_ptr<framework::TabletData>& tabletData,
                              const std::shared_ptr<NormalTabletMeta>& tabletMeta,
                              const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader)
{
    _tabletMeta = tabletMeta;
    _deletionMapReader = deletionMapReader;
    InitSegmentIds(tabletData);
    InitBaseDocIds(tabletData);
    InitDocCount(tabletData);
    InitOrderedDocIdRanges(tabletData);
    InitUnorderedDocIdRange();
    InitTabletInfoHint(tabletData);
    return Status::OK();
}

void NormalTabletInfo::InitSegmentIds(const std::shared_ptr<framework::TabletData>& tabletData)
{
    const auto& version = tabletData->GetOnDiskVersion();
    _segmentIds.reserve(tabletData->GetSegmentCount());
    for (const auto& [segId, _] : version) {
        _segmentIds.push_back(segId);
        _segmentIdSet.insert(segId);
    }
    auto segments = tabletData->CreateSlice();
    for (const auto& segment : segments) {
        auto segStatus = segment->GetSegmentStatus();
        if (segStatus == framework::Segment::SegmentStatus::ST_DUMPING ||
            segStatus == framework::Segment::SegmentStatus::ST_BUILDING) {
            _segmentIds.push_back(segment->GetSegmentId());
            _segmentIdSet.insert(segment->GetSegmentId());
        }
    }
}

void NormalTabletInfo::InitBaseDocIds(const std::shared_ptr<framework::TabletData>& tabletData)
{
    auto segments = tabletData->CreateSlice();
    docid_t baseDocId = 0;
    for (const auto& segment : segments) {
        _baseDocIds.push_back(baseDocId);
        baseDocId += segment->GetSegmentInfo()->docCount;
    }
}

bool NormalTabletInfo::HasSegment(segmentid_t segmentId) const
{
    return _segmentIdSet.find(segmentId) != _segmentIdSet.end();
}

std::unique_ptr<NormalTabletInfo> NormalTabletInfo::Clone() const { return std::make_unique<NormalTabletInfo>(*this); }

void NormalTabletInfo::InitDocCount(const std::shared_ptr<framework::TabletData>& tabletData)
{
    auto segments = tabletData->CreateSlice();
    size_t totalDocCount = 0;
    size_t incDocCount = 0;
    for (const auto& segment : segments) {
        auto segStatus = segment->GetSegmentStatus();
        _segmentCount++;
        totalDocCount += segment->GetSegmentInfo()->docCount;
        if (segStatus == framework::Segment::SegmentStatus::ST_BUILT) {
            if (HasSegment(segment->GetSegmentId())) {
                incDocCount += segment->GetSegmentInfo()->docCount;
            }
        }
        _delDocCount = _deletionMapReader ? _deletionMapReader->GetDeletedDocCount() : 0;
    }
    _totalDocCount = totalDocCount;
    _incDocCount = incDocCount;
}

void NormalTabletInfo::InitOrderedDocIdRanges(const std::shared_ptr<framework::TabletData>& tabletData)
{
    _orderRanges.clear();
    if (_tabletMeta->Size() == 0) {
        return;
    }
    docid_t baseDocId = 0;
    auto segments = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);
    for (const auto& segment : segments) {
        if (!HasSegment(segment->GetSegmentId())) {
            break;
        }
        if (!SegmentSortDecisionMaker::IsSortedDiskSegment(_tabletMeta->GetSortDescriptions(),
                                                           segment->GetSegmentId())) {
            break;
        }
        docid_t begin = baseDocId;
        docid_t end = begin + segment->GetSegmentInfo()->docCount;
        _orderRanges.push_back(DocIdRange(begin, end));
        baseDocId += segment->GetSegmentInfo()->docCount;
    }
}

bool NormalTabletInfo::GetOrderedDocIdRanges(DocIdRangeVector& ranges) const
{
    if (_orderRanges.empty()) {
        return false;
    }
    ranges = _orderRanges;
    return true;
}

void NormalTabletInfo::InitUnorderedDocIdRange()
{
    if (_orderRanges.size() == 0) {
        _unorderRange.first = (docid_t)0;
        _unorderRange.second = (docid_t)_totalDocCount;
    } else {
        _unorderRange.first = _orderRanges[_orderRanges.size() - 1].second;
        _unorderRange.second = (docid_t)_totalDocCount;
    }
}

bool NormalTabletInfo::GetUnorderedDocIdRange(DocIdRange& range) const
{
    if (_unorderRange.first >= _unorderRange.second) {
        return false;
    }
    range = _unorderRange;
    return true;
}

void NormalTabletInfo::InitTabletInfoHint(const std::shared_ptr<framework::TabletData>& tabletData)
{
    _tabletInfoHint.lastRtSegmentId = INVALID_SEGMENTID;
    auto segments = tabletData->CreateSlice();

    for (auto iter = segments.rbegin(); iter != segments.rend(); ++iter) {
        auto& segment = *iter;
        segmentid_t segId = segment->GetSegmentId();
        auto segStatus = segment->GetSegmentStatus();
        if (segStatus == framework::Segment::SegmentStatus::ST_DUMPING ||
            segStatus == framework::Segment::SegmentStatus::ST_BUILDING) {
            _tabletInfoHint.lastRtSegmentId = segId;
            _tabletInfoHint.lastRtSegmentDocCount = segment->GetSegmentInfo()->docCount;
            break;
        }
    }

    auto dumpingSegments = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_DUMPING);
    if (!dumpingSegments.empty()) {
        _tabletInfoHint.needRefindRtSegmentId = (*(dumpingSegments.begin()))->GetSegmentId();
    } else {
        auto buildingSegments = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILDING);
        _tabletInfoHint.needRefindRtSegmentId =
            buildingSegments.empty() ? INVALID_SEGMENTID : (*(buildingSegments.begin()))->GetSegmentId();
    }

    assert(_tabletInfoHint.needRefindRtSegmentId <= _tabletInfoHint.lastRtSegmentId);

    _tabletInfoHint.lastIncSegmentId = INVALID_SEGMENTID;
    auto builtSegments = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);
    for (auto iter = builtSegments.rbegin(); iter != builtSegments.rend(); ++iter) {
        auto& segment = *iter;
        if (HasSegment(segment->GetSegmentId())) {
            _tabletInfoHint.lastIncSegmentId = segment->GetSegmentId();
            break;
        }
    }
}

globalid_t NormalTabletInfo::GetGlobalId(docid_t docId) const
{
    if (_baseDocIds.size() == 0 || docId >= (docid_t)_totalDocCount) {
        return INVALID_GLOBALID;
    }

    globalid_t gid = INVALID_GLOBALID;
    for (int32_t i = (int32_t)_baseDocIds.size() - 1; i >= 0; --i) {
        if (docId >= _baseDocIds[i]) {
            docid_t localDocId = docId - _baseDocIds[i];
            segmentid_t segId = _segmentIds[i];
            if (_deletionMapReader->IsDeleted(docId)) {
                return INVALID_GLOBALID;
            }
            gid = localDocId;
            gid |= ((globalid_t)segId << 32);
            break;
        }
    }
    return gid;
}

docid_t NormalTabletInfo::GetSegmentDocCount(size_t idx) const
{
    if (idx == _baseDocIds.size() - 1) {
        return _totalDocCount - _baseDocIds[idx];
    }
    return _baseDocIds[idx + 1] - _baseDocIds[idx];
}

docid_t NormalTabletInfo::GetDocId(const TabletInfoHint& infoHint, globalid_t gid) const
{
    docid_t docId = INVALID_DOCID;
    int32_t segId = gid >> 32;
    if (segId >= infoHint.needRefindRtSegmentId) {
        return INVALID_DOCID;
    }
    for (size_t i = 0; i < _baseDocIds.size(); ++i) {
        if (_segmentIds[i] == segId) {
            docid_t localDocId = gid & 0xFFFFFFFF;
            if (localDocId >= GetSegmentDocCount(i)) {
                return INVALID_GLOBALID;
            }
            docId = localDocId + _baseDocIds[i];
            if (_deletionMapReader->IsDeleted(docId)) {
                return INVALID_DOCID;
            }
            break;
        }
    }
    return docId;
}

bool NormalTabletInfo::GetDiffDocIdRanges(const TabletInfoHint& infoHint, DocIdRangeVector& docIdRanges) const
{
    if (infoHint.lastIncSegmentId == INVALID_SEGMENTID && infoHint.lastRtSegmentId == INVALID_SEGMENTID) {
        return false;
    }

    docIdRanges.clear();

    DocIdRange range;
    if (GetIncDiffDocIdRange(infoHint.lastIncSegmentId, range)) {
        docIdRanges.push_back(range);
    }

    if (GetRtDiffDocIdRange(infoHint.needRefindRtSegmentId, range)) {
        docIdRanges.push_back(range);
    }

    return !docIdRanges.empty();
}

bool NormalTabletInfo::GetIncDiffDocIdRange(segmentid_t lastIncSegId, DocIdRange& range) const
{
    size_t segmentCount = _segmentIds.size();
    for (size_t i = 0; i < segmentCount; ++i) {
        // TODO(yonghao.fyh): confirm check is inc segment condition
        if (!HasSegment(_segmentIds[i]) || _segmentIds[i] <= lastIncSegId) {
            continue;
        }
        if (static_cast<size_t>(_baseDocIds[i]) >= _incDocCount) {
            return false;
        }
        range = DocIdRange(_baseDocIds[i], _incDocCount);
        return true;
    }
    return false;
}

bool NormalTabletInfo::GetRtDiffDocIdRange(segmentid_t needRefindSegmentId, DocIdRange& range) const
{
    size_t totalDocCount = _totalDocCount;
    size_t segmentCount = _segmentIds.size();
    for (size_t i = 0; i < segmentCount; ++i) {
        if (_segmentIds[i] >= needRefindSegmentId) {
            range = DocIdRange(_baseDocIds[i], totalDocCount);
            return range.first < range.second;
        }
    }
    return false;
}

bool NormalTabletInfo::NeedUpdate(segmentid_t lastRtSegId, size_t lastRtSegDocCount) const
{
    assert(_tabletInfoHint.lastRtSegmentId == lastRtSegId);
    return (lastRtSegDocCount > _tabletInfoHint.lastRtSegmentDocCount);
}

bool NormalTabletInfo::NeedUpdate(segmentid_t lastRtSegId, size_t lastRtSegDocCount, segmentid_t refindRtSegment) const
{
    return NeedUpdate(lastRtSegId, lastRtSegDocCount) || (refindRtSegment != _tabletInfoHint.needRefindRtSegmentId);
}

void NormalTabletInfo::UpdateDocCount(segmentid_t lastRtSegId, size_t lastRtSegDocCount, segmentid_t refindRtSegId)
{
    assert(_tabletInfoHint.lastRtSegmentId == lastRtSegId);
    if (NeedUpdate(lastRtSegId, lastRtSegDocCount)) {
        size_t increaseDocCount = lastRtSegDocCount - _tabletInfoHint.lastRtSegmentDocCount;
        _tabletInfoHint.lastRtSegmentDocCount = lastRtSegDocCount;
        _totalDocCount += increaseDocCount;
        _unorderRange.second = _totalDocCount;
    }
    if (_deletionMapReader) {
        _delDocCount = _deletionMapReader->GetDeletedDocCount();
    }
    _tabletInfoHint.needRefindRtSegmentId = refindRtSegId;

    assert(refindRtSegId <= lastRtSegId);
}

} // namespace indexlibv2::table
