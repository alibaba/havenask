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

void NormalTabletInfo::Init(const std::shared_ptr<framework::TabletData>& tabletData,
                            const std::shared_ptr<NormalTabletMeta>& tabletMeta,
                            const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader,
                            const HistorySegmentInfos& historySegmentMap)
{
    _tabletMeta = tabletMeta;
    _deletionMapReader = deletionMapReader;
    _historySegmentInfos = historySegmentMap;

    InitCurrentInfo(tabletData);
    InitHistorySegMap(tabletData, historySegmentMap);
    InitDocCount(tabletData);
    InitOrderedDocIdRanges(tabletData);
    InitUnorderedDocIdRange();
    InitTabletInfoHint(tabletData);
    assert(_historySegToIdxMap.size() == _historySegmentInfos.infos.size());
}

void NormalTabletInfo::InitHistorySegMap(const std::shared_ptr<framework::TabletData>& tabletData,
                                         const HistorySegmentInfos& historySegmentInfos)
{
    // <segmentid, <current idx, is sorted> >
    std::map<segmentid_t, std::pair<int32_t, bool>> segIdxMap;
    assert(!_historySegmentInfos.infos.empty());
    const auto& currentSegments = *(_historySegmentInfos.infos.rbegin());
    for (size_t i = 0; i < currentSegments.size(); ++i) {
        segIdxMap[currentSegments[i].segmentId] = std::make_pair(i, currentSegments[i].isSorted);
    }
    for (size_t i = 0; i < _historySegmentInfos.infos.size(); ++i) {
        std::unordered_map<segmentid_t, int32_t> currentInfo;
        auto& sortInfos = _historySegmentInfos.infos[i];
        for (const auto& sortInfo : sortInfos) {
            auto currentIter = segIdxMap.find(sortInfo.segmentId);
            if (currentIter == segIdxMap.end() || (sortInfo.isSorted != currentIter->second.second)) {
                // do not has current segment or sort status changed
                continue;
            }
            currentInfo[sortInfo.segmentId] = currentIter->second.first;
        }
        _historySegToIdxMap.push_back(currentInfo);
    }
}

void NormalTabletInfo::InitCurrentInfo(const std::shared_ptr<framework::TabletData>& tabletData)
{
    auto segments = tabletData->CreateSlice();
    docid_t baseDocId = 0;
    std::vector<SegmentSortInfo> currentSegmentSortInfo;

    for (const auto& segment : segments) {
        _baseDocIds.push_back(baseDocId);
        baseDocId += segment->GetSegmentInfo()->docCount;
        auto segStatus = segment->GetSegmentStatus();
        SegmentSortInfo sortInfo;
        sortInfo.segmentId = segment->GetSegmentId();
        sortInfo.isSorted = false;
        if (segStatus == framework::Segment::SegmentStatus::ST_BUILT) {
            sortInfo.isSorted = SegmentSortDecisionMaker::IsSortedDiskSegment(_tabletMeta->GetSortDescriptions(),
                                                                              segment->GetSegmentId());
        }
        currentSegmentSortInfo.push_back(sortInfo);
    }
    assert(std::is_sorted(currentSegmentSortInfo.begin(), currentSegmentSortInfo.end(),
                          [](const auto& left, const auto& right) { return left.segmentId < right.segmentId; }));
    _historySegmentInfos.infos.push_back(currentSegmentSortInfo);
    if (_historySegmentInfos.infos.size() > MAX_HISTORY_SEG_INFO_COUNT) {
        _historySegmentInfos.infos.erase(_historySegmentInfos.infos.begin());
        _historySegmentInfos.minVersion++;
    }
}

std::unique_ptr<NormalTabletInfo> NormalTabletInfo::Clone() const { return std::make_unique<NormalTabletInfo>(*this); }

// calculate _segmentCount, _delDocCount, _totalDocCount, _incDocCount
void NormalTabletInfo::InitDocCount(const std::shared_ptr<framework::TabletData>& tabletData)
{
    auto segments = tabletData->CreateSlice();
    size_t totalDocCount = 0;
    size_t incDocCount = 0;
    for (const auto& segment : segments) {
        auto segStatus = segment->GetSegmentStatus();
        _segmentCount++;
        totalDocCount += segment->GetSegmentInfo()->docCount;
        if (segStatus == framework::Segment::SegmentStatus::ST_BUILT ||
            !framework::Segment::IsPrivateSegmentId(segment->GetSegmentId())) {
            incDocCount += segment->GetSegmentInfo()->docCount;
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
    _tabletInfoHint.infoVersion = _historySegmentInfos.minVersion + _historySegmentInfos.infos.size() - 1;
    auto buildingSegmentSlice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILDING);
    auto iter = buildingSegmentSlice.begin();
    if (iter != buildingSegmentSlice.end()) {
        auto segmentPtr = *iter;
        assert(segmentPtr);
        _tabletInfoHint.lastRtSegmentId = segmentPtr->GetSegmentId();
        _tabletInfoHint.lastRtSegmentDocCount = segmentPtr->GetSegmentInfo()->docCount;
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
            const auto& currentSegmentInfos = *(_historySegmentInfos.infos.rbegin());
            segmentid_t segId = currentSegmentInfos[i].segmentId;
            if (_deletionMapReader && _deletionMapReader->IsDeleted(docId)) {
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

bool NormalTabletInfo::GetHistoryMapOffset(int32_t version, int32_t* offset) const
{
    assert(!_historySegmentInfos.infos.empty());
    assert(offset);
    int32_t mapOffset = version - _historySegmentInfos.minVersion;
    if (mapOffset < 0 || mapOffset >= _historySegmentInfos.infos.size()) {
        return false;
    }
    *offset = mapOffset;
    return true;
}

docid_t NormalTabletInfo::GetDocId(const TabletInfoHint& infoHint, globalid_t gid) const
{
    int32_t mapOffset = 0;
    if (!GetHistoryMapOffset(infoHint.infoVersion, &mapOffset)) {
        return INVALID_DOCID;
    }
    int32_t segId = gid >> 32;
    const auto& idxMap = _historySegToIdxMap[mapOffset];
    auto currentIdxIter = idxMap.find(segId);
    if (currentIdxIter == idxMap.end()) {
        return INVALID_DOCID;
    }
    docid_t localDocId = gid & 0xFFFFFFFF;
    if (localDocId >= GetSegmentDocCount(currentIdxIter->second)) {
        return INVALID_DOCID;
    }
    docid_t docId = localDocId + _baseDocIds[currentIdxIter->second];
    if (_deletionMapReader && _deletionMapReader->IsDeleted(docId)) {
        return INVALID_DOCID;
    }
    return docId;
}

bool NormalTabletInfo::GetDiffDocIdRanges(const TabletInfoHint& infoHint, DocIdRangeVector& docIdRanges) const
{
    int32_t mapOffset = 0;
    docIdRanges.clear();
    if (!GetHistoryMapOffset(infoHint.infoVersion, &mapOffset)) {
        MigrageDocIdRanges(docIdRanges, {0, _totalDocCount});
    } else {
        const auto& hisSegments = _historySegmentInfos.infos[mapOffset];
        const auto& currentSegments = *(_historySegmentInfos.infos.rbegin());
        auto iterHis = hisSegments.begin();
        auto iterCurrent = currentSegments.begin();
        uint32_t currentIdx = 0;
        while (iterCurrent != currentSegments.end()) {
            segmentid_t currentSegId = iterCurrent->segmentId;
            while (iterHis != hisSegments.end() && iterHis->segmentId < currentSegId) {
                iterHis++;
            }
            docid_t beginDocId = INVALID_DOCID;
            docid_t endDocId = INVALID_DOCID;
            if (iterHis == hisSegments.end() || currentSegId != iterHis->segmentId) {
                // history not has this segment, migrate all
                beginDocId = _baseDocIds[currentIdx];
                endDocId = _baseDocIds[currentIdx] + GetSegmentDocCount(currentIdx);
            } else if (iterHis->isSorted != iterCurrent->isSorted) {
                // segment sort status changed, migrage all
                beginDocId = _baseDocIds[currentIdx];
                endDocId = _baseDocIds[currentIdx] + GetSegmentDocCount(currentIdx);
            } else if (currentSegId == infoHint.lastRtSegmentId) {
                // last building segment, migrage new docs
                beginDocId = _baseDocIds[currentIdx] + infoHint.lastRtSegmentDocCount;
                endDocId = _baseDocIds[currentIdx] + GetSegmentDocCount(currentIdx);
            }
            MigrageDocIdRanges(docIdRanges, {beginDocId, endDocId});
            iterCurrent++;
            currentIdx++;
        }
    }
    return !docIdRanges.empty();
}

bool NormalTabletInfo::NeedUpdate(size_t lastRtSegDocCount) const
{
    return lastRtSegDocCount > _tabletInfoHint.lastRtSegmentDocCount;
}

void NormalTabletInfo::UpdateDocCount(size_t lastRtSegDocCount)
{
    if (NeedUpdate(lastRtSegDocCount)) {
        assert(_tabletInfoHint.lastRtSegmentId);
        size_t increaseDocCount = lastRtSegDocCount - _tabletInfoHint.lastRtSegmentDocCount;
        _tabletInfoHint.lastRtSegmentDocCount = lastRtSegDocCount;
        _totalDocCount += increaseDocCount;
        _unorderRange.second = _totalDocCount;
    }
    if (_deletionMapReader) {
        _delDocCount = _deletionMapReader->GetDeletedDocCount();
    }
}

void NormalTabletInfo::MigrageDocIdRanges(DocIdRangeVector& current, const DocIdRange& toAppend) const
{
    if (toAppend.first >= toAppend.second) {
        return;
    }
    assert(current.empty() || current.rbegin()->second <= toAppend.first);
    if (current.empty() || current.rbegin()->second != toAppend.first) {
        current.push_back(toAppend);
    } else {
        current.rbegin()->second = toAppend.second;
    }
}

} // namespace indexlibv2::table
