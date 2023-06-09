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

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::index {
class DeletionMapIndexReader;
}

namespace indexlibv2::framework {
class TabletData;
}

namespace indexlibv2::table {
class NormalTabletMeta;

struct TabletInfoHint {
    segmentid_t lastIncSegmentId;
    segmentid_t lastRtSegmentId;
    segmentid_t needRefindRtSegmentId;
    size_t lastRtSegmentDocCount;

    TabletInfoHint()
        : lastIncSegmentId(INVALID_SEGMENTID)
        , lastRtSegmentId(INVALID_SEGMENTID)
        , needRefindRtSegmentId(INVALID_SEGMENTID)
        , lastRtSegmentDocCount(0)
    {
    }

    bool operator==(const TabletInfoHint& other)
    {
        return lastIncSegmentId == other.lastIncSegmentId && lastRtSegmentId == other.lastRtSegmentId &&
               needRefindRtSegmentId == other.needRefindRtSegmentId &&
               lastRtSegmentDocCount == other.lastRtSegmentDocCount;
    }
};

class NormalTabletInfo
{
public:
    NormalTabletInfo() = default;
    ~NormalTabletInfo() = default;
    NormalTabletInfo(const NormalTabletInfo& other);

    Status Init(const std::shared_ptr<framework::TabletData>& tabletData,
                const std::shared_ptr<NormalTabletMeta>& tabletMeta,
                const std::shared_ptr<index::DeletionMapIndexReader>& deletionMapReader);

    const std::vector<docid_t>& GetBaseDocIds() const { return _baseDocIds; }
    bool GetOrderedDocIdRanges(DocIdRangeVector& ranges) const;
    bool GetUnorderedDocIdRange(DocIdRange& range) const;
    const TabletInfoHint& GetTabletInfoHint() const { return _tabletInfoHint; }
    std::shared_ptr<NormalTabletMeta> GetTabletMeta() const { return _tabletMeta; }
    size_t GetTotalDocCount() const { return _totalDocCount; }
    size_t GetSegmentCount() const { return _segmentCount; }
    bool NeedUpdate(segmentid_t lastRtSegId, size_t lastRtSegDocCount, segmentid_t refindRtSegId) const;
    globalid_t GetGlobalId(docid_t docId) const;
    docid_t GetDocId(const TabletInfoHint& infoHint, globalid_t gid) const;
    bool GetDiffDocIdRanges(const TabletInfoHint& infoHint, DocIdRangeVector& docIdRanges) const;

    size_t GetIncDocCount() const { return _incDocCount; }
    void UpdateDocCount(segmentid_t lastRtSegId, size_t lastRtSegDocCount, segmentid_t refindRtSegId);
    std::unique_ptr<NormalTabletInfo> Clone() const;

private:
    bool NeedUpdate(segmentid_t lastRtSegId, size_t lastRtSegDocCount) const;
    void InitSegmentIds(const std::shared_ptr<framework::TabletData>& tabletData);
    void InitBaseDocIds(const std::shared_ptr<framework::TabletData>& tabletData);
    void InitDocCount(const std::shared_ptr<framework::TabletData>& tabletData);
    void InitOrderedDocIdRanges(const std::shared_ptr<framework::TabletData>& tabletData);
    void InitUnorderedDocIdRange();
    void InitTabletInfoHint(const std::shared_ptr<framework::TabletData>& tabletData);

    docid_t GetSegmentDocCount(size_t idx) const;
    bool GetIncDiffDocIdRange(segmentid_t lastIncSegId, DocIdRange& range) const;
    bool GetRtDiffDocIdRange(segmentid_t needRefindRtSegmentId, DocIdRange& range) const;
    bool HasSegment(segmentid_t segmentId) const;

private:
    std::shared_ptr<NormalTabletMeta> _tabletMeta;
    std::shared_ptr<index::DeletionMapIndexReader> _deletionMapReader;

    std::vector<segmentid_t> _segmentIds;
    std::vector<docid_t> _baseDocIds;
    std::unordered_set<segmentid_t> _segmentIdSet; // speedup lookup

    size_t _segmentCount = 0;
    size_t _delDocCount = 0;
    size_t _totalDocCount = 0;
    size_t _incDocCount = 0;

    std::vector<DocIdRange> _orderRanges;
    DocIdRange _unorderRange;

    TabletInfoHint _tabletInfoHint;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
