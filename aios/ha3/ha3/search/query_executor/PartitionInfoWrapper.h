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
#include <vector>

#include "autil/Log.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/normal_table/NormalTabletInfo.h"
#include "indexlib/table/normal_table/NormalTabletMeta.h"

namespace isearch {
namespace search {

class PartitionInfoWrapper {
public:
    explicit PartitionInfoWrapper(const std::shared_ptr<indexlib::index::PartitionInfo> &partInfo)
        : _partitionInfoPtr(partInfo) {
        assert(_partitionInfoPtr);
    }
    explicit PartitionInfoWrapper(
        const std::shared_ptr<indexlibv2::table::NormalTabletInfo> &tabletInfo)
        : _normalTabletInfoPtr(tabletInfo) {
        assert(_normalTabletInfoPtr);
    }

    const std::vector<docid_t> &GetBaseDocIds() const;
    bool GetOrderedDocIdRanges(indexlib::DocIdRangeVector &ranges) const;
    bool GetUnorderedDocIdRange(indexlib::DocIdRange &range) const;
    indexlib::index::PartitionInfoHint GetPartitionInfoHint() const;
    std::shared_ptr<indexlib::index_base::PartitionMeta> GetPartitionMeta() const;
    size_t GetTotalDocCount() const;
    size_t GetSegmentCount() const;
    globalid_t GetGlobalId(docid_t docId) const;
    docid_t GetDocId(const indexlib::index::PartitionInfoHint &infoHint, globalid_t gid) const;
    bool GetDiffDocIdRanges(const indexlib::index::PartitionInfoHint &infoHint,
                            indexlib::DocIdRangeVector &docIdRanges) const;

private:
    indexlibv2::table::TabletInfoHint
    ConverPartitionInfoToTabletInfo(const indexlib::index::PartitionInfoHint &infoHint) const;

private:
    std::shared_ptr<indexlib::index::PartitionInfo> _partitionInfoPtr;
    std::shared_ptr<indexlibv2::table::NormalTabletInfo> _normalTabletInfoPtr;

    AUTIL_LOG_DECLARE();
};

inline const std::vector<docid_t> &PartitionInfoWrapper::GetBaseDocIds() const {
    if (_normalTabletInfoPtr) {
        return _normalTabletInfoPtr->GetBaseDocIds();
    }
    assert(_partitionInfoPtr);
    return _partitionInfoPtr->GetBaseDocIds();
}

inline bool PartitionInfoWrapper::GetOrderedDocIdRanges(indexlib::DocIdRangeVector &ranges) const {
    if (_normalTabletInfoPtr) {
        return _normalTabletInfoPtr->GetOrderedDocIdRanges(ranges);
    }
    assert(_partitionInfoPtr);
    return _partitionInfoPtr->GetOrderedDocIdRanges(ranges);
}

inline bool PartitionInfoWrapper::GetUnorderedDocIdRange(indexlib::DocIdRange &range) const {
    if (_normalTabletInfoPtr) {
        return _normalTabletInfoPtr->GetUnorderedDocIdRange(range);
    }
    assert(_partitionInfoPtr);
    return _partitionInfoPtr->GetUnorderedDocIdRange(range);
}

inline indexlib::index::PartitionInfoHint PartitionInfoWrapper::GetPartitionInfoHint() const {
    static_assert(
        std::is_same<decltype(indexlibv2::table::TabletInfoHint::infoVersion),
                     decltype(indexlib::index::PartitionInfoHint::lastRtSegmentId)>::value,
        "type of infoVersion and lastIncSegmentId need to be same type");
    if (_normalTabletInfoPtr) {
        indexlibv2::table::TabletInfoHint tabletInfoHint
            = _normalTabletInfoPtr->GetTabletInfoHint();
        indexlib::index::PartitionInfoHint partitionInfoHint;
        partitionInfoHint.lastIncSegmentId = tabletInfoHint.infoVersion;
        partitionInfoHint.lastRtSegmentId = tabletInfoHint.lastRtSegmentId;
        partitionInfoHint.lastRtSegmentDocCount = tabletInfoHint.lastRtSegmentDocCount;
        return partitionInfoHint;
    }
    assert(_partitionInfoPtr);
    return _partitionInfoPtr->GetPartitionInfoHint();
}

inline std::shared_ptr<indexlib::index_base::PartitionMeta>
PartitionInfoWrapper::GetPartitionMeta() const {
    if (_normalTabletInfoPtr) {
        auto tabletMeta = _normalTabletInfoPtr->GetTabletMeta();
        auto partMeta = std::make_shared<indexlib::index_base::PartitionMeta>();
        for (auto &desc : tabletMeta->GetSortDescriptions()) {
            partMeta->AddSortDescription(desc.GetSortFieldName(), desc.GetSortPattern());
        }
        return partMeta;
    }
    assert(_partitionInfoPtr);
    return _partitionInfoPtr->GetPartitionMeta();
}

inline size_t PartitionInfoWrapper::GetTotalDocCount() const {
    if (_normalTabletInfoPtr) {
        return _normalTabletInfoPtr->GetTotalDocCount();
    }
    assert(_partitionInfoPtr);
    return _partitionInfoPtr->GetTotalDocCount();
}

inline size_t PartitionInfoWrapper::GetSegmentCount() const {
    if (_normalTabletInfoPtr) {
        return _normalTabletInfoPtr->GetSegmentCount();
    }
    assert(_partitionInfoPtr);
    return _partitionInfoPtr->GetSegmentCount();
}

inline globalid_t PartitionInfoWrapper::GetGlobalId(docid_t docId) const {
    if (_normalTabletInfoPtr) {
        return _normalTabletInfoPtr->GetGlobalId(docId);
    }
    assert(_partitionInfoPtr);
    return _partitionInfoPtr->GetGlobalId(docId);
}

inline docid_t PartitionInfoWrapper::GetDocId(const indexlib::index::PartitionInfoHint &infoHint,
                                              globalid_t gid) const {
    if (_normalTabletInfoPtr) {
        auto tabletInfo = ConverPartitionInfoToTabletInfo(infoHint);
        return _normalTabletInfoPtr->GetDocId(tabletInfo, gid);
    }
    assert(_partitionInfoPtr);
    return _partitionInfoPtr->GetDocId(gid);
}

inline indexlibv2::table::TabletInfoHint PartitionInfoWrapper::ConverPartitionInfoToTabletInfo(
    const indexlib::index::PartitionInfoHint &infoHint) const {
    indexlibv2::table::TabletInfoHint tabletInfoHint;
    static_assert(
        std::is_same<decltype(indexlibv2::table::TabletInfoHint::infoVersion),
                     decltype(indexlib::index::PartitionInfoHint::lastRtSegmentId)>::value,
        "type of infoVersion and lastIncSegmentId need to be same type");

    tabletInfoHint.infoVersion = infoHint.lastIncSegmentId;
    tabletInfoHint.lastRtSegmentId = infoHint.lastRtSegmentId;
    tabletInfoHint.lastRtSegmentDocCount = infoHint.lastRtSegmentDocCount;
    return tabletInfoHint;
}

inline bool
PartitionInfoWrapper::GetDiffDocIdRanges(const indexlib::index::PartitionInfoHint &infoHint,
                                         indexlib::DocIdRangeVector &docIdRanges) const {
    if (_normalTabletInfoPtr) {
        indexlibv2::table::TabletInfoHint tabletInfoHint
            = ConverPartitionInfoToTabletInfo(infoHint);
        return _normalTabletInfoPtr->GetDiffDocIdRanges(tabletInfoHint, docIdRanges);
    }
    assert(_partitionInfoPtr);
    return _partitionInfoPtr->GetDiffDocIdRanges(infoHint, docIdRanges);
}

} // namespace search
} // namespace isearch
