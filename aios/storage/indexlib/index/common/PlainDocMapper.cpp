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
#include "indexlib/index/common/PlainDocMapper.h"

#include "indexlib/framework/Segment.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PlainDocMapper);

PlainDocMapper::PlainDocMapper(const IIndexMerger::SegmentMergeInfos& segmentMergeInfos)
    : DocMapper(GetDocMapperName(), GetDocMapperType())
    , _segmentMergeInfos(segmentMergeInfos)
{
}

PlainDocMapper::~PlainDocMapper() {}

std::pair<segmentid_t, docid32_t> PlainDocMapper::Map(docid64_t oldDocId) const
{
    docid64_t docIndex = GetNewId(oldDocId);
    if (docIndex == INVALID_DOCID) {
        return std::make_pair(INVALID_SEGMENTID, INVALID_DOCID);
    }

    for (const auto& targetSegment : _segmentMergeInfos.targetSegments) {
        if (docIndex < targetSegment->segmentInfo->GetDocCount()) {
            return {targetSegment->segmentId, docIndex};
        } else {
            docIndex -= targetSegment->segmentInfo->GetDocCount();
        }
    }
    assert(false);
    return std::make_pair(INVALID_SEGMENTID, INVALID_DOCID);
}
std::pair<segmentid_t, docid32_t> PlainDocMapper::ReverseMap(docid64_t newDocId) const
{
    size_t docIndex = 0;
    for (const auto& srcSegment : _segmentMergeInfos.srcSegments) {
        auto docCount = srcSegment.segment->GetDocCount();
        if (newDocId >= docIndex && newDocId < docIndex + docCount) {
            return {srcSegment.segment->GetSegmentId(), newDocId - docIndex};
        }
        docIndex += docCount;
    }
    return std::make_pair(INVALID_SEGMENTID, INVALID_DOCID);
}

int64_t PlainDocMapper::GetTargetSegmentDocCount(segmentid_t segmentId) const
{
    for (auto targetSegment : _segmentMergeInfos.targetSegments) {
        if (targetSegment->segmentId == segmentId) {
            return targetSegment->segmentInfo->GetDocCount();
        }
    }
    assert(false);
    return 0;
}
// oldGlobalDocId -> newDocId
docid64_t PlainDocMapper::GetNewId(docid64_t oldId) const
{
    docid64_t docIndex = 0;
    for (const auto& srcSegment : _segmentMergeInfos.srcSegments) {
        auto docCount = srcSegment.segment->GetDocCount();
        if (srcSegment.baseDocid <= oldId && srcSegment.baseDocid + docCount > oldId) {
            docIndex += (oldId - srcSegment.baseDocid);
            return docIndex;
        }
        if (srcSegment.baseDocid + docCount <= oldId) {
            docIndex += docCount;
        } else {
            assert(false);
        }
    }
    return INVALID_DOCID;
}

// new total doc count
uint32_t PlainDocMapper::GetNewDocCount() const
{
    assert(false);
    return {};
}
segmentid_t PlainDocMapper::GetLocalId(docid64_t newId) const
{
    assert(false);
    return {};
}

Status PlainDocMapper::Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    RETURN_STATUS_ERROR(Status::Unimplement, "PlainDocMapper not supoprt store");
}
Status PlainDocMapper::Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    RETURN_STATUS_ERROR(Status::Unimplement, "PlainDocMapper not supoprt load");
}

} // namespace indexlibv2::index
