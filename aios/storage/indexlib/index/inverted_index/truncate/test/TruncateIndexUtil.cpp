#include "indexlib/index/inverted_index/truncate/test/TruncateIndexUtil.h"

#include "indexlib/index/attribute/Common.h"

namespace indexlib::index {
indexlibv2::index::IIndexMerger::SegmentMergeInfos TruncateIndexUtil::GetSegmentMergeInfos(const std::string& attr)
{
    auto createSegment = []() {
        auto segmentInfo = std::make_shared<indexlibv2::framework::SegmentInfo>();
        segmentInfo->docCount = 100;

        indexlibv2::framework::SegmentMeta meta;
        meta.segmentInfo = segmentInfo;

        auto segment = std::make_shared<FakeSegment>(indexlibv2::framework::Segment::SegmentStatus::ST_BUILT);
        segment->_segmentMeta = std::move(meta);
        return segment;
    };

    indexlibv2::index::IIndexMerger::SegmentMergeInfos mergeInfos;
    auto segment = createSegment();
    std::vector<int32_t> attrValues = {1, 2, 3, 4, 5};
    segment->AddIndexer(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR, attr,
                        std::make_shared<FakeSingleAttributeDiskIndexer>(attrValues));
    mergeInfos.srcSegments.push_back({/*baseDocId*/ 0, std::move(segment)});
    return mergeInfos;
}

} // namespace indexlib::index
