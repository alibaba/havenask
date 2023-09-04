#pragma once

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/test/IndexTestUtil.h"
#include "indexlib/table/normal_table/index_task/ReclaimMap.h"

namespace indexlibv2::table {

class FakeSegment : public framework::Segment
{
public:
    FakeSegment(framework::Segment::SegmentStatus segmentStatus) : framework::Segment(segmentStatus) {}
    std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>> GetIndexer(const std::string& type,
                                                                               const std::string& indexName) override
    {
        auto ret = _indexers[{type, indexName}];
        auto status = ret ? Status::OK() : Status::NotFound();
        return {status, ret};
    }

    void AddIndexer(const std::string& type, const std::string& indexName,
                    std::shared_ptr<indexlibv2::index::IIndexer> indexer) override
    {
        _indexers[{type, indexName}] = indexer;
    }
    size_t EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override { return 0; }
    size_t EvaluateCurrentMemUsed() override { return 0; }

private:
    std::map<std::pair<std::string, std::string>, std::shared_ptr<indexlibv2::index::IIndexer>> _indexers;
};

class ReclaimMapUtil : private autil::NoCopyable
{
public:
    ReclaimMapUtil() = default;
    ~ReclaimMapUtil() = default;

public:
    static void PrepareSegments(const indexlib::file_system::DirectoryPtr& rootDir,
                                const std::vector<uint32_t>& docCounts, const std::vector<segmentid_t>& segIds,
                                index::IndexTestUtil::ToDelete toDel,
                                std::vector<index::IIndexMerger::SourceSegment>& sourceSegments);

    static void CheckAnswer(const ReclaimMap& reclaimMap, const std::string& expectedDocIdStr, bool needReverseMapping,
                            const std::string& expectedOldDocIdStr, const std::string& expectedSegIdStr);

    static void CreateTargetSegment(const std::shared_ptr<indexlib::file_system::Directory>& rootDir,
                                    segmentid_t targetSegmentId,
                                    const std::shared_ptr<framework::SegmentInfo>& lastSegmentInfo,
                                    indexlibv2::index::IIndexMerger::SegmentMergeInfos& segMergeInfos);

    static std::shared_ptr<indexlib::file_system::Directory>
    MakeSegmentDirectory(const std::shared_ptr<indexlib::file_system::Directory>& rootDir, segmentid_t segId);

private:
    static framework::SegmentInfo MakeOneSegment(const std::shared_ptr<indexlib::file_system::Directory>& rootDir,
                                                 segmentid_t segmentId, uint32_t docCount);
};

} // namespace indexlibv2::table
