#pragma once
#include <memory>
#include <vector>

#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/MergeStrategy.h"

namespace indexlibv2::table {

class FakeSegment : public framework::Segment
{
public:
    FakeSegment(framework::Segment::SegmentStatus segmentStatus) : framework::Segment(segmentStatus) {}
    std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>> GetIndexer(const std::string& type,
                                                                               const std::string& indexName) override
    {
        if (_deletionmapDiskIndexer) {
            return std::make_pair(Status::OK(), _deletionmapDiskIndexer);
        }
        return std::make_pair(Status::NotFound(), nullptr);
    }

    void AddIndexer(const std::string& type, const std::string& indexName,
                    std::shared_ptr<indexlibv2::index::IIndexer> indexer) override
    {
        _deletionmapDiskIndexer = indexer;
    }
    std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override
    {
        return {Status::OK(), 0};
    }
    size_t EvaluateCurrentMemUsed() override { return 0; }

private:
    std::shared_ptr<index::IIndexer> _deletionmapDiskIndexer;
};
class FakeContext : public framework::IndexTaskContext
{
public:
    FakeContext() {}
    ~FakeContext() {}
    void Init(const std::shared_ptr<framework::TabletData>& tabletData)
    {
        _tabletData = tabletData;
        _tabletOptions = std::make_shared<config::TabletOptions>();
    }
    void TEST_SetMaxMergedSegmentId(segmentid_t segId) { _maxMergedSegmentId = segId; }
    void TEST_SetMaxMergedVersionId(versionid_t versionId) { _maxMergedVersionId = versionId; }
};

class FakeDirectory : public indexlib::file_system::LocalDirectory
{
public:
    FakeDirectory(size_t size) : LocalDirectory("", nullptr), _size(size) {}
    indexlib::file_system::FSResult<size_t> GetDirectorySize(const std::string& path) noexcept override
    {
        assert(path == "");
        return {indexlib::file_system::FSEC_OK, _size};
    }

private:
    size_t _size;
};

struct FakeSegmentInfo {
    segmentid_t id = INVALID_SEGMENTID;
    bool isMerged;
    uint32_t docCount;
    int64_t deleteDocCount;
    int64_t timestamp;
    uint64_t segmentSize;
};

class MergeTestHelper
{
public:
    MergeTestHelper();
    ~MergeTestHelper();

public:
    // segInfos: same as CreateContext
    // levelInfo:topo,shardCount,levelNum:segId,segId(level0)|segId,segId(level1)...
    static std::shared_ptr<framework::IndexTaskContext>
    CreateContextWithLevelInfo(const std::vector<FakeSegmentInfo>& segInfos, const std::string& levelInfo);

    static std::shared_ptr<framework::IndexTaskContext>
    CreateContextAndParseParams(const std::string& rawStrategyParamString, const std::string& mergeStrategyName,
                                const std::vector<FakeSegmentInfo>& segInfos);

    static void VerifyMergePlan(MergeStrategy* mergeStrategy, const framework::IndexTaskContext* context,
                                const std::vector<std::vector<segmentid_t>>& expectSrcSegments);

    static void TestMergeStrategy(MergeStrategy* mergeStrategy,
                                  const std::vector<std::vector<segmentid_t>>& expectSrcSegments,
                                  const std::string& rawStrategyParamString,
                                  const std::vector<FakeSegmentInfo>& segInfos, bool isOptimizeMerge = false);

    static void StoreVersion(const framework::Version& version,
                             const std::shared_ptr<indexlib::file_system::IDirectory>& fenceDir,
                             bool ignoreEmptyVersion = true);

private:
    static std::shared_ptr<framework::IndexTaskContext> CreateContext(const std::vector<FakeSegmentInfo>& segInfos);
};

} // namespace indexlibv2::table
