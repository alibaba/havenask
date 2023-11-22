#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace merger {
class DemoParallelReduceMeta : public index::ParallelReduceMeta
{
public:
    DemoParallelReduceMeta(uint32_t parallelCount) : index::ParallelReduceMeta(parallelCount) {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("parallel_count", parallelCount, parallelCount);
        if (json.GetMode() == TO_JSON) {
            std::string test = "test";
            json.Jsonize(test, test);
        }
    }
};

class DemoIndexReducer : public index::IndexReducer
{
public:
    DemoIndexReducer(const util::KeyValueMap& parameters) : IndexReducer(parameters) {}

    ~DemoIndexReducer() {}

public:
    index::IndexReduceItemPtr CreateReduceItem() override;

    bool DoInit(const IndexerResourcePtr& resource) override;

    bool Reduce(const std::vector<index::IndexReduceItemPtr>& reduceItems,
                const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool isSortMerge,
                const index::ReduceResource& resource) override;

    int64_t EstimateMemoryUse(const std::vector<file_system::DirectoryPtr>& indexDirs,
                              const index_base::SegmentMergeInfos& segmentMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortMerge) const override;

    std::vector<index::ReduceTask> CreateReduceTasks(const std::vector<file_system::DirectoryPtr>& indexDirs,
                                                     const index_base::SegmentMergeInfos& segmentInfos,
                                                     uint32_t instanceCount, bool isEntireDataSet,
                                                     index_base::MergeTaskResourceManagerPtr& resMgr) override;

    void EndParallelReduce(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
                           const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) override;

private:
    util::StateCounterPtr mMergedItemCounter;
    typedef std::map<std::string, std::string> KVMap;
    KVMap mMeta;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoIndexReducer);
}} // namespace indexlib::merger
