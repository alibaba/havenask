#ifndef __INDEXLIB_AITHETA_INDEX_REDUCER_H
#define __INDEXLIB_AITHETA_INDEX_REDUCER_H

#include "aitheta_indexer/plugins/aitheta/common_define.h"

#include <memory>
#include <unordered_map>
#include <vector>

#include <aitheta/index_framework.h>
#include <autil/ThreadPool.h>
#include <autil/mem_pool/Pool.h>
#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reducer.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/aitheta_index_reduce_item.h"
#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_config.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaIndexReducer : public IE_NAMESPACE(index)::IndexReducer {
 public:
    AithetaIndexReducer(const util::KeyValueMap &parameters)
        : IndexReducer(parameters), mComParam(parameters), mKvParam(parameters) {}
    ~AithetaIndexReducer();

 public:
    bool DoInit(const IndexerResourcePtr &resource) override;

    IE_NAMESPACE(index)::IndexReduceItemPtr CreateReduceItem() override {
        return IE_NAMESPACE(index)::IndexReduceItemPtr(new AithetaIndexReduceItem());
    }

    int64_t EstimateMemoryUse(const std::vector<IE_NAMESPACE(file_system)::DirectoryPtr> &indexDirs,
                              const IE_NAMESPACE(index_base)::SegmentMergeInfos &segmentInfos,
                              const IE_NAMESPACE(index_base)::OutputSegmentMergeInfos &outputSegMergeInfos,
                              bool isSortMerge) const override;

    std::vector<IE_NAMESPACE(index)::ReduceTask> CreateReduceTasks(
        const std::vector<IE_NAMESPACE(file_system)::DirectoryPtr> &indexDirs,
        const IE_NAMESPACE(index_base)::SegmentMergeInfos &segmentInfos, uint32_t instanceCount, bool isEntireDataSet,
        IE_NAMESPACE(index_base)::MergeTaskResourceManagerPtr &resMgr) override;

    bool Reduce(const std::vector<IE_NAMESPACE(index)::IndexReduceItemPtr> &reduceItems,
                const IE_NAMESPACE(index_base)::OutputSegmentMergeInfos &outputSegMergeInfos, bool isSortMerge,
                const IE_NAMESPACE(index)::ReduceResource &resource) override;

    void EndParallelReduce(const index_base::OutputSegmentMergeInfos &outputSegMergeInfos, int32_t totalParallelCount,
                           const std::vector<index_base::MergeTaskResourceVector> &instResourceVec) override;
 private:
    CommonParam mComParam;
    util::KeyValueMap mKvParam;
    aitheta::IndexPluginBroker mPluginBroker;
    KnnConfig mKnnConfig;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AithetaIndexReducer);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_INDEX_REDUCER_H
