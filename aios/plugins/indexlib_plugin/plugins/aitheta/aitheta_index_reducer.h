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
#ifndef __INDEXLIB_AITHETA_INDEX_REDUCER_H
#define __INDEXLIB_AITHETA_INDEX_REDUCER_H

#include "indexlib_plugin/plugins/aitheta/common_define.h"

#include <memory>
#include <unordered_map>
#include <vector>

#include <aitheta/index_framework.h>
#include "autil/ThreadPool.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib/indexlib.h"

#include "indexlib_plugin/plugins/aitheta/aitheta_index_reduce_item.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_config.h"

namespace indexlib {
namespace aitheta_plugin {

class AithetaIndexReducer : public indexlib::index::IndexReducer {
 public:
    AithetaIndexReducer(const util::KeyValueMap &parameters) : IndexReducer(parameters), mSchemaParam(parameters) {}
    ~AithetaIndexReducer();

 public:
    bool DoInit(const IndexerResourcePtr &resource) override;

    indexlib::index::IndexReduceItemPtr CreateReduceItem() override {
        return indexlib::index::IndexReduceItemPtr(new AithetaIndexReduceItem());
    }

    int64_t EstimateMemoryUse(const std::vector<file_system::DirectoryPtr> &indexDirs,
                              const index_base::SegmentMergeInfos &segmentInfos,
                              const index_base::OutputSegmentMergeInfos &outputSegMergeInfos,  // target segments
                              bool isSortMerge) const override;

    std::vector<indexlib::index::ReduceTask> CreateReduceTasks(
        const std::vector<indexlib::file_system::DirectoryPtr> &indexDirs,
        const indexlib::index_base::SegmentMergeInfos &segmentInfos, uint32_t instanceCount, bool isEntireDataSet,
        indexlib::index_base::MergeTaskResourceManagerPtr &resMgr) override;

    bool Reduce(const std::vector<indexlib::index::IndexReduceItemPtr> &reduceItems,
                const indexlib::index_base::OutputSegmentMergeInfos &outputSegMergeInfos, bool isSortMerge,
                const indexlib::index::ReduceResource &resource) override;

    void EndParallelReduce(const index_base::OutputSegmentMergeInfos &outputSegMergeInfos, int32_t totalParallelCount,
                           const std::vector<index_base::MergeTaskResourceVector> &instResourceVec) override;

 private:
    SchemaParameter mSchemaParam;
    IndexerResourcePtr mIndexerResource;
    KnnConfig mKnnConfig;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AithetaIndexReducer);

}
}

#endif  //__INDEXLIB_AITHETA_INDEX_REDUCER_H
