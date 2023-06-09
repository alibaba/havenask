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
#ifndef INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_PARALLEL_MERGE_PARALLEL_MERGE_TASK_H
#define INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_PARALLEL_MERGE_PARALLEL_MERGE_TASK_H

#include "autil/legacy/jsonizable.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"

namespace indexlib {
namespace aitheta_plugin {

typedef std::unordered_map<catid_t, size_t> ParallelMergeInput;

struct ParallelMergeTask;
DEFINE_SHARED_PTR(ParallelMergeTask);

struct ParallelMergeTask : public autil::legacy::Jsonizable {
 public:
    ParallelMergeTask(int32_t id = 0, uint32_t cnt = 0u, float ratio = 0.0f, const std::set<int64_t> &st = {})
        : taskId(id), docNum(cnt), taskRatio(ratio), catidSet(st) {}
    ~ParallelMergeTask() {}

 public:
    static bool Create(uint32_t suggestTaskNum, const ParallelMergeInput &parallelMergeInput,
                       std::vector<ParallelMergeTask> &parallelMergeTasks);

    static bool Retrieve(const indexlib::index_base::MergeItemHint &mergeHint,
                         const indexlib::index_base::MergeTaskResourceVector &taskResources,
                         ParallelMergeTaskPtr &parallelMergeTask);

    static bool Transform(const std::vector<ParallelMergeTask> &parallelMergeTasks,
                          indexlib::index_base::MergeTaskResourceManagerPtr &resMgr,
                          std::vector<indexlib::index::ReduceTask> &reduceTasks);

 public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("category_set", catidSet, {});
        json.Jsonize("document_num", docNum, 0u);
        json.Jsonize("task_id", taskId, 0);
        json.Jsonize("task_ratio", taskRatio, 0.0f);
    }

    int32_t taskId = 0;
    uint32_t docNum = 0u;
    float taskRatio = 0.0f;
    std::set<catid_t> catidSet;

 private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelMergeTask);

}
}

#endif  // INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_PARALLEL_MERGE_PARALLEL_MERGE_TASK_H