#ifndef __INDEXLIB_REDUCE_TASK_REDUCE_TASK_CREATER_H_
#define __INDEXLIB_REDUCE_TASK_REDUCE_TASK_CREATER_H_

#include <autil/legacy/jsonizable.h>
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class CustomReduceTask : public autil::legacy::Jsonizable {
 public:
    CustomReduceTask(int32_t id = -1, uint32_t cnt = 0u, float ratio = 0, const std::set<int64_t> &st = {})
        : taskId(id), documentCnt(cnt), taskRatio(ratio), categorySet(st) {}
    ~CustomReduceTask() {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("category_set", categorySet, {});
        json.Jsonize("document_cnt", documentCnt, 0u);
        json.Jsonize("task_id", taskId, -1);
        json.Jsonize("task_ratio", taskRatio, 0.0f);
    }

    int32_t taskId = -1;
    uint32_t documentCnt = 0u;
    float taskRatio = 0.0f;
    std::set<int64_t> categorySet;
};

DEFINE_SHARED_PTR(CustomReduceTask);

class ReduceTaskCreater {
 public:
    ReduceTaskCreater() {}
    ~ReduceTaskCreater() {}

 public:
    static std::vector<IE_NAMESPACE(index)::ReduceTask> Create(
        uint32_t suggestTaskNum, bool isEntireDataSet, IE_NAMESPACE(index_base)::MergeTaskResourceManagerPtr &resMgr,
        bool hasIndexSeg, const ReduceTaskInput &taskInput);
    static bool Retrieve(const IE_NAMESPACE(index_base)::MergeItemHint &mergeHint,
                         const IE_NAMESPACE(index_base)::MergeTaskResourceVector &taskResources,
                         CustomReduceTaskPtr &task);

 private:
    static std::vector<CustomReduceTask> CustomCreate(const ReduceTaskInput &taskInput, uint32_t suggestTaskNum);

 private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(aitheta_plugin);

#endif  // __INDEXLIB_REDUCE_TASK_REDUCE_TASK_CREATER_H_
