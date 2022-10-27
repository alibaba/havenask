#ifndef __INDEXLIB_MERGE_TASK_MAKER_H
#define __INDEXLIB_MERGE_TASK_MAKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"

IE_NAMESPACE_BEGIN(merger);

class MergeTaskMaker
{
public:
    MergeTaskMaker();
    ~MergeTaskMaker();
public:
    static void CreateMergeTask(const std::string& mergeTaskStr, MergeTask& mergeTask,
        const index_base::SegmentMergeInfos& segMergeInfos = {});
};

DEFINE_SHARED_PTR(MergeTaskMaker);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_TASK_MAKER_H
