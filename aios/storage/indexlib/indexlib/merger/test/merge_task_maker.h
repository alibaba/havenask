#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_task.h"

namespace indexlib { namespace merger {

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
}} // namespace indexlib::merger
