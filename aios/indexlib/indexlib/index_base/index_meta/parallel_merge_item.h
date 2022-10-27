#ifndef __INDEXLIB_PARALLEL_MERGE_ITEM_H
#define __INDEXLIB_PARALLEL_MERGE_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(index_base);

class ParallelMergeItem : public autil::legacy::Jsonizable
{
public:
    static const int32_t INVALID_PARALLEL_MERGE_ITEM_ID = -1;
    static const int32_t INVALID_TASK_GROUP_ID = -1;
    static const int32_t INVALID_TOTAL_PARALLEL_COUNT = -1;
public:
    ParallelMergeItem();
    ParallelMergeItem(int32_t id, float dataRatio,
                      int32_t taskGroupId = INVALID_TASK_GROUP_ID,
                      int32_t totalParallelCount = INVALID_TOTAL_PARALLEL_COUNT);
    ~ParallelMergeItem();

public:
    int32_t GetId() const;
    void SetId(int32_t id);
    float GetDataRatio() const;
    void SetDataRatio(float dataRatio);
    std::vector<int32_t> GetResourceIds() const;
    void AddResource(int32_t resId);
    void RemoveResource(int32_t resId);
    int32_t GetTaskGroupId() const;
    void SetTaskGroupId(int32_t taskGroupId);
    int32_t GetTotalParallelCount() const;
    void SetTotalParallelCount(int32_t totalParallelCount);

    std::string GetParallelInstanceDirName() const;
    static std::string GetParallelInstanceDirName(int32_t parallelCount, int32_t parallelId);
    
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::string ToString() const;
    bool operator == (const ParallelMergeItem &other) const;

private:
    int32_t mId;
    float mDataRatio;
    int32_t mTaskGroupId;
    int32_t mTotalParallelCount;
    std::vector<int32_t> mResourceIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelMergeItem);

typedef ParallelMergeItem MergeItemHint;

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PARALLEL_MERGE_ITEM_H
