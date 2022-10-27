#ifndef __INDEXLIB_MERGE_WORK_ITEM_H
#define __INDEXLIB_MERGE_WORK_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/resource_control_work_item.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/util/progress_metrics.h"
#include "indexlib/util/counter/state_counter.h"

DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);

IE_NAMESPACE_BEGIN(merger);

class MergeWorkItem : public util::ResourceControlWorkItem
{
public:
    MergeWorkItem(const MergeFileSystemPtr& mergeFileSystem)
        : mMergeFileSystem(mergeFileSystem)
        , mMetrics(NULL)
        , mCost(0.0)
    {}

    virtual ~MergeWorkItem() {}
    
public:
    virtual int64_t EstimateMemoryUse() const = 0;
    
    virtual void SetMergeItemMetrics(const util::ProgressMetricsPtr& itemMetrics)
    {}

    void Process() override;
    
    std::string GetIdentifier() const { return mIdentifier; }
    void SetIdentifier(const std::string& identifier)
    { mIdentifier = identifier; }

    const std::string& GetCheckPointFileName() const { return mCheckPointName; }
    void SetCheckPointFileName(const std::string& checkPointName)
    { mCheckPointName = checkPointName; }

    void SetMetrics(merger::IndexPartitionMergerMetrics* metrics, double cost)
    { 
        mMetrics = metrics; 
        mCost = cost;
        if (mMetrics)
        {
            mItemMetrics = mMetrics->RegisterMergeItem(mCost);
            SetMergeItemMetrics(mItemMetrics);
        }
    }
    
    void ReportMetrics()
    {
        if (mItemMetrics)
        {
            mItemMetrics->SetFinish();
        }
    }
    
    double GetCost() const { return mCost; }

    void SetCounter(const util::StateCounterPtr& counter)
    {
        mCounter = counter;
    }

protected:
    virtual void DoProcess() = 0;
    
protected:
    util::StateCounterPtr mCounter;
    MergeFileSystemPtr mMergeFileSystem;
    
private:
    std::string mIdentifier;
    std::string mCheckPointName;
    merger::IndexPartitionMergerMetrics* mMetrics;
    util::ProgressMetricsPtr mItemMetrics;
    double mCost;
private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(MergeWorkItem);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_WORK_ITEM_H
