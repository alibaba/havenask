#ifndef __INDEXLIB_ATTRIBUTE_PATCH_WORK_ITEM_H
#define __INDEXLIB_ATTRIBUTE_PATCH_WORK_ITEM_H

#include <tr1/memory>
#include <autil/TimeUtility.h>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <limits>
#include <autil/WorkItem.h>

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, InplaceAttributeModifier);

IE_NAMESPACE_BEGIN(index);

class AttributePatchWorkItem : public autil::WorkItem
{
public:
    // also be used to rank complexity when load patch
    enum PatchWorkItemType
    {
        PWIT_PACK_ATTR = 3,
        PWIT_VAR_NUM = 2,
        PWIT_FIX_LENGTH = 1,
        PWIT_UNKNOWN = 0
    };
public:
    AttributePatchWorkItem(const std::string& id, size_t patchItemCount,
                           bool isSub, PatchWorkItemType itemType)
        : mIdentifier(id)
        , mPatchCount(patchItemCount)
        , mCost(0u)
        , mIsSub(isSub)
        , mType(itemType)
        , mProcessLimit(std::numeric_limits<size_t>::max())
        , mLastProcessTime(0)
        , mLastProcessCount(0)
    {}
    virtual ~AttributePatchWorkItem() {};

public:
    virtual bool Init(const index::DeletionMapReaderPtr& deletionMapReader,
                      const index::InplaceAttributeModifierPtr& attrModifier) = 0;
    
    std::string GetIdentifier() const { return mIdentifier; }
    size_t GetPatchItemCount() const
    {
        return mPatchCount;
    }
    void SetCost(size_t cost)
    {
        mCost = cost;
    }
    size_t GetCost() const
    {
        return mCost;
    };
    uint32_t GetItemType() const
    {
        return (uint32_t)mType;
    }

    virtual bool HasNext() const = 0;
    virtual void ProcessNext() = 0;

    void process();

    void SetProcessLimit(size_t limit)
    {
        mProcessLimit = limit;
    }

    int64_t GetLastProcessTime() const
    {
        autil::ScopedLock lock(mLock);
        return mLastProcessTime;
    }

    size_t GetLastProcessCount() const
    {
        autil::ScopedLock lock(mLock);
        return mLastProcessCount;
    }

    bool IsSub() const
    {
        return mIsSub;
    }

protected:
    std::string mIdentifier;
    size_t mPatchCount;
    size_t mCost;
    bool mIsSub;
    PatchWorkItemType mType;
    size_t mProcessLimit;
    int64_t mLastProcessTime;
    size_t mLastProcessCount;
    mutable autil::ThreadMutex mLock;
    
private:
    IE_LOG_DECLARE();    
};

DEFINE_SHARED_PTR(AttributePatchWorkItem);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_PATCH_WORK_ITEM_H
