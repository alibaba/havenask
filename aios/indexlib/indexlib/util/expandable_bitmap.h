#ifndef __INDEXLIB_EXPANDABLE_BITMAP_H
#define __INDEXLIB_EXPANDABLE_BITMAP_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/util/bitmap.h"

IE_NAMESPACE_BEGIN(util);

class ExpandableBitmap : public Bitmap
{
public:
    ExpandableBitmap(bool bSet = false,
                     autil::mem_pool::PoolBase* pool = NULL);
    ExpandableBitmap(uint32_t nItemCount, bool bSet = false,
                     autil::mem_pool::PoolBase* pool = NULL);
    ExpandableBitmap(const ExpandableBitmap& rhs);
    virtual ~ExpandableBitmap() {}
public:
    void Set(uint32_t nIndex);
    void Reset(uint32_t nIndex);
    void ReSize(uint32_t nItemCount);
    void Mount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete = true);
    void MountWithoutRefreshSetCount(uint32_t nItemCount, uint32_t* pData, 
            bool doNotDelete = true);    
    uint32_t GetValidItemCount() const {return mValidItemCount;}

    // return ExpandableBitmap use independent memory, use different pool
    ExpandableBitmap* CloneWithOnlyValidData() const;

    ExpandableBitmap* Clone() const;

    void Expand(uint32_t nIndex);

private:
    uint32_t mValidItemCount;
    
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<ExpandableBitmap> ExpandableBitmapPtr;

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_EXPANDABLE_BITMAP_H
