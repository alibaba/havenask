#ifndef __INDEXLIB_MULTI_SECTION_META_H
#define __INDEXLIB_MULTI_SECTION_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/section_attribute/section_attribute_formatter.h"

IE_NAMESPACE_BEGIN(index);

class MultiSectionMeta
{
public:
    MultiSectionMeta() 
        : mFieldIds(NULL)
        , mSectionLens(NULL)
        , mSectionWeights(NULL)
        , mSectionCount(0)
    {
    }

    ~MultiSectionMeta() 
    {
    }



public:
    void Init(const uint8_t* dataBuf, bool hasFieldId, bool hasSectionWeight)
    {
        mSectionCount = common::SectionAttributeFormatter::UnpackBuffer(
                dataBuf, hasFieldId, hasSectionWeight,
                mSectionLens, mFieldIds, mSectionWeights);
    }

    uint32_t GetSectionCount() const { return mSectionCount; }

    section_len_t GetSectionLen(uint32_t secId) const 
    {
        assert(mSectionLens); 
        return mSectionLens[secId]; 
    }

    section_fid_t GetFieldId(uint32_t secId) const
    { 
        if (mFieldIds)
        {
            return mFieldIds[secId];
        }
        // Do not have fieldid, all section are considered to first field in package index
        return 0;
    }
        
    section_weight_t GetSectionWeight(uint32_t secId) const
    { 
        if (mSectionWeights)
        {
            return mSectionWeights[secId]; 
        }
        return 0;
    }

private:
    section_fid_t* mFieldIds;
    section_len_t* mSectionLens;
    section_weight_t* mSectionWeights;
    uint32_t mSectionCount;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiSectionMeta);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_SECTION_META_H
