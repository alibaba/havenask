#ifndef __INDEXLIB_ATTRIBUTE_PATCH_ITERATOR_H
#define __INDEXLIB_ATTRIBUTE_PATCH_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/patch_iterator.h"

IE_NAMESPACE_BEGIN(index);

class AttributePatchIterator : public PatchIterator
{
public:
    enum AttrPatchType {APT_SINGLE=0, APT_PACK=1};
    
public:
    AttributePatchIterator(AttrPatchType type, bool isSubDocId)
        : mDocId(INVALID_DOCID)
        , mType(type)
        , mAttrIdentifier(INVALID_FIELDID)
        , mIsSub(isSubDocId)
    {}
    
    virtual ~AttributePatchIterator() {}
    
public:
    bool HasNext() const
    {
        return mDocId != INVALID_DOCID;
    }
    
    virtual void Next(AttrFieldValue& value) = 0;
    virtual void Reserve(AttrFieldValue& value) = 0;

    AttrPatchType GetType() const { return mType; }
    docid_t GetCurrentDocId() const { return mDocId; }
    
    bool operator < (const AttributePatchIterator& other) const
    {
        if (mDocId != other.mDocId)
        {
            return mDocId < other.mDocId;
        }
        if (mType != other.mType)
        {
            return mType < other.mType;
        }
        return mAttrIdentifier < other.mAttrIdentifier;
    }
    
protected:
    docid_t mDocId;
    AttrPatchType mType;
    uint32_t mAttrIdentifier;
    bool mIsSub;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributePatchIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_PATCH_ITERATOR_H
