#ifndef __INDEXLIB_PATCH_ITERATOR_H
#define __INDEXLIB_PATCH_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_work_item.h"
//#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"

DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index, AttrFieldValue);

IE_NAMESPACE_BEGIN(index);

class PatchIterator
{
public:
    PatchIterator() {}
    virtual ~PatchIterator() {}

public:
    virtual bool HasNext() const = 0;
    virtual void Next(AttrFieldValue& value) = 0;
    virtual void Reserve(AttrFieldValue& value) = 0;
    virtual size_t GetPatchLoadExpandSize() const = 0;
    virtual void CreateIndependentPatchWorkItems(
        std::vector<AttributePatchWorkItem*>& workItems) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PATCH_ITERATOR_H
