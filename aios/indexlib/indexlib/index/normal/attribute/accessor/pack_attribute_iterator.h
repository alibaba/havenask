#ifndef __INDEXLIB_PACK_ATTRIBUTE_ITERATOR_H
#define __INDEXLIB_PACK_ATTRIBUTE_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"

IE_NAMESPACE_BEGIN(index);

class PackAttributeIterator : public AttributeIteratorBase
{
public:
    PackAttributeIterator(AttributeIteratorTyped<autil::MultiChar>* iterator,
                          autil::mem_pool::Pool *pool)
        : AttributeIteratorBase(pool)
        , mIterator(iterator)
    {}

    ~PackAttributeIterator()
    {
        POOL_COMPATIBLE_DELETE_CLASS(mPool, mIterator);
        mIterator = NULL;
    }

public:
    void Reset() override
    {
        if (mIterator)
        {
            mIterator->Reset();
        }
    }

    inline const char* GetBaseAddress(docid_t docId) __ALWAYS_INLINE
    {
        assert(mIterator);
        autil::MultiChar multiChar;
        if (!mIterator->Seek(docId, multiChar))
        {
            return NULL;
        }
        return multiChar.data();
    }

private:
    AttributeIteratorTyped<autil::MultiChar>* mIterator;
};

DEFINE_SHARED_PTR(PackAttributeIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_ATTRIBUTE_ITERATOR_H
