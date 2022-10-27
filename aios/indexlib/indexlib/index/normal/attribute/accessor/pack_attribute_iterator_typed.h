#ifndef __INDEXLIB_PACK_ATTRIBUTE_ITERATOR_TYPED_H
#define __INDEXLIB_PACK_ATTRIBUTE_ITERATOR_TYPED_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference_typed.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_iterator.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class PackAttributeIteratorTyped : public PackAttributeIterator
{
public:
    PackAttributeIteratorTyped(common::AttributeReferenceTyped<T>* reference,
                               AttributeIteratorTyped<autil::MultiChar>* iterator,
                               autil::mem_pool::Pool* pool);
    ~PackAttributeIteratorTyped();

public:
    inline bool Seek(docid_t docId, T& value) __ALWAYS_INLINE;

private:
    common::AttributeReferenceTyped<T>* mReference;
};

//////////////////////////////////////////////////////////////////////
template<typename T>
PackAttributeIteratorTyped<T>::PackAttributeIteratorTyped(
        common::AttributeReferenceTyped<T>* reference,
        AttributeIteratorTyped<autil::MultiChar>* iterator,
        autil::mem_pool::Pool* pool)
    : PackAttributeIterator(iterator, pool)
    , mReference(reference)
{
}

template<typename T>
PackAttributeIteratorTyped<T>::~PackAttributeIteratorTyped()
{
    mReference = NULL;
}

template<typename T>
inline bool PackAttributeIteratorTyped<T>::Seek(docid_t docId, T& value)
{
    const char* base = GetBaseAddress(docId);
    if (base == NULL)
    {
        return false;
    }
    assert(mReference);
    mReference->GetValue(base, value);
    return true;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_ATTRIBUTE_ITERATOR_TYPED_H
