/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_PACK_ATTRIBUTE_ITERATOR_TYPED_H
#define __INDEXLIB_PACK_ATTRIBUTE_ITERATOR_TYPED_H

#include <memory>

#include "indexlib/common/field_format/pack_attribute/attribute_reference_typed.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class PackAttributeIteratorTyped : public PackAttributeIterator
{
public:
    typedef T AttrType;

    PackAttributeIteratorTyped(common::AttributeReferenceTyped<T>* reference,
                               AttributeIteratorTyped<autil::MultiChar>* iterator, autil::mem_pool::Pool* pool);
    ~PackAttributeIteratorTyped();

public:
    inline bool Seek(docid_t docId, T& value) __ALWAYS_INLINE;

private:
    common::AttributeReferenceTyped<T>* mReference;
};

//////////////////////////////////////////////////////////////////////
template <typename T>
PackAttributeIteratorTyped<T>::PackAttributeIteratorTyped(common::AttributeReferenceTyped<T>* reference,
                                                          AttributeIteratorTyped<autil::MultiChar>* iterator,
                                                          autil::mem_pool::Pool* pool)
    : PackAttributeIterator(iterator, pool)
    , mReference(reference)
{
}

template <typename T>
PackAttributeIteratorTyped<T>::~PackAttributeIteratorTyped()
{
    mReference = NULL;
}

template <typename T>
inline bool PackAttributeIteratorTyped<T>::Seek(docid_t docId, T& value)
{
    const char* base = GetBaseAddress(docId);
    if (base == NULL) {
        return false;
    }
    assert(mReference);
    mReference->GetValue(base, value);
    return true;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_PACK_ATTRIBUTE_ITERATOR_TYPED_H
