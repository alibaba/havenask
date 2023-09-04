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
#pragma once

#include "indexlib/index/common/field_format/attribute/AttributeFieldPrinter.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReferenceTyped.h"
#include "indexlib/index/pack_attribute/PackAttributeIterator.h"

namespace indexlibv2::index {

template <typename T>
class PackAttributeIteratorTyped : public PackAttributeIterator
{
public:
    typedef T AttrType;

    PackAttributeIteratorTyped(AttributeReferenceTyped<T>* reference,
                               AttributeIteratorTyped<autil::MultiChar>* iterator,
                               const AttributeFieldPrinter* fieldPrinter, autil::mem_pool::Pool* pool)
        : PackAttributeIterator(iterator, pool)
        , _reference(reference)
        , _fieldPrinter(fieldPrinter)
    {
    }
    ~PackAttributeIteratorTyped() { _reference = nullptr; }

public:
    inline bool Seek(docid_t docId, T& value) __ALWAYS_INLINE;
    bool Seek(docid_t docId, std::string& attrValue) noexcept override
    {
        T value {};
        if (!Seek(docId, value)) {
            return false;
        }
        assert(_fieldPrinter);
        return _fieldPrinter->Print(false, value, &attrValue);
    }

private:
    AttributeReferenceTyped<T>* _reference = nullptr;
    const AttributeFieldPrinter* _fieldPrinter = nullptr;
};

//////////////////////////////////////////////////////////////////////
template <typename T>
inline bool PackAttributeIteratorTyped<T>::Seek(docid_t docId, T& value)
{
    const char* base = GetBaseAddress(docId);
    if (base == nullptr) {
        return false;
    }
    assert(_reference);
    _reference->GetValue(base, value);
    return true;
}
} // namespace indexlibv2::index
