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
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/DocValueFilter.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

template <typename T>
class NumberDocValueTraits
{
public:
    typedef indexlib::index::SingleValueAttributeReader<T> AttributeReaderType;
    typedef indexlib::index::AttributeIteratorTyped<T> AttributeIterator;
    static inline bool TestValue(T value, int64_t leftValue, int64_t rightValue)
    {
        return leftValue <= (int64_t)value && (int64_t)value <= rightValue;
    }
};

#define DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(T)                                                                        \
    template <>                                                                                                        \
    class NumberDocValueTraits<autil::MultiValueType<T>>                                                               \
    {                                                                                                                  \
    public:                                                                                                            \
        typedef indexlib::index::VarNumAttributeReader<T> AttributeReaderType;                                         \
        typedef indexlib::index::AttributeIteratorTyped<autil::MultiValueType<T>> AttributeIterator;                   \
        static inline bool TestValue(autil::MultiValueType<T> value, int64_t leftValue, int64_t rightValue)            \
        {                                                                                                              \
            uint32_t num = value.size();                                                                               \
            for (uint32_t i = 0; i < num; i++) {                                                                       \
                T v = value[i];                                                                                        \
                if (leftValue <= (int64_t)v && (int64_t)v <= rightValue) {                                             \
                    return true;                                                                                       \
                }                                                                                                      \
            }                                                                                                          \
            return false;                                                                                              \
        }                                                                                                              \
    };

DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(uint8_t);
DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(uint16_t);
DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(uint32_t);
DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(uint64_t);
DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(int8_t);
DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(int16_t);
DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(int32_t);
DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(int64_t);
DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(float);
DEFINE_MULTI_NUMBER_DOC_VALUE_TRAITS(double);

template <typename T>
class NumberDocValueFilterTyped : public DocValueFilter
{
public:
    typedef typename NumberDocValueTraits<T>::AttributeReaderType AttributeReaderType;
    DEFINE_SHARED_PTR(AttributeReaderType);
    typedef typename NumberDocValueTraits<T>::AttributeIterator AttributeIterator;

    NumberDocValueFilterTyped(const indexlib::index::AttributeReaderPtr& attrReader, int64_t left, int64_t right,
                              autil::mem_pool::Pool* sessionPool)
        : DocValueFilter(sessionPool)
        , _left(left)
        , _right(right)
        , _attrIter(NULL)
    {
        _attrReader = DYNAMIC_POINTER_CAST(AttributeReaderType, attrReader);
        if (_attrReader) {
            _attrIter = _attrReader->CreateIterator(_sessionPool);
        }
    }

    NumberDocValueFilterTyped(const NumberDocValueFilterTyped<T>& other)
        : DocValueFilter(other)
        , _left(other._left)
        , _right(other._right)
        , _attrReader(other._attrReader)
        , _attrIter(NULL)
    {
        if (_attrReader) {
            _attrIter = _attrReader->CreateIterator(_sessionPool);
        }
    }

    ~NumberDocValueFilterTyped()
    {
        if (_attrIter) {
            indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _attrIter);
        }
    }

    bool Test(docid_t docid) override { return InnerTest(docid); }

    bool InnerTest(docid_t docid)
    {
        T value {};
        if (!_attrIter || !_attrIter->Seek(docid, value)) {
            return false;
        }
        return NumberDocValueTraits<T>::TestValue(value, _left, _right);
    }

    DocValueFilter* Clone() const override
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, NumberDocValueFilterTyped<T>, *this);
    }

private:
    int64_t _left;
    int64_t _right;
    AttributeReaderTypePtr _attrReader;
    AttributeIterator* _attrIter;
};

} // namespace indexlib::index::legacy
