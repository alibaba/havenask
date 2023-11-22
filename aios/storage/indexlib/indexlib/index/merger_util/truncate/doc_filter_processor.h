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
#include "indexlib/config/diversity_constrain.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader.h"
#include "indexlib/index/merger_util/truncate/truncate_meta_reader.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, PostingIterator);

namespace indexlib::index::legacy {

class DocFilterProcessor
{
public:
    DocFilterProcessor() {}
    virtual ~DocFilterProcessor() {}

public:
    virtual bool BeginFilter(const index::DictKeyInfo& key, const std::shared_ptr<PostingIterator>& postingIt) = 0;
    virtual void BeginFilter(int64_t minValue, int64_t maxValue,
                             const PostingIteratorPtr& postingIt = PostingIteratorPtr()) = 0;
    virtual bool IsFiltered(docid_t docId) = 0;
    virtual void SetTruncateMetaReader(const TruncateMetaReaderPtr& metaReader) = 0;
};

template <typename T>
class DocFilterProcessorTyped final : public DocFilterProcessor
{
public:
    DocFilterProcessorTyped(const TruncateAttributeReaderPtr attrReader, const config::DiversityConstrain& constrain);
    ~DocFilterProcessorTyped();

public:
    bool BeginFilter(const index::DictKeyInfo& key, const std::shared_ptr<PostingIterator>& postingIt) override;
    void BeginFilter(int64_t minValue, int64_t maxValue,
                     const PostingIteratorPtr& postingIt = PostingIteratorPtr()) override;
    bool IsFiltered(docid_t docId) override;
    void SetTruncateMetaReader(const TruncateMetaReaderPtr& metaReader) override;

    void SetPostingIterator(const std::shared_ptr<PostingIterator>& postingIt) {};

    // for test
    void GetFilterRange(int64_t& min, int64_t& max);

private:
    TruncateAttributeReaderPtr mAttrReader;
    TruncateMetaReaderPtr mMetaReader;
    int64_t mMinValue;
    int64_t mMaxValue;
    uint64_t mMask;
    AttributeSegmentReader::ReadContextBasePtr mReadCtx;
    autil::mem_pool::UnsafePool mPool;
};

template <typename T>
DocFilterProcessorTyped<T>::DocFilterProcessorTyped(const TruncateAttributeReaderPtr attrReader,
                                                    const config::DiversityConstrain& constrain)
{
    mMaxValue = constrain.GetFilterMaxValue();
    mMinValue = constrain.GetFilterMinValue();
    mMask = constrain.GetFilterMask();
    mAttrReader = attrReader;
    mReadCtx = mAttrReader->CreateReadContextPtr(&mPool);
}

template <typename T>
DocFilterProcessorTyped<T>::~DocFilterProcessorTyped()
{
    mReadCtx.reset();
}

template <typename T>
inline bool DocFilterProcessorTyped<T>::BeginFilter(const index::DictKeyInfo& key,
                                                    const std::shared_ptr<PostingIterator>& postingIt)
{
    if (mMetaReader && !mMetaReader->Lookup(key, mMinValue, mMaxValue)) {
        return false;
    }
    return true;
}

template <typename T>
inline bool DocFilterProcessorTyped<T>::IsFiltered(docid_t docId)
{
    T value {};
    bool isNull = false;
    mAttrReader->Read(docId, mReadCtx, (uint8_t*)&value, sizeof(T), isNull);
    if (isNull) {
        // null value will not match the section [min, max]
        return true;
    }
    value &= (T)mMask;
    return !(mMinValue <= (int64_t)value && (int64_t)value <= mMaxValue);
}

template <typename T>
inline void DocFilterProcessorTyped<T>::SetTruncateMetaReader(const TruncateMetaReaderPtr& metaReader)
{
    mMetaReader = metaReader;
}

template <typename T>
inline void DocFilterProcessorTyped<T>::BeginFilter(int64_t minValue, int64_t maxValue,
                                                    const PostingIteratorPtr& postingIt)
{
    mMinValue = minValue;
    mMaxValue = maxValue;
}

template <>
inline bool DocFilterProcessorTyped<autil::uint128_t>::IsFiltered(docid_t docId)
{
    assert(false);
    return false;
}

template <>
inline bool DocFilterProcessorTyped<float>::IsFiltered(docid_t docId)
{
    assert(false);
    return false;
}

template <>
inline bool DocFilterProcessorTyped<double>::IsFiltered(docid_t docId)
{
    assert(false);
    return false;
}

template <typename T>
inline void DocFilterProcessorTyped<T>::GetFilterRange(int64_t& min, int64_t& max)
{
    min = mMinValue;
    max = mMaxValue;
}

DEFINE_SHARED_PTR(DocFilterProcessor);
} // namespace indexlib::index::legacy
