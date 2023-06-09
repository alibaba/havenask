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

#include "indexlib/index/inverted_index/truncate/IDocFilterProcessor.h"

namespace indexlib::index {

template <typename T>
class DocFilterProcessorTyped final : public IDocFilterProcessor
{
public:
    DocFilterProcessorTyped(const std::shared_ptr<TruncateAttributeReader>& attrReader,
                            const indexlibv2::config::DiversityConstrain& constrain);
    ~DocFilterProcessorTyped();

public:
    bool BeginFilter(const DictKeyInfo& key, const std::shared_ptr<PostingIterator>& postingIt) override;
    bool IsFiltered(docid_t docId) override;
    void SetTruncateMetaReader(const std::shared_ptr<TruncateMetaReader>& metaReader) override;

private:
    std::shared_ptr<TruncateAttributeReader> _attrReader;
    std::shared_ptr<TruncateMetaReader> _metaReader;
    int64_t _minValue;
    int64_t _maxValue;
    uint64_t _mask;
    std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase> _readCtx;
    autil::mem_pool::UnsafePool _pool;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
DocFilterProcessorTyped<T>::DocFilterProcessorTyped(const std::shared_ptr<TruncateAttributeReader>& attrReader,
                                                    const indexlibv2::config::DiversityConstrain& constrain)
{
    _maxValue = constrain.GetFilterMaxValue();
    _minValue = constrain.GetFilterMinValue();
    _mask = constrain.GetFilterMask();
    _attrReader = attrReader;
    _readCtx = _attrReader->CreateReadContextPtr(&_pool);
}

template <typename T>
DocFilterProcessorTyped<T>::~DocFilterProcessorTyped()
{
    _readCtx.reset();
}

template <typename T>
inline bool DocFilterProcessorTyped<T>::BeginFilter(const DictKeyInfo& key,
                                                    const std::shared_ptr<PostingIterator>& postingIt)
{
    if (_metaReader && !_metaReader->Lookup(key, _minValue, _maxValue)) {
        return false;
    }
    return true;
}

template <typename T>
inline bool DocFilterProcessorTyped<T>::IsFiltered(docid_t docId)
{
    T value {};
    bool isNull = false;
    uint32_t dataLen = 0;
    _attrReader->Read(docId, _readCtx, (uint8_t*)&value, sizeof(T), dataLen, isNull);
    if (isNull) {
        // null value will not match the section [min, max]
        return true;
    }
    value &= (T)_mask;
    return !(_minValue <= (int64_t)value && (int64_t)value <= _maxValue);
}

template <typename T>
inline void DocFilterProcessorTyped<T>::SetTruncateMetaReader(const std::shared_ptr<TruncateMetaReader>& metaReader)
{
    _metaReader = metaReader;
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

} // namespace indexlib::index
