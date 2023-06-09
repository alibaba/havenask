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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/attribute/format/SingleEncodedNullValue.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/field_format/pack_attribute/FloatCompressConvertor.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueNullAttributeFormatter : private autil::NoCopyable
{
public:
    SingleValueNullAttributeFormatter() : _recordSize(sizeof(T)), _groupSize(0) {}
    ~SingleValueNullAttributeFormatter() = default;

public:
    static uint32_t GetDataLen(int64_t docCount);

public:
    void Init(indexlib::config::CompressTypeOption compressType);
    uint32_t GetRecordSize() const { return _recordSize; }
    uint32_t GetDocCount(int64_t dataLength) const;
    T GetEncodedNullValue() const { return _encodedNullValue; }

protected:
    uint32_t _recordSize;
    uint32_t _groupSize;
    indexlib::config::CompressTypeOption _compressType;
    T _encodedNullValue;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueNullAttributeFormatter, T);
template <typename T>
inline void SingleValueNullAttributeFormatter<T>::Init(indexlib::config::CompressTypeOption compressType)
{
    _compressType = compressType;
    _groupSize =
        _recordSize * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE + SingleEncodedNullValue::BITMAP_HEADER_SIZE;
    SingleEncodedNullValue::GetEncodedValue<T>((void*)&_encodedNullValue);
}

template <>
inline void SingleValueNullAttributeFormatter<float>::Init(indexlib::config::CompressTypeOption compressType)
{
    _compressType = compressType;
    _recordSize = FloatCompressConvertor::GetSingleValueCompressLen(_compressType);
    _groupSize =
        _recordSize * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE + SingleEncodedNullValue::BITMAP_HEADER_SIZE;
    SingleEncodedNullValue::GetEncodedValue<float>((void*)&_encodedNullValue);
}

template <typename T>
inline uint32_t SingleValueNullAttributeFormatter<T>::GetDocCount(int64_t dataLength) const
{
    int32_t groupCount = dataLength / _groupSize;
    int64_t remainSize = dataLength - groupCount * _groupSize;
    assert(remainSize < _groupSize);
    remainSize = remainSize > 0 ? remainSize - SingleEncodedNullValue::BITMAP_HEADER_SIZE : remainSize;
    return SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE * groupCount + remainSize / _recordSize;
}

template <typename T>
inline uint32_t SingleValueNullAttributeFormatter<T>::GetDataLen(int64_t docCount)
{
    int32_t groupCount = docCount / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    int64_t remain = docCount - groupCount * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
    if (remain > 0) {
        return docCount * sizeof(T) + (groupCount + 1) * sizeof(uint64_t);
    }
    return docCount * sizeof(T) + groupCount * sizeof(uint64_t);
}

} // namespace indexlibv2::index
