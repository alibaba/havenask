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

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/attribute/format/AttributeFormatter.h"
#include "indexlib/index/attribute/format/SingleValueNullAttributeFormatter.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/field_format/pack_attribute/FloatCompressConvertor.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeFormatter : public AttributeFormatter
{
public:
    explicit SingleValueAttributeFormatter(indexlib::config::CompressTypeOption compressType, bool supportNull)
    {
        Init(compressType, supportNull);
    }

    virtual ~SingleValueAttributeFormatter() {}

public:
    uint32_t GetRecordSize() const { return _recordSize; }
    docid_t AlignDocId(docid_t docId) const;
    bool IsSupportNull() const { return _supportNull; }

private:
    void Init(indexlib::config::CompressTypeOption compressType, bool supportNull);

protected:
    uint32_t _recordSize;
    indexlib::config::CompressTypeOption _compressType;
    bool _supportNull;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeFormatter, T);

template <typename T>
inline void SingleValueAttributeFormatter<T>::Init(indexlib::config::CompressTypeOption compressType, bool supportNull)
{
    _recordSize = sizeof(T);
    _compressType = compressType;
    _supportNull = supportNull;
}

template <>
inline void SingleValueAttributeFormatter<float>::Init(indexlib::config::CompressTypeOption compressType,
                                                       bool supportNull)
{
    _compressType = compressType;
    _recordSize = FloatCompressConvertor::GetSingleValueCompressLen(_compressType);
    _supportNull = supportNull;
}

template <typename T>
inline docid_t SingleValueAttributeFormatter<T>::AlignDocId(docid_t docId) const
{
    if (!_supportNull) {
        return docId;
    }
    return docId / SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE * SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE;
}

} // namespace indexlibv2::index
