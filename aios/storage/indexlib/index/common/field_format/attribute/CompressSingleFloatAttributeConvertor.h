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

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/index/common/field_format/attribute/SingleValueAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"

namespace indexlibv2::index {
template <typename T>
class CompressSingleFloatAttributeConvertor : public SingleValueAttributeConvertor<T>
{
public:
    CompressSingleFloatAttributeConvertor(indexlib::config::CompressTypeOption compressType,
                                          const std::string& fieldName)
        : SingleValueAttributeConvertor<T>(/*needHash*/ false, fieldName)
        , _compressType(compressType)
    {
    }

    ~CompressSingleFloatAttributeConvertor() = default;

private:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& resultStr, char* outBuffer,
                                  AttributeConvertor::EncodeStatus& status) override;

private:
    indexlib::config::CompressTypeOption _compressType;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, CompressSingleFloatAttributeConvertor, T);
template <typename T>
inline autil::StringView CompressSingleFloatAttributeConvertor<T>::InnerEncode(const autil::StringView& attrData,
                                                                               autil::mem_pool::Pool* memPool,
                                                                               std::string& resultStr, char* outBuffer,
                                                                               AttributeConvertor::EncodeStatus& status)
{
    float value;
    if (!StrToT(attrData, value)) {
        status = AttributeConvertor::EncodeStatus::ES_TYPE_ERROR;
        value = (float)0.0;
        AUTIL_LOG(DEBUG, "convert attribute[%s] error value:[%s]", this->_fieldName.c_str(), attrData.data());
        ERROR_COLLECTOR_LOG(ERROR, "convert attribute[%s] error value:[%s]", this->_fieldName.c_str(), attrData.data());
    }

    T* output = (T*)(this->Allocate(memPool, resultStr, outBuffer, sizeof(T)));
    if (_compressType.HasFp16EncodeCompress()) {
        indexlib::util::Fp16Encoder::Encode(value, (char*)output);
    } else if (_compressType.HasInt8EncodeCompress()) {
        indexlib::util::FloatInt8Encoder::Encode(_compressType.GetInt8AbsMax(), value, (char*)output);
    } else {
        AUTIL_LOG(ERROR, "invalid compressType[%s] for field[%s]", this->_compressType.GetCompressStr().c_str(),
                  this->_fieldName.c_str());
        ERROR_COLLECTOR_LOG(ERROR, "invalid compressType[%s] for field[%s]",
                            this->_compressType.GetCompressStr().c_str(), this->_fieldName.c_str());
        status = AttributeConvertor::EncodeStatus::ES_TYPE_ERROR;
    }
    return autil::StringView((char*)output, sizeof(T));
}
} // namespace indexlibv2::index
