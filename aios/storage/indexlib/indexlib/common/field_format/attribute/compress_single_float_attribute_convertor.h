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

#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"

namespace indexlib { namespace common {

template <typename T>
class CompressSingleFloatAttributeConvertor : public SingleValueAttributeConvertor<T>
{
public:
    CompressSingleFloatAttributeConvertor(config::CompressTypeOption compressType, const std::string& fieldName)
        : SingleValueAttributeConvertor<T>(false, fieldName)
        , mCompressType(compressType)
    {
    }

    virtual ~CompressSingleFloatAttributeConvertor() {}

private:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& resultStr, char* outBuffer,
                                  AttributeConvertor::EncodeStatus& status) override;

private:
    config::CompressTypeOption mCompressType;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(common, CompressSingleFloatAttributeConvertor);
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
        IE_LOG(DEBUG, "convert attribute[%s] error value:[%s]", this->mFieldName.c_str(), attrData.data());
        ERROR_COLLECTOR_LOG(ERROR, "convert attribute[%s] error value:[%s]", this->mFieldName.c_str(), attrData.data());
    }

    T* output = (T*)(this->allocate(memPool, resultStr, outBuffer, sizeof(T)));
    if (mCompressType.HasFp16EncodeCompress()) {
        util::Fp16Encoder::Encode(value, (char*)output);
    } else if (mCompressType.HasInt8EncodeCompress()) {
        util::FloatInt8Encoder::Encode(mCompressType.GetInt8AbsMax(), value, (char*)output);
    } else {
        IE_LOG(ERROR, "invalid compressType[%s] for field[%s]", this->mCompressType.GetCompressStr().c_str(),
               this->mFieldName.c_str());
        ERROR_COLLECTOR_LOG(ERROR, "invalid compressType[%s] for field[%s]",
                            this->mCompressType.GetCompressStr().c_str(), this->mFieldName.c_str());
        status = AttributeConvertor::EncodeStatus::ES_TYPE_ERROR;
    }
    return autil::StringView((char*)output, sizeof(T));
}
}} // namespace indexlib::common
