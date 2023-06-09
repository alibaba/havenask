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
#include "indexlib/index/common/field_format/attribute/CompressFloatAttributeConvertor.h"

#include "indexlib/index/common/field_format/attribute/TypeInfo.h"
#include "indexlib/util/BlockFpEncoder.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"

using namespace indexlib::util;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, CompressFloatAttributeConvertor);

CompressFloatAttributeConvertor::CompressFloatAttributeConvertor(indexlib::config::CompressTypeOption compressType,
                                                                 bool needHash, const std::string& fieldName,
                                                                 int32_t fixedValueCount, const std::string& seperator)
    : MultiValueAttributeConvertor<float>(needHash, fieldName, fixedValueCount, seperator)
    , _compressType(compressType)
{
    assert(fixedValueCount != -1);
    assert(_compressType.HasFp16EncodeCompress() || _compressType.HasBlockFpEncodeCompress() ||
           _compressType.HasInt8EncodeCompress());
}

autil::StringView CompressFloatAttributeConvertor::InnerEncode(const autil::StringView& originalValue,
                                                               const std::vector<autil::StringView>& vec,
                                                               autil::mem_pool::Pool* memPool, std::string& resultStr,
                                                               char* outBuffer, EncodeStatus& status)
{
    size_t tokenNum = vec.size();
    tokenNum = AdjustCount(tokenNum);

    size_t bufSize = sizeof(uint64_t); // hash value
    if (_compressType.HasBlockFpEncodeCompress()) {
        bufSize += BlockFpEncoder::GetEncodeBytesLen(tokenNum);
    } else if (_compressType.HasFp16EncodeCompress()) {
        bufSize += Fp16Encoder::GetEncodeBytesLen(tokenNum);
    } else if (_compressType.HasInt8EncodeCompress()) {
        bufSize += FloatInt8Encoder::GetEncodeBytesLen(tokenNum);
    }

    char* begin = (char*)Allocate(memPool, resultStr, outBuffer, bufSize);
    char* buffer = begin;
    AppendHashAndCount(originalValue, (uint32_t)tokenNum, buffer);

    std::vector<float> floatVec;
    floatVec.resize(tokenNum, float());
    for (size_t i = 0; i < tokenNum; ++i) {
        float& value = floatVec[i];
        if (!StrToT(vec[i], value)) {
            status = EncodeStatus::ES_TYPE_ERROR;
            AUTIL_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]", _fieldName.c_str(), originalValue.data());
            ERROR_COLLECTOR_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]", _fieldName.c_str(),
                                originalValue.data());
        }
    }

    size_t encodeLen = bufSize - (buffer - begin);
    int32_t compLen = -1;
    if (_compressType.HasBlockFpEncodeCompress()) {
        compLen = BlockFpEncoder::Encode((const float*)floatVec.data(), floatVec.size(), buffer, encodeLen);
    } else if (_compressType.HasFp16EncodeCompress()) {
        compLen = Fp16Encoder::Encode((const float*)floatVec.data(), floatVec.size(), buffer, encodeLen);
    } else if (_compressType.HasInt8EncodeCompress()) {
        compLen = FloatInt8Encoder::Encode(_compressType.GetInt8AbsMax(), (const float*)floatVec.data(),
                                           floatVec.size(), buffer, encodeLen);
    }

    if (compLen == -1) {
        ERROR_COLLECTOR_LOG(ERROR,
                            "convert attribute [%s] error, multi_value:[%s], "
                            "compress type[%s]",
                            _fieldName.c_str(), originalValue.data(), _compressType.GetCompressStr().c_str());
        return autil::StringView::empty_instance();
    }
    assert((size_t)(compLen + (buffer - begin)) == bufSize);
    return autil::StringView(begin, bufSize);
}

bool CompressFloatAttributeConvertor::DecodeLiteralField(const autil::StringView& str, std::string& value)
{
    uint32_t count = 0;
    const char* addr = nullptr;
    autil::StringView data = Decode(str).data;
    if (_fixedValueCount != -1) {
        addr = data.data();
        count = _fixedValueCount;
    } else {
        size_t countLen = 0;
        bool isNull = false;
        count = MultiValueAttributeFormatter::DecodeCount(data.data(), countLen, isNull);
        if (isNull) {
            value = std::string("");
            return true;
        }
        addr = (const char*)(data.data() + countLen);
        data = {addr, data.size() - countLen};
    }

    int32_t ret = -1;
    std::vector<float> tmp;
    tmp.resize(count);
    if (_compressType.HasBlockFpEncodeCompress()) {
        ret = BlockFpEncoder::Decode(data, (char*)tmp.data(), tmp.size() * sizeof(float));
    } else if (_compressType.HasFp16EncodeCompress()) {
        ret = Fp16Encoder::Decode(data, (char*)tmp.data(), tmp.size() * sizeof(float));
    } else if (_compressType.HasInt8EncodeCompress()) {
        ret = FloatInt8Encoder::Decode(_compressType.GetInt8AbsMax(), data, (char*)tmp.data(),
                                       tmp.size() * sizeof(float));
    }

    if (ret != (int32_t)tmp.size()) {
        return false;
    }
    value = autil::StringUtil::toString(tmp, "");
    return true;
}
} // namespace indexlibv2::index
