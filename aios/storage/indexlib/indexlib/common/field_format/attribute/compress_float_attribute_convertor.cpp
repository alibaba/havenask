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
#include "indexlib/common/field_format/attribute/compress_float_attribute_convertor.h"

#include "indexlib/util/BlockFpEncoder.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, CompressFloatAttributeConvertor);

CompressFloatAttributeConvertor::CompressFloatAttributeConvertor(CompressTypeOption compressType, bool needHash,
                                                                 const string& fieldName, int32_t fixedValueCount,
                                                                 const std::string& seperator)
    : VarNumAttributeConvertor(needHash, fieldName, fixedValueCount, seperator)
    , mCompressType(compressType)
{
    assert(fixedValueCount != -1);
    assert(mCompressType.HasFp16EncodeCompress() || mCompressType.HasBlockFpEncodeCompress() ||
           mCompressType.HasInt8EncodeCompress());
}

CompressFloatAttributeConvertor::~CompressFloatAttributeConvertor() {}

StringView CompressFloatAttributeConvertor::InnerEncode(const StringView& originalValue, const vector<StringView>& vec,
                                                        Pool* memPool, string& resultStr, char* outBuffer,
                                                        EncodeStatus& status)
{
    size_t tokenNum = vec.size();
    tokenNum = AdjustCount(tokenNum);

    size_t bufSize = sizeof(uint64_t); // hash value
    if (mCompressType.HasBlockFpEncodeCompress()) {
        bufSize += BlockFpEncoder::GetEncodeBytesLen(tokenNum);
    } else if (mCompressType.HasFp16EncodeCompress()) {
        bufSize += Fp16Encoder::GetEncodeBytesLen(tokenNum);
    } else if (mCompressType.HasInt8EncodeCompress()) {
        bufSize += FloatInt8Encoder::GetEncodeBytesLen(tokenNum);
    }

    char* begin = (char*)allocate(memPool, resultStr, outBuffer, bufSize);
    char* buffer = begin;
    AppendHashAndCount(originalValue, (uint32_t)tokenNum, buffer);

    vector<float> floatVec;
    floatVec.resize(tokenNum, float());
    for (size_t i = 0; i < tokenNum; ++i) {
        float& value = floatVec[i];
        if (!StrToT(vec[i], value)) {
            status = ES_TYPE_ERROR;
            IE_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]", mFieldName.c_str(), originalValue.data());
            ERROR_COLLECTOR_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]", mFieldName.c_str(),
                                originalValue.data());
        }
    }

    size_t encodeLen = bufSize - (buffer - begin);
    int32_t compLen = -1;
    if (mCompressType.HasBlockFpEncodeCompress()) {
        compLen = BlockFpEncoder::Encode((const float*)floatVec.data(), floatVec.size(), buffer, encodeLen);
    } else if (mCompressType.HasFp16EncodeCompress()) {
        compLen = Fp16Encoder::Encode((const float*)floatVec.data(), floatVec.size(), buffer, encodeLen);
    } else if (mCompressType.HasInt8EncodeCompress()) {
        compLen = FloatInt8Encoder::Encode(mCompressType.GetInt8AbsMax(), (const float*)floatVec.data(),
                                           floatVec.size(), buffer, encodeLen);
    }

    if (compLen == -1) {
        ERROR_COLLECTOR_LOG(ERROR,
                            "convert attribute [%s] error, multi_value:[%s], "
                            "compress type[%s]",
                            mFieldName.c_str(), originalValue.data(), mCompressType.GetCompressStr().c_str());
        return StringView::empty_instance();
    }
    assert((size_t)(compLen + (buffer - begin)) == bufSize);
    return StringView(begin, bufSize);
}

bool CompressFloatAttributeConvertor::DecodeLiteralField(const autil::StringView& str, std::string& value)
{
    uint32_t count = 0;
    const char* addr = nullptr;
    autil::StringView data = Decode(str).data;
    if (mFixedValueCount != -1) {
        addr = data.data();
        count = mFixedValueCount;
    } else {
        size_t countLen = 0;
        bool isNull = false;
        count = VarNumAttributeFormatter::DecodeCount(data.data(), countLen, isNull);
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
    if (mCompressType.HasBlockFpEncodeCompress()) {
        ret = BlockFpEncoder::Decode(data, (char*)tmp.data(), tmp.size() * sizeof(float));
    } else if (mCompressType.HasFp16EncodeCompress()) {
        ret = Fp16Encoder::Decode(data, (char*)tmp.data(), tmp.size() * sizeof(float));
    } else if (mCompressType.HasInt8EncodeCompress()) {
        ret = FloatInt8Encoder::Decode(mCompressType.GetInt8AbsMax(), data, (char*)tmp.data(),
                                       tmp.size() * sizeof(float));
    }

    if (ret != (int32_t)tmp.size()) {
        return false;
    }
    value = autil::StringUtil::toString(tmp, "");
    return true;
}
}} // namespace indexlib::common
