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
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"

namespace indexlibv2::index {

class LocationAttributeConvertor : public MultiValueAttributeConvertor<double>
{
public:
    LocationAttributeConvertor() : LocationAttributeConvertor(false, "", INDEXLIB_MULTI_VALUE_SEPARATOR_STR) {}
    LocationAttributeConvertor(bool needHash, const std::string& fieldName, const std::string& separator)
        : MultiValueAttributeConvertor<double>(needHash, fieldName, /*fixedValueCount*/ -1, separator)
    {
    }
    ~LocationAttributeConvertor() = default;

protected:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& resultStr, char* outBuffer, EncodeStatus& status) override;

    autil::StringView InnerEncodeVec(const autil::StringView& originalValue, const std::vector<double>& vec,
                                     autil::mem_pool::Pool* memPool, std::string& resultStr, char* outBuffer);

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////
inline autil::StringView LocationAttributeConvertor::InnerEncode(const autil::StringView& attrData,
                                                                 autil::mem_pool::Pool* memPool, std::string& resultStr,
                                                                 char* outBuffer, EncodeStatus& status)
{
    std::vector<double> encodeVec;
    std::vector<autil::StringView> vec = autil::StringTokenizer::constTokenize(
        attrData, _separator, autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (size_t i = 0; i < vec.size(); i++) {
        std::shared_ptr<indexlib::index::Point> point = indexlib::index::Point::FromString(vec[i]);
        if (point) {
            encodeVec.push_back(point->GetX());
            encodeVec.push_back(point->GetY());
        } else {
            status = EncodeStatus::ES_TYPE_ERROR;
            ERROR_COLLECTOR_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]", _fieldName.c_str(),
                                attrData.data());
        }
    }
    return InnerEncodeVec(attrData, encodeVec, memPool, resultStr, outBuffer);
}

inline autil::StringView LocationAttributeConvertor::InnerEncodeVec(const autil::StringView& originalValue,
                                                                    const std::vector<double>& vec,
                                                                    autil::mem_pool::Pool* memPool,
                                                                    std::string& resultStr, char* outBuffer)
{
    size_t tokenNum = vec.size();
    tokenNum = AdjustCount(tokenNum);
    size_t bufSize = GetBufferSize(tokenNum);
    char* begin = (char*)Allocate(memPool, resultStr, outBuffer, bufSize);

    char* buffer = begin;
    AppendHashAndCount(originalValue, (uint32_t)tokenNum, buffer);
    double* resultBuf = (double*)buffer;
    for (size_t i = 0; i < tokenNum; ++i) {
        double& value = *(resultBuf++);
        value = vec[i];
    }
    assert(size_t(((char*)resultBuf) - begin) == bufSize);
    return autil::StringView(begin, bufSize);
}
} // namespace indexlibv2::index
