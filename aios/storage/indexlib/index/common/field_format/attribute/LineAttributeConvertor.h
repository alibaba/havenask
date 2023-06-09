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
#include "indexlib/index/common/field_format/attribute/LocationAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/ShapeAttributeUtil.h"

namespace indexlibv2::index {

class LineAttributeConvertor : public LocationAttributeConvertor
{
public:
    LineAttributeConvertor() : LineAttributeConvertor(false, "", INDEXLIB_MULTI_VALUE_SEPARATOR_STR) {}
    LineAttributeConvertor(bool needHash, const std::string& fieldName, const std::string& separator)
        : LocationAttributeConvertor(needHash, fieldName, separator)
    {
    }
    ~LineAttributeConvertor() = default;

private:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& resultStr, char* outBuffer, EncodeStatus& status) override;

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////
inline autil::StringView LineAttributeConvertor::InnerEncode(const autil::StringView& attrData,
                                                             autil::mem_pool::Pool* memPool, std::string& resultStr,
                                                             char* outBuffer, EncodeStatus& status)
{
    std::vector<double> encodeVec;
    bool result = ShapeAttributeUtil::EncodeShape<indexlib::index::Line>(
        attrData, indexlib::index::Shape::ShapeType::LINE, _fieldName, _separator, status, encodeVec);
    if (!result) {
        return autil::StringView();
    }
    return LocationAttributeConvertor::InnerEncodeVec(attrData, encodeVec, memPool, resultStr, outBuffer);
}
} // namespace indexlibv2::index
