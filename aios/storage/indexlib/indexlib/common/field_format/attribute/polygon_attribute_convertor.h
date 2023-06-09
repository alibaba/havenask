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
#ifndef __INDEXLIB_POLYGON_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_POLYGON_ATTRIBUTE_CONVERTOR_H

#include <memory>

#include "indexlib/common/field_format/attribute/location_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/shape_attribute_util.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

class PolygonAttributeConvertor : public LocationAttributeConvertor
{
public:
    PolygonAttributeConvertor(bool needHash = false, const std::string& fieldName = "",
                              const std::string& separator = INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
        : LocationAttributeConvertor(needHash, fieldName, separator)
    {
    }
    ~PolygonAttributeConvertor() {}

private:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& resultStr, char* outBuffer, EncodeStatus& status) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PolygonAttributeConvertor);

/////////////////////////////////////////////////////////////////
autil::StringView PolygonAttributeConvertor::InnerEncode(const autil::StringView& attrData,
                                                         autil::mem_pool::Pool* memPool, std::string& resultStr,
                                                         char* outBuffer, EncodeStatus& status)
{
    std::vector<double> encodeVec;
    bool result = ShapeAttributeUtil::EncodeShape<index::Polygon>(attrData, index::Shape::ShapeType::POLYGON,
                                                                  mFieldName, mSeparator, status, encodeVec);
    if (!result) {
        return autil::StringView();
    }
    return LocationAttributeConvertor::InnerEncodeVec(attrData, encodeVec, memPool, resultStr, outBuffer);
}
}} // namespace indexlib::common

#endif //__INDEXLIB_POLYGON_ATTRIBUTE_CONVERTOR_H
