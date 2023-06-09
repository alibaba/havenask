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
#include "autil/NoCopyable.h"
#include "autil/StringTokenizer.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/spatial/shape/Line.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "indexlib/index/common/field_format/spatial/shape/Polygon.h"
#include "indexlib/util/ErrorLogCollector.h"

namespace indexlibv2::index {

class ShapeAttributeUtil : private autil::NoCopyable
{
public:
    ShapeAttributeUtil() = default;
    ~ShapeAttributeUtil() = default;

public:
    template <typename T>
    static bool EncodeShape(const autil::StringView& attrData, indexlib::index::Shape::ShapeType shapeType,
                            const std::string& fieldName, const std::string& separator,
                            AttributeConvertor::EncodeStatus& status, std::vector<double>& encodeVec);
    static bool DecodeAttrValueToString(indexlib::index::Shape::ShapeType shapeType,
                                        const autil::MultiValueType<double>& value, const std::string& separator,
                                        std::string& attrValue);

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////
inline bool ShapeAttributeUtil::DecodeAttrValueToString(indexlib::index::Shape::ShapeType shapeType,
                                                        const autil::MultiValueType<double>& value,
                                                        const std::string& separator, std::string& attrValue)
{
    if (shapeType != indexlib::index::Shape::ShapeType::LINE &&
        shapeType != indexlib::index::Shape::ShapeType::POLYGON) {
        return false;
    }

    attrValue.clear();
    uint32_t size = value.size();
    if (0 == size) {
        return true;
    }

    size_t cursor = 0;
    while (true) {
        size_t pointNum = (size_t)value[cursor];
        size_t valueNum = 2 * pointNum;
        if (unlikely(cursor + valueNum >= size)) {
            return false;
        }
        cursor++;
        for (size_t i = 0; i < valueNum; i++) {
            std::string item = autil::StringUtil::toString<double>(value[cursor]);
            cursor++;
            if (i != 0) {
                if (i & 1) {
                    attrValue += " ";
                } else {
                    attrValue += ",";
                }
            }
            attrValue += item;
        }

        if (cursor >= size) {
            break;
        }
        attrValue += separator;
    }

    return true;
}

template <typename T>
inline bool ShapeAttributeUtil::EncodeShape(const autil::StringView& attrData,
                                            indexlib::index::Shape::ShapeType shapeType, const std::string& fieldName,
                                            const std::string& separator, AttributeConvertor::EncodeStatus& status,
                                            std::vector<double>& encodeVec)
{
    encodeVec.clear();
    if (shapeType != indexlib::index::Shape::ShapeType::POLYGON &&
        shapeType != indexlib::index::Shape::ShapeType::LINE) {
        ERROR_COLLECTOR_LOG(DEBUG, "not suppoerted shape of field [%s]", fieldName.c_str());
        status = AttributeConvertor::EncodeStatus::ES_TYPE_ERROR;
        return false;
    }

    std::vector<autil::StringView> vec = autil::StringTokenizer::constTokenize(
        attrData, separator, autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    for (size_t i = 0; i < vec.size(); i++) {
        auto shape = T::FromString(vec[i]);
        if (shape) {
            const std::vector<indexlib::index::Point>& points = shape->GetPoints();
            encodeVec.push_back(points.size());
            for (const auto& point : points) {
                encodeVec.push_back(point.GetX());
                encodeVec.push_back(point.GetY());
            }
        } else {
            status = AttributeConvertor::EncodeStatus::ES_TYPE_ERROR;
            ERROR_COLLECTOR_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]", fieldName.c_str(),
                                attrData.data());
            return false;
        }
    }
    return true;
}
} // namespace indexlibv2::index
