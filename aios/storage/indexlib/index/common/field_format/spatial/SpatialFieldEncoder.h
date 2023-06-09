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
#include "indexlib/base/Constant.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/common/field_format/spatial/ShapeIndexQueryStrategy.h"
#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Line.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "indexlib/index/common/field_format/spatial/shape/Polygon.h"
#include "indexlib/index/common/field_format/spatial/shape/ShapeCoverer.h"

namespace indexlib::index {

class SpatialFieldEncoder : private autil::NoCopyable
{
public:
    SpatialFieldEncoder() = default; // for legacy
    SpatialFieldEncoder(const std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& indexConfigs); // for v2;
    ~SpatialFieldEncoder();

public:
    template <typename SpatialIndexConfig>
    void Init(const std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& indexConfigs);

    bool IsSpatialIndexField(fieldid_t fieldId) const;
    void Encode(fieldid_t fieldId, const std::string& fieldValue, std::vector<dictkey_t>& keys);

    template <FieldType fieldType>
    std::vector<dictkey_t> EncodeShape(fieldid_t fieldId, const std::vector<std::string>& shapeStrs);

    int8_t TEST_GetTopLevel(fieldid_t fieldId) const;
    int8_t TEST_GetDetailLevel(fieldid_t fieldId) const;
    double TEST_GetDistanceLoss(fieldid_t fieldId) const;

private:
    // <fieldType, topLevel, detailLevel, distanceLoss>
    std::vector<std::tuple<FieldType, int8_t, int8_t, double>> _infos;

private:
    AUTIL_LOG_DECLARE();
};

template <typename SpatialIndexConfig>
inline void
SpatialFieldEncoder::Init(const std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& indexConfigs)
{
    for (auto& indexConfig : indexConfigs) {
        auto spatialConfig = std::dynamic_pointer_cast<SpatialIndexConfig>(indexConfig);
        if (!spatialConfig) {
            continue;
        }
        fieldid_t fieldId = spatialConfig->GetFieldConfig()->GetFieldId();
        FieldType fieldType = spatialConfig->GetFieldConfig()->GetFieldType();
        assert(fieldType == ft_location || fieldType == ft_line || fieldType == ft_polygon);
        if (fieldId >= _infos.size()) {
            _infos.resize(fieldId + 1, {ft_unknown, -1, -1, SpatialIndexConfig::DEFAULT_DIST_LOSS_ACCURACY});
        }
        auto topLevel = GeoHashUtil::DistanceToLevel(spatialConfig->GetMaxSearchDist());
        auto detailLevel = GeoHashUtil::DistanceToLevel(spatialConfig->GetMaxDistError());
        _infos[fieldId] = {fieldType, topLevel, detailLevel, spatialConfig->GetDistanceLoss()};
    }
}

inline bool SpatialFieldEncoder::IsSpatialIndexField(fieldid_t fieldId) const
{
    if (fieldId >= _infos.size()) {
        return false;
    }
    auto topLevel = std::get<1>(_infos[fieldId]);
    return topLevel != -1;
}

inline void SpatialFieldEncoder::Encode(fieldid_t fieldId, const std::string& fieldValue, std::vector<dictkey_t>& keys)
{
    keys.clear();
    if (!IsSpatialIndexField(fieldId)) {
        return;
    }

    if (fieldValue.empty()) {
        AUTIL_LOG(DEBUG, "field [%d] value is empty.", fieldId);
        return;
    }

    std::vector<std::string> shapeStrs;
    autil::StringUtil::fromString(fieldValue, shapeStrs, INDEXLIB_MULTI_VALUE_SEPARATOR_STR);
    auto fieldType = std::get<0>(_infos[fieldId]);
    switch (fieldType) {
    case ft_location:
        keys = EncodeShape<ft_location>(fieldId, shapeStrs);
        break;
    case ft_line:
        keys = EncodeShape<ft_line>(fieldId, shapeStrs);
        break;
    case ft_polygon:
        keys = EncodeShape<ft_polygon>(fieldId, shapeStrs);
        break;
    default:
        assert(false);
    }
}

template <FieldType fieldType>
std::vector<dictkey_t> SpatialFieldEncoder::EncodeShape(fieldid_t fieldId, const std::vector<std::string>& shapeStrs)
{
    static_assert(fieldType == ft_line || fieldType == ft_location || fieldType == ft_polygon);

    std::vector<dictkey_t> dictkeys;
    auto [_, topLevel, detailLevel, distanceLoss] = _infos[fieldId];
    for (auto& shapeStr : shapeStrs) {
        std::vector<dictkey_t> keys;
        if constexpr (fieldType == ft_location) {
            if (auto point = Point::FromString(shapeStr)) {
                keys = GeoHashUtil::Encode(point->GetX(), point->GetY(), topLevel, detailLevel);
            }
        } else {
            std::shared_ptr<Shape> shape;
            if constexpr (fieldType == ft_line) {
                shape = Line::FromString(shapeStr);
            } else {
                shape = Polygon::FromString(shapeStr);
            }

            if (shape) {
                auto searchLevel =
                    ShapeIndexQueryStrategy::CalculateDetailSearchLevel(shape, distanceLoss, topLevel, detailLevel);
                ShapeCoverer shapeCoverer(topLevel, detailLevel, false);
                keys = shapeCoverer.GetShapeCoveredCells(shape, 30000, searchLevel);
            }
        }
        dictkeys.insert(dictkeys.end(), keys.begin(), keys.end());
    }
    return dictkeys;
}

} // namespace indexlib::index
