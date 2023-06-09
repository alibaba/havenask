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
#include "indexlib/index/common/field_format/spatial/LocationIndexQueryStrategy.h"

#include "autil/StringUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, LocationIndexQueryStrategy);

LocationIndexQueryStrategy::LocationIndexQueryStrategy(int8_t topLevel, int8_t detailLevel,
                                                       const std::string& indexName, double distanceLoss,
                                                       size_t maxSearchTerms)
    : QueryStrategy(topLevel, detailLevel, indexName, distanceLoss, maxSearchTerms, true)
{
}

int8_t LocationIndexQueryStrategy::CalculateDetailSearchLevel(Shape::ShapeType shapeType, double distance)
{
    int8_t detailLevel = (shapeType == Shape::ShapeType::LINE || shapeType == Shape::ShapeType::POLYGON)
                             ? GeoHashUtil::DistanceToLevel(distance * _distanceLoss)
                             : GeoHashUtil::DistanceToLevel(distance * 1.2) + 1;
    detailLevel = std::min(detailLevel, _detailLevel);
    detailLevel = std::max(detailLevel, _topLevel);
    return detailLevel;
}

std::vector<dictkey_t> LocationIndexQueryStrategy::CalculateTerms(const std::shared_ptr<Shape>& shape)
{
    std::vector<dictkey_t> cells;
    std::vector<dictkey_t> coverCells = GetShapeCoverCells(shape);
    for (auto cell : coverCells) {
        Cell::RemoveLeafTag(cell);
        cells.push_back(cell);
    }

    if (cells.size() > 1000) {
        AUTIL_LOG(DEBUG,
                  "query shape [%s] term num [%lu] too much, "
                  "index level [%u,%u]",
                  shape->ToString().c_str(), cells.size(), _topLevel, _detailLevel);
    }
    return cells;
}

std::vector<uint64_t> LocationIndexQueryStrategy::GetPointCoveredCells(const std::shared_ptr<Point>& point)
{
    return _shapeCover.GetPointCoveredCells(point, true);
}

} // namespace indexlib::index
