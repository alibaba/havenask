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
#include "indexlib/index/common/field_format/spatial/ShapeIndexQueryStrategy.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, ShapeIndexQueryStrategy);

ShapeIndexQueryStrategy::ShapeIndexQueryStrategy(int8_t topLevel, int8_t detailLevel, const std::string& indexName,
                                                 double distanceLoss, size_t maxSearchTerms)
    : QueryStrategy(topLevel, detailLevel, indexName, distanceLoss, maxSearchTerms, false)
{
}

int8_t ShapeIndexQueryStrategy::CalculateDetailSearchLevel(Shape::ShapeType type, double distance)
{
    assert(_detailLevel > 0 && _topLevel > 0);
    auto detailLevel = GeoHashUtil::DistanceToLevel(distance * _distanceLoss);
    detailLevel = std::min(detailLevel, _detailLevel);
    detailLevel = std::max(detailLevel, _topLevel);
    return detailLevel;
}

int8_t ShapeIndexQueryStrategy::CalculateDetailSearchLevel(const std::shared_ptr<Shape>& shape, double distanceLoss,
                                                           int8_t topLevel, int8_t detailLevel)
{
    double distance = shape->GetBoundingBox()->CalculateDiagonalLength();
    auto searchLevel = GeoHashUtil::DistanceToLevel(distance * distanceLoss);
    searchLevel = std::min(searchLevel, detailLevel);
    searchLevel = std::max(searchLevel, topLevel);
    return searchLevel;
}

std::vector<dictkey_t> ShapeIndexQueryStrategy::CalculateTerms(const std::shared_ptr<Shape>& shape)
{
    assert(shape);

    std::vector<dictkey_t> cells;
    std::vector<uint64_t> coverCells = GetShapeCoverCells(shape);

    for (auto cell : coverCells) {
        if (Cell::IsLeafCell(cell)) {
            cells.push_back(cell);
            Cell::RemoveLeafTag(cell);
            cells.push_back(cell);
        } else {
            Cell::SetLeafTag(cell);
            cells.push_back(cell);
        }
    }

    if (cells.size() > 1000) {
        AUTIL_LOG(DEBUG,
                  "query shape [%s] term num [%lu] too much, "
                  "index level [%u,%u]",
                  shape->ToString().c_str(), cells.size(), _topLevel, _detailLevel);
    }
    return cells;
}

std::vector<dictkey_t> ShapeIndexQueryStrategy::GetPointCoveredCells(const std::shared_ptr<Point>& point)
{
    return _shapeCover.GetPointCoveredCells(point, false);
}

} // namespace indexlib::index
