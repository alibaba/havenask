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
#include "indexlib/index/common/field_format/spatial/QueryStrategy.h"

#include "indexlib/index/common/field_format/spatial/shape/Circle.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, QueryStrategy);

std::vector<dictkey_t> QueryStrategy::GetShapeCoverCells(const std::shared_ptr<Shape>& shape)
{
    assert(shape);
    Shape::ShapeType type = shape->GetType();
    if (type == Shape::ShapeType::POINT) {
        auto point = std::dynamic_pointer_cast<Point>(shape);
        assert(point);
        return GetPointCoveredCells(point);
    } else if (type == Shape::ShapeType::CIRCLE) {
        auto circle = std::dynamic_pointer_cast<Circle>(shape);
        assert(circle);
        auto detailLevel = CalculateDetailSearchLevel(Shape::ShapeType::CIRCLE, circle->GetRadius() * 2);
        auto rectangle = shape->GetBoundingBox();
        assert(rectangle);
        return _shapeCover.GetShapeCoveredCells(rectangle, _maxSearchTerms, detailLevel);
    } else {
        auto rectangle = shape->GetBoundingBox();
        assert(rectangle);
        auto detailLevel = CalculateDetailSearchLevel(type, rectangle->CalculateDiagonalLength());
        return _shapeCover.GetShapeCoveredCells(shape, _maxSearchTerms, detailLevel);
    }
}

} // namespace indexlib::index
