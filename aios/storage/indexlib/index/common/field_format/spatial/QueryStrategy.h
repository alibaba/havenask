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
#include "indexlib/base/Types.h"
#include "indexlib/index/common/field_format/spatial/shape/ShapeCoverer.h"
namespace indexlib::index {

class QueryStrategy : private autil::NoCopyable
{
public:
    QueryStrategy(int8_t topLevel, int8_t detailLevel, const std::string& indexName, double distanceLoss,
                  size_t maxSearchTerms, bool shapeCoverOnlyGetLeafCell)
        : _indexName(indexName)
        , _maxSearchTerms(maxSearchTerms)
        , _shapeCover(topLevel, detailLevel, shapeCoverOnlyGetLeafCell)
        , _topLevel(topLevel)
        , _detailLevel(detailLevel)
        , _distanceLoss(distanceLoss)
    {
    }
    virtual ~QueryStrategy() = default;

public:
    virtual std::vector<dictkey_t> CalculateTerms(const std::shared_ptr<Shape>& shape) = 0;

protected:
    virtual std::vector<dictkey_t> GetPointCoveredCells(const std::shared_ptr<Point>& point) = 0;
    virtual int8_t CalculateDetailSearchLevel(Shape::ShapeType type, double distance) = 0;
    std::vector<dictkey_t> GetShapeCoverCells(const std::shared_ptr<Shape>& shape);

protected:
    std::string _indexName;
    size_t _maxSearchTerms;
    ShapeCoverer _shapeCover;
    int8_t _topLevel;
    int8_t _detailLevel;
    double _distanceLoss;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
