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
#include "indexlib/index/common/field_format/spatial/QueryStrategy.h"

namespace indexlib::index {

class ShapeIndexQueryStrategy : public QueryStrategy
{
public:
    ShapeIndexQueryStrategy(int8_t topLevel, int8_t detailLevel, const std::string& indexName, double distanceLoss,
                            size_t maxSearchTerms);
    ~ShapeIndexQueryStrategy() = default;

public:
    static int8_t CalculateDetailSearchLevel(const std::shared_ptr<Shape>& shape, double distanceLoss, int8_t topLevel,
                                             int8_t detailLevel);
    int8_t CalculateDetailSearchLevel(Shape::ShapeType type, double distance) override;
    std::vector<dictkey_t> CalculateTerms(const std::shared_ptr<Shape>& shape) override;
    std::vector<dictkey_t> GetPointCoveredCells(const std::shared_ptr<Point>& point) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
