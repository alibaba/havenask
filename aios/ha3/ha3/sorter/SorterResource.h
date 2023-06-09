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

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/AttributeItem.h"
#include "ha3/search/SearchPluginResource.h"

namespace isearch {
namespace rank {
class ComparatorCreator;
}  // namespace rank
namespace search {
class SortExpressionCreator;
}  // namespace search
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeExpression;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace sorter {

enum SorterLocation {
    SL_UNKNOWN = 1,
    SL_SEARCHER = 2,
    SL_SEARCHCACHEMERGER = 4,
    SL_PROXY = 8,
    SL_QRS = 16
};

SorterLocation transSorterLocation(const std::string &location);


class SorterResource : public search::SearchPluginResource
{
public:
    SorterResource()
        : scoreExpression(NULL)
        , resultSourceNum(1)
        , requiredTopk(NULL)
        , comparatorCreator(NULL)
        , sortExprCreator(NULL)
        , location(SL_UNKNOWN)
        , globalVariables(NULL)
    {
    }
public:
    suez::turing::AttributeExpression *scoreExpression = nullptr;
    uint32_t resultSourceNum;
    uint32_t *requiredTopk = nullptr;
    rank::ComparatorCreator *comparatorCreator = nullptr;
    search::SortExpressionCreator *sortExprCreator = nullptr;
    SorterLocation location;
    const std::vector<common::AttributeItemMapPtr> *globalVariables = nullptr;
};

typedef std::shared_ptr<SorterResource> SorterResourcePtr;

} // namespace sorter
} // namespace isearch
