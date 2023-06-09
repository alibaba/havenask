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

#include "ha3/rank/ComparatorCreator.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SortExpressionCreator.h"

namespace isearch {
namespace search {
struct DocCountLimits;
}  // namespace search
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeExpression;
class SorterManager;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace rank {
class RankProfile;
class ScoringProvider;

} // namespace rank
} // namespace isearch

namespace ha3 {
class ScorerProvider;
}

namespace isearch {
namespace search {

struct SearchProcessorResource {
    const rank::RankProfile *rankProfile;
    const suez::turing::SorterManager *sorterManager;
    SortExpressionCreator *sortExpressionCreator;
    rank::ComparatorCreator *comparatorCreator;
    rank::ScoringProvider *scoringProvider;
    suez::turing::AttributeExpression *scoreExpression;
    DocCountLimits &docCountLimits;
    ha3::ScorerProvider *cavaScorerProvider;
    search::LayerMetas *layerMetas;
    SearchProcessorResource(SearchRuntimeResource &runtimeResource)
        : rankProfile(NULL)
        , sorterManager(NULL)
        , sortExpressionCreator(runtimeResource.sortExpressionCreator.get())
        , comparatorCreator(runtimeResource.comparatorCreator.get())
        , scoringProvider(NULL)
        , scoreExpression(NULL)
        , docCountLimits(runtimeResource.docCountLimits)
        , cavaScorerProvider(NULL)
        , layerMetas(NULL)
    {
    }
};

} // namespace search
} // namespace isearch

