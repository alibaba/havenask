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

#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class AggFunDescription;
class AggregateDescription;
class FilterClause;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace queryparser {

class AggregateParser
{
public:
    AggregateParser();
    ~AggregateParser();
public:
    common::AggregateDescription *createAggDesc();
    std::vector<std::string> *createRangeExpr();
    std::vector<common::AggFunDescription*> *createFuncDescs();
    common::AggFunDescription* createAggMaxFunc(suez::turing::SyntaxExpr *expr);
    common::AggFunDescription* createAggMinFunc(suez::turing::SyntaxExpr *expr);
    common::AggFunDescription* createAggCountFunc();
    common::AggFunDescription* createAggSumFunc(suez::turing::SyntaxExpr *expr);
    common::AggFunDescription *createAggDistinctCountFunc(suez::turing::SyntaxExpr *expr);
    common::FilterClause* createFilterClause(suez::turing::SyntaxExpr *expr);
private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch

