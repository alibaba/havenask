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
class DistinctDescription;
class ErrorResult;
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

class DistinctParser
{
public:
    DistinctParser(common::ErrorResult *errorResult);
    ~DistinctParser();
public:
    common::DistinctDescription *createDistinctDescription();
    std::vector<std::string *> *createThresholds();
    common::FilterClause* createFilterClause(suez::turing::SyntaxExpr *expr);
public:
    void setDistinctCount(common::DistinctDescription *distDes,
                          const std::string &clauseCountStr);
    void setDistinctTimes(common::DistinctDescription *distDes,
                          const std::string &clauseTimesStr);
    void setMaxItemCount(common::DistinctDescription *distDes,
                         const std::string &maxItemCountStr);
    void setReservedFlag(common::DistinctDescription *distDes,
                         const std::string &reservedFalg);
    void setUpdateTotalHitFlag(common::DistinctDescription *distDes,
                               const std::string &updateTotalHitFlagStr);
    void setGradeThresholds(common::DistinctDescription *distDes,
                            std::vector<std::string *> *gradeThresholdsStr);
private:
    common::ErrorResult *_errorResult;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch

