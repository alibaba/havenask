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

#include <stdint.h>
#include <string>
#include <vector>

#include "ha3/common/ClauseBase.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace common {
class SortDescription;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace common {

class SortClause : public ClauseBase
{
public:
    SortClause();
    ~SortClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    const std::vector<SortDescription*>& getSortDescriptions() const;
    SortDescription* getSortDescription(uint32_t pos);

    void addSortDescription(SortDescription *sortDescription);
    void addSortDescription(suez::turing::SyntaxExpr* syntaxExpr,
                            bool isRank,
                            bool sortAscendFlag = false);
    void clearSortDescriptions();
private:
    std::vector<SortDescription*> _sortDescriptions;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch

 
