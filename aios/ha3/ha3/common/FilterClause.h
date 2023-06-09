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

#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"

namespace autil {
class DataBuffer;
}  // namespace autil
namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace common {

class FilterClause : public ClauseBase
{
public:
    FilterClause();
    FilterClause(suez::turing::SyntaxExpr *filterExpr);
    ~FilterClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    void setRootSyntaxExpr(suez::turing::SyntaxExpr *filterExpr);
    const suez::turing::SyntaxExpr* getRootSyntaxExpr() const;
private:
    suez::turing::SyntaxExpr *_filterExpr;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FilterClause> FilterClausePtr;

typedef FilterClause AuxFilterClause;
typedef std::shared_ptr<AuxFilterClause> AuxFilterClausePtr;

} // namespace common
} // namespace isearch
