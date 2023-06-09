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

#include "autil/Log.h" // IWYU pragma: keep

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

class SortDescription
{
public:
    enum RankSortType {
        RS_NORMAL,
        RS_RANK
    };
public:
    SortDescription();
    SortDescription(const std::string &originalString);
    ~SortDescription();
public:
    bool getSortAscendFlag() const;
    void setSortAscendFlag(bool sortAscendFlag);

    const std::string& getOriginalString() const;
    void setOriginalString(const std::string &originalString);

    suez::turing::SyntaxExpr* getRootSyntaxExpr() const;
    void setRootSyntaxExpr(suez::turing::SyntaxExpr* expr);

    bool isRankExpression() const {
        return _type == RS_RANK;
    }

    void setExpressionType(RankSortType type) {
        _type = type;
    }
    RankSortType getExpressionType() const {
        return _type;
    }

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    std::string toString() const;
private:
    suez::turing::SyntaxExpr *_rootSyntaxExpr;
    std::string _originalString;
    bool _sortAscendFlag;
    RankSortType _type;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch

