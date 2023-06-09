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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Query.h"
#include "ha3/isearch.h"

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace common {
class ModifyQueryVisitor;
class QueryVisitor;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

class RankQuery : public Query
{
public:
    RankQuery(const std::string &label);
    RankQuery(const RankQuery &other);
    ~RankQuery();
public:
    void addQuery(QueryPtr queryPtr) override;
    void addQuery(QueryPtr queryPtr, uint32_t rankBoost);

    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;

    std::string getQueryName() const override {
        return "RankQuery";
    }

    void setRankBoost(uint32_t rankBoost, uint32_t pos);
    uint32_t getRankBoost(uint32_t pos) const;

    QueryType getType() const override {
        return RANK_QUERY;
    }
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelBinary(label);
    }
private:
    typedef std::vector<uint32_t> RankBoostVecor;
    RankBoostVecor _rankBoosts;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch
