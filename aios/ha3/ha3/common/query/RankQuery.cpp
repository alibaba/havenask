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
#include "ha3/common/RankQuery.h"

#include <assert.h>
#include <stdint.h>
#include <sstream>
#include <string>
#include <memory>
#include <vector>

#include "autil/DataBuffer.h"

#include "ha3/common/ModifyQueryVisitor.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, RankQuery);


RankQuery::RankQuery(const std::string &label) {
    setQueryLabelBinary(label);
}

RankQuery::RankQuery(const RankQuery &other)
    : Query(other)
    , _rankBoosts(other._rankBoosts)
{
}

RankQuery::~RankQuery() {
}

bool RankQuery::operator == (const Query& query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    const QueryVector &children2 = dynamic_cast<const RankQuery&>(query)._children;

    if (_children.size() != children2.size()) {
        return false;
    }
    QueryVector::const_iterator it1 = _children.begin();
    QueryVector::const_iterator it2 = children2.begin();
    for (; it1 != _children.end(); it1++, it2++)
    {
        if (!( *(*it1) == *(*it2) )) {
            return false;
        }
    }
    return true;
}

void RankQuery::addQuery(QueryPtr queryPtr) {
    _children.push_back(queryPtr);
    _rankBoosts.push_back(0);
}

void RankQuery::addQuery(QueryPtr queryPtr, uint32_t rankBoost) {
    _children.push_back(queryPtr);
    _rankBoosts.push_back(rankBoost);
}

void RankQuery::accept(QueryVisitor *visitor) const {
    visitor->visitRankQuery(this);
}

void RankQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitRankQuery(this);
}

Query *RankQuery::clone() const {
    return new RankQuery(*this);
}

void RankQuery::setRankBoost(uint32_t rankBoost, uint32_t pos) {
    assert(pos < _rankBoosts.size());
    _rankBoosts[pos] = rankBoost;
}

uint32_t RankQuery::getRankBoost(uint32_t pos) const {
    assert(pos < _rankBoosts.size());
    return _rankBoosts[pos];
}

void RankQuery::serialize(autil::DataBuffer &dataBuffer) const
{
    Query::serialize(dataBuffer);
    dataBuffer.write(_rankBoosts);
}

void RankQuery::deserialize(autil::DataBuffer &dataBuffer)
{
    Query::deserialize(dataBuffer);
    dataBuffer.read(_rankBoosts);
}

} // namespace common
} // namespace isearch
