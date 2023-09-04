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
#include "ha3/common/NumberQuery.h"

#include <sstream>
#include <stddef.h>

#include "autil/Log.h"
#include "ha3/common/ModifyQueryVisitor.h"
#include "ha3/common/NumberTerm.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"

namespace isearch {
namespace common {
struct RequiredFields;
} // namespace common
} // namespace isearch

using namespace std;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, NumberQuery);

NumberQuery::NumberQuery(int64_t leftNumber,
                         bool leftInclusive,
                         int64_t rightNumber,
                         bool rightInclusive,
                         const char *indexName,
                         const RequiredFields &requiredFields,
                         const std::string &label,
                         int64_t boost,
                         const std::string &truncateName)
    : _term(leftNumber,
            leftInclusive,
            rightNumber,
            rightInclusive,
            indexName,
            requiredFields,
            boost,
            truncateName) {
    setQueryLabelTerm(label);
}

NumberQuery::NumberQuery(int64_t num,
                         const char *indexName,
                         const RequiredFields &requiredFields,
                         const std::string &label,
                         int64_t boost,
                         const std::string &truncateName)
    : _term(num, indexName, requiredFields, boost, truncateName) {
    setQueryLabelTerm(label);
}

NumberQuery::NumberQuery(const NumberTerm &term, const std::string &label)
    : _term(term) {
    setQueryLabelTerm(label);
}

NumberQuery::NumberQuery(const NumberQuery &other)
    : Query(other)
    , _term(other._term) {}

NumberQuery::~NumberQuery() {}

void NumberQuery::setTerm(const NumberTerm &term) {
    _term = term;
}

bool NumberQuery::operator==(const Query &query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    return (_term == dynamic_cast<const NumberQuery &>(query)._term);
}

std::string NumberQuery::toString() const {
    std::stringstream ss;
    ss << "NumberQuery";
    if (!_queryLabel.empty()) {
        ss << "@" << _queryLabel;
    }
    if (getMatchDataLevel() != MDL_NONE) {
        ss << "$" << getMatchDataLevel();
    }
    ss << ":[" << _term.toString() << "]";
    return ss.str();
}

void NumberQuery::accept(QueryVisitor *visitor) const {
    visitor->visitNumberQuery(this);
}

void NumberQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitNumberQuery(this);
}

Query *NumberQuery::clone() const {
    return new NumberQuery(*this);
}

bool NumberQuery::removeQuery(const Query *query) {
    assert(false);
    return false;
}

Query *NumberQuery::rewriteQuery() {
    assert(false);
    return NULL;
}

const NumberTerm &NumberQuery::getTerm() const {
    return _term;
}

NumberTerm &NumberQuery::getTerm() {
    return _term;
}

} // namespace common
} // namespace isearch
