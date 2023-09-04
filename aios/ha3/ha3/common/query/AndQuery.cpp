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
#include "ha3/common/AndQuery.h"

#include <memory>
#include <vector>

#include "autil/Log.h"
#include "ha3/common/ModifyQueryVisitor.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AndQuery);

AndQuery::AndQuery(const std::string &label) {
    setQueryLabelBinary(label);
}

AndQuery::AndQuery(const AndQuery &other)
    : Query(other) {}

AndQuery::~AndQuery() {}

bool AndQuery::operator==(const Query &query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    const QueryVector &children2 = dynamic_cast<const AndQuery &>(query)._children;

    if (_children.size() != children2.size()) {
        return false;
    }
    QueryVector::const_iterator it1 = _children.begin();
    QueryVector::const_iterator it2 = children2.begin();
    for (; it1 != _children.end(); it1++, it2++) {
        if (!(*(*it1) == *(*it2))) {
            return false;
        }
    }
    return true;
}

void AndQuery::accept(QueryVisitor *visitor) const {
    visitor->visitAndQuery(this);
}

void AndQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitAndQuery(this);
}

Query *AndQuery::clone() const {
    return new AndQuery(*this);
}

} // namespace common
} // namespace isearch
