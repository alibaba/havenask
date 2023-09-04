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
#include "ha3/common/AndNotQuery.h"

#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/common/ModifyQueryVisitor.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AndNotQuery);

AndNotQuery::AndNotQuery(const std::string &label) {
    setQueryLabelBinary(label);
}

AndNotQuery::~AndNotQuery() {}

bool AndNotQuery::operator==(const Query &query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    const QueryVector &children2 = dynamic_cast<const AndNotQuery &>(query)._children;

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

void AndNotQuery::accept(QueryVisitor *visitor) const {
    visitor->visitAndNotQuery(this);
}

void AndNotQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitAndNotQuery(this);
}

Query *AndNotQuery::clone() const {
    return new AndNotQuery(*this);
}

} // namespace common
} // namespace isearch
