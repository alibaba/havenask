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
#include "ha3/common/DocIdsQuery.h"

#include <sstream>
#include <stddef.h>

#include "autil/Log.h"
#include "ha3/common/ModifyQueryVisitor.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"

using namespace std;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, DocIdsQuery);

DocIdsQuery::DocIdsQuery(const vector<docid_t> &docIds)
    : _docIds(docIds) {}

DocIdsQuery::DocIdsQuery(const DocIdsQuery &other)
    : Query(other)
    , _docIds(other._docIds) {}

DocIdsQuery::~DocIdsQuery() {}

bool DocIdsQuery::operator==(const Query &query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    return (_docIds == dynamic_cast<const DocIdsQuery &>(query)._docIds);
}

std::string DocIdsQuery::toString() const {
    std::stringstream ss;
    ss << "DocIdsQuery";
    if (getMatchDataLevel() != MDL_NONE) {
        ss << "$" << getMatchDataLevel();
    }
    ss << ":[";
    for (size_t i = 0; i < _docIds.size(); ++i) {
        ss << (i == 0 ? "" : ", ") << _docIds[i];
    }
    ss << "]";
    return ss.str();
}

void DocIdsQuery::accept(QueryVisitor *visitor) const {
    visitor->visitDocIdsQuery(this);
}

void DocIdsQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitDocIdsQuery(this);
}

Query *DocIdsQuery::clone() const {
    return new DocIdsQuery(*this);
}

} // namespace common
} // namespace isearch
