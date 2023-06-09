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
#include "ha3/common/TermQuery.h"

#include <sstream>
#include <string>

#include "autil/DataBuffer.h"

#include "ha3/common/ModifyQueryVisitor.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"
#include "ha3/common/Term.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, TermQuery);

TermQuery::TermQuery(const Term& term, const std::string &label) : _term(term) {
    setQueryLabelTerm(label);
}

TermQuery::TermQuery(const TermQuery& other)
    : Query(other)
    , _term(other._term)
{
}

TermQuery::~TermQuery() {
}

bool TermQuery::operator == (const Query& query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    return (_term == dynamic_cast<const TermQuery&>(query)._term);
}

void TermQuery::accept(QueryVisitor *visitor) const {
    visitor->visitTermQuery(this);
}

void TermQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitTermQuery(this);
}

Query *TermQuery::clone() const {
    return new TermQuery(*this);
}

std::string TermQuery::getQueryName() const {
    return "TermQuery";
}

std::string TermQuery::toString() const {
    std::stringstream ss;
    ss << "TermQuery";
    if (!_queryLabel.empty()) {
        ss << "@" << _queryLabel;
    }
    if (getMatchDataLevel() != MDL_NONE) {
        ss << "$" << getMatchDataLevel();
    }
    ss << ":[" << _term.toString() << "]";
    return ss.str();
}

void TermQuery::setTerm(const Term& term) {
    _term = term;
}

const Term& TermQuery::getTerm() const {
    return _term;
}

Term& TermQuery::getTerm() {
    return _term;
}

void TermQuery::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_term);
    serializeMDLandQL(dataBuffer);
}

void TermQuery::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_term);
    deserializeMDLandQL(dataBuffer);
}

} // namespace common
} // namespace isearch
