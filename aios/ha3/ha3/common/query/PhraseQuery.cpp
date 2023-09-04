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
#include "ha3/common/PhraseQuery.h"

#include <memory>
#include <sstream>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "ha3/common/ModifyQueryVisitor.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryVisitor.h"
#include "ha3/common/Term.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, PhraseQuery);

PhraseQuery::PhraseQuery(const std::string &label) {
    _queryRestrictor = true;
    setQueryLabelTerm(label);
}

PhraseQuery::PhraseQuery(const PhraseQuery &other)
    : Query(other) {
    for (TermArray::const_iterator it = other._terms.begin(); it != other._terms.end(); ++it) {
        _terms.push_back(TermPtr(new Term(**it)));
    }
    _queryRestrictor = other._queryRestrictor;
}

PhraseQuery::~PhraseQuery() {}

void PhraseQuery::addTerm(const TermPtr &term) {
    _terms.push_back(term);
}

bool PhraseQuery::operator==(const Query &query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    const TermArray &terms2 = dynamic_cast<const PhraseQuery &>(query)._terms;

    if (_terms.size() != terms2.size()) {
        return false;
    }
    TermArray::const_iterator it1 = _terms.begin();
    TermArray::const_iterator it2 = terms2.begin();
    for (; it1 != _terms.end(); it1++, it2++) {
        if (!((**it1) == (**it2))) {
            return false;
        }
    }
    return true;
}

void PhraseQuery::accept(QueryVisitor *visitor) const {
    visitor->visitPhraseQuery(this);
}

void PhraseQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitPhraseQuery(this);
}

Query *PhraseQuery::clone() const {
    return new PhraseQuery(*this);
}

std::string PhraseQuery::toString() const {
    std::stringstream ss;
    ss << "PhraseQuery";
    if (!_queryLabel.empty()) {
        ss << "@" << _queryLabel;
    }
    if (getMatchDataLevel() != MDL_NONE) {
        ss << "$" << getMatchDataLevel();
    }
    ss << ":[";
    for (TermArray::const_iterator it = _terms.begin(); it != _terms.end(); it++) {
        ss << (*it)->toString() << ", ";
    }
    ss << "]";
    return ss.str();
}

const PhraseQuery::TermArray &PhraseQuery::getTermArray() const {
    return _terms;
}

void PhraseQuery::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_terms);
    serializeMDLandQL(dataBuffer);
}

void PhraseQuery::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_terms);
    deserializeMDLandQL(dataBuffer);
}

} // namespace common
} // namespace isearch
