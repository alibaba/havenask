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
#include "ha3/common/TableQuery.h"

#include <ostream>
#include <stddef.h>
#include <sys/types.h>

#include "autil/DataBuffer.h"
#include "ha3/common/ColumnTerm.h"
#include "ha3/common/ModifyQueryVisitor.h"
#include "ha3/common/QueryVisitor.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, TableQuery);

TableQuery::TableQuery(const std::string &label,
                       QueryOperator rowOp,
                       QueryOperator columnOp,
                       QueryOperator multiValueOp)
    : _rowOp(rowOp)
    , _columnOp(columnOp)
    , _multiValueOp(multiValueOp) {
    setQueryLabelTerm(label);
}

TableQuery::~TableQuery() {
    for (auto p : _terms) {
        delete p;
    }
}

bool TableQuery::operator==(const Query &query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    auto p = dynamic_cast<const TableQuery *>(&query);
    if (!p) {
        return false;
    }
    const auto &tableQuery = *p;
    if (getQueryLabel() != tableQuery.getQueryLabel()) {
        return false;
    }
    if (_rowOp != tableQuery._rowOp || _columnOp != tableQuery._columnOp
        || _multiValueOp != tableQuery._multiValueOp) {
        return false;
    }
    const TermArray &terms2 = tableQuery._terms;
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

void TableQuery::accept(QueryVisitor *visitor) const {
    visitor->visitTableQuery(this);
}

void TableQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitTableQuery(this);
}

Query *TableQuery::clone() const {
    auto p = new TableQuery(*this);
    for (auto &term : p->_terms) {
        auto newTerm = term->clone();
        term = newTerm;
    }
    return p;
}

std::string TableQuery::toString() const {
    std::stringstream ss;
    ss << "TableQuery:rowOp:" << _rowOp << ":columnOp:" << _columnOp
       << ":multiValueOp:" << _multiValueOp;
    if (!_queryLabel.empty()) {
        ss << "@" << _queryLabel;
    }
    if (getMatchDataLevel() != MDL_NONE) {
        ss << "$" << getMatchDataLevel();
    }
    ss << ":[";
    for (auto term : _terms) {
        ss << term->toString() << ", ";
    }
    ss << "]";
    return ss.str();
}

void TableQuery::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_terms.size());
    for (auto p : _terms) {
        ColumnTerm::serialize(p, dataBuffer);
    }
    dataBuffer.write((int8_t)_rowOp);
    dataBuffer.write((int8_t)_columnOp);
    dataBuffer.write((int8_t)_multiValueOp);
    serializeMDLandQL(dataBuffer);
}

void TableQuery::deserialize(autil::DataBuffer &dataBuffer) {
    size_t termSize = 0;
    dataBuffer.read(termSize);
    _terms.reserve(termSize);
    for (size_t i = 0; i < termSize; ++i) {
        _terms.push_back(ColumnTerm::deserialize(dataBuffer));
    }
    int8_t op = 0;
    dataBuffer.read(op);
    _rowOp = (QueryOperator)op;
    dataBuffer.read(op);
    _columnOp = (QueryOperator)op;
    dataBuffer.read(op);
    _multiValueOp = (QueryOperator)op;
    deserializeMDLandQL(dataBuffer);
}

} // namespace common
} // namespace isearch
