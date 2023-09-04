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

#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Query.h"
#include "ha3/isearch.h"

namespace autil {
class DataBuffer;
} // namespace autil

namespace isearch {
namespace common {
class ColumnTerm;
class ModifyQueryVisitor;
class QueryVisitor;
} // namespace common
} // namespace isearch

namespace isearch {
namespace common {

class TableQuery : public Query {
public:
    using TermArray = std::vector<ColumnTerm *>;

public:
    TableQuery(const std::string &label,
               QueryOperator rowOp = OP_OR,
               QueryOperator columnOp = OP_AND,
               QueryOperator multiValueOp = OP_OR);
    ~TableQuery();

private:
    TableQuery(const TableQuery &) = default;

public:
    bool operator==(const Query &query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;

    std::string getQueryName() const override {
        return "TableQuery";
    }
    std::string toString() const override;
    const TermArray &getTermArray() const {
        return _terms;
    }
    TermArray &getTermArray() {
        return _terms;
    }
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;

    QueryType getType() const override {
        return TABLE_QUERY;
    }
    QueryOperator getRowOp() const {
        return _rowOp;
    }
    QueryOperator getColumnOp() const {
        return _columnOp;
    }
    QueryOperator getMultiValueOp() const {
        return _multiValueOp;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelTerm(label);
    }

private:
    TermArray _terms;
    QueryOperator _rowOp;
    QueryOperator _columnOp;
    QueryOperator _multiValueOp;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TableQuery> TableQueryPtr;

} // namespace common
} // namespace isearch
