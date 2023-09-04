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
#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/analyzer/Token.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"

namespace autil {
class DataBuffer;
} // namespace autil
namespace isearch {
namespace common {
class ModifyQueryVisitor;
class QueryVisitor;
} // namespace common
} // namespace isearch

namespace isearch {
namespace common {

class TermQuery : public Query {
public:
    TermQuery(const Term &term, const std::string &label);
    TermQuery(const std::string &word,
              const std::string &indexName,
              const RequiredFields &requiredFields,
              const std::string &label,
              int32_t boost = DEFAULT_BOOST_VALUE,
              const std::string &truncateName = "")
        : _term(word, indexName, requiredFields, boost, truncateName) {
        setQueryLabelTerm(label);
    }

    TermQuery(const build_service::analyzer::Token &token,
              const char *indexName,
              const RequiredFields &requiredFields,
              const std::string &label,
              int32_t boost = DEFAULT_BOOST_VALUE,
              const std::string &truncateName = "")
        : _term(token, indexName, requiredFields, boost, truncateName) {
        setQueryLabelTerm(label);
    }
    TermQuery(const TermQuery &other);
    ~TermQuery();

public:
    bool operator==(const Query &query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    void setTerm(const Term &term);
    const Term &getTerm() const;
    Term &getTerm();
    std::string getQueryName() const override;
    std::string toString() const override;
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    QueryType getType() const override {
        return TERM_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelTerm(label);
    }

private:
    Term _term;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TermQuery> TermQueryPtr;

} // namespace common
} // namespace isearch
