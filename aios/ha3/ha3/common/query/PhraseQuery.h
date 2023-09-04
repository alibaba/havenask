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

#include "autil/Log.h" // IWYU pragma: keep
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

class PhraseQuery : public Query {
public:
    PhraseQuery(const std::string &label);
    PhraseQuery(const PhraseQuery &other);
    ~PhraseQuery();

public:
    void addTerm(const TermPtr &term);
    bool operator==(const Query &query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    std::string getQueryName() const override {
        return "PhraseQuery";
    }
    std::string toString() const override;
    const TermArray &getTermArray() const;
    TermArray &getTermArray() {
        return _terms;
    }
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    QueryType getType() const override {
        return PHRASE_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelTerm(label);
    }

private:
    TermArray _terms;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PhraseQuery> PhraseQueryPtr;

} // namespace common
} // namespace isearch
