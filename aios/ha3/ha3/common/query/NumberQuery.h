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

#include <assert.h>
#include <stdint.h>
#include <string>

#include "autil/DataBuffer.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/NumberTerm.h"
#include "ha3/common/Query.h"
#include "ha3/isearch.h"

namespace isearch {
namespace common {
class ModifyQueryVisitor;
class QueryVisitor;
struct RequiredFields;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

class NumberQuery : public Query
{
public:
    NumberQuery(int64_t leftNumber, bool leftInclusive, int64_t rightNumber,
                bool rightInclusive, const char *indexName,
                const RequiredFields &requiredFields,
                const std::string &label,
                int64_t boost = DEFAULT_BOOST_VALUE,
                const std::string &truncateName = "");
    NumberQuery(int64_t num, const char *indexName,
                const RequiredFields &requiredFields,
                const std::string &label,
                int64_t boost = DEFAULT_BOOST_VALUE,
                const std::string &truncateName = "");
    NumberQuery(const NumberTerm& term, const std::string &label);
    NumberQuery(const NumberQuery &other);
    ~NumberQuery();
public:
    void setTerm(const NumberTerm& term);
    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    void addQuery(QueryPtr query) override {
        assert(false);
    }
    bool removeQuery(const Query *query);
    Query *rewriteQuery();//optimize query
    const NumberTerm& getTerm() const;
    NumberTerm& getTerm();
    std::string getQueryName() const override { return "NumberQuery"; }
    std::string toString() const override;

    void serialize(autil::DataBuffer &dataBuffer) const override {
        dataBuffer.write(_term);
        serializeMDLandQL(dataBuffer);
    }
    void deserialize(autil::DataBuffer &dataBuffer) override {
        dataBuffer.read(_term);
        deserializeMDLandQL(dataBuffer);
    }
    QueryType getType() const override {
        return NUMBER_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelTerm(label);
    }
private:
    NumberTerm _term;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch
