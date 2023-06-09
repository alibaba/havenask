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

#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Query.h"
#include "ha3/isearch.h"

namespace isearch {
namespace common {
class ModifyQueryVisitor;
class QueryVisitor;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

class OrQuery : public Query
{
public:
    OrQuery(const std::string &label);
    OrQuery(const OrQuery &other);
    ~OrQuery();
public:
    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    std::string getQueryName() const override {
        return "OrQuery";
    }

    QueryType getType() const override {
        return OR_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelBinary(label);
    }
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch
