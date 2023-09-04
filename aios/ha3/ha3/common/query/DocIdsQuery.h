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
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Query.h"
#include "ha3/isearch.h"
#include "indexlib/indexlib.h"

namespace isearch {
namespace common {
class ModifyQueryVisitor;
class QueryVisitor;
} // namespace common
} // namespace isearch

namespace isearch {
namespace common {

class DocIdsQuery : public Query {
public:
    DocIdsQuery(const std::vector<docid_t> &docIds);
    DocIdsQuery(const DocIdsQuery &other);
    ~DocIdsQuery();

public:
    bool operator==(const Query &query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    std::string getQueryName() const override {
        return "DocIdsQuery";
    }
    std::string toString() const override;
    const std::vector<docid_t> &getDocIds() const {
        return _docIds;
    }
    QueryType getType() const override {
        return DOCIDS_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelTerm(label);
    }

private:
    std::vector<docid_t> _docIds;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch
