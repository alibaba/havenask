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

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/QueryExecutor.h"
#include "indexlib/indexlib.h"

namespace isearch {
namespace search {

class MultiQueryExecutor : public QueryExecutor
{
public:
    typedef std::vector<QueryExecutor *> QueryExecutorVector;

public:
    MultiQueryExecutor();
    virtual ~MultiQueryExecutor();
public:
    const std::string getName() const override;
    void reset() override;
    void moveToEnd() override;
    void setEmpty() override;
    void setCurrSub(docid_t docid) override;
    uint32_t getSeekDocCount() override;
    virtual void addQueryExecutors(const std::vector<QueryExecutor*> &queryExecutors) = 0;

    inline QueryExecutor *getQueryExecutor(int idx) {
        return _queryExecutors[idx];
    }

    inline void clear() {
        _queryExecutors.clear();
    }

    inline const QueryExecutor *getQueryExecutor(int idx) const {
        return _queryExecutors[idx];
    }

    inline int32_t getQueryExecutorCount() const {
        return _queryExecutors.size();
    }

    inline const QueryExecutorVector& getQueryExecutors() const {
        return _queryExecutors;
    }

    inline QueryExecutorVector& getQueryExecutors() {
        return _queryExecutors;
    }

public:
    // for test.
    inline void addQueryExecutor(QueryExecutor *queryExecutor) {
        _queryExecutors.push_back(queryExecutor);
    }

    template <class QueryExcutorType>
    static std::string convertToString(const std::vector<QueryExcutorType*> &queryExecutors);
    std::string toString() const override;

protected:
    QueryExecutorVector _queryExecutors;
private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////

template <class QueryExcutorType>
std::string MultiQueryExecutor::convertToString(const std::vector<QueryExcutorType*> &queryExecutors) {
    std::string queryStr = "(";
    for (size_t i = 0; i < queryExecutors.size(); ++i) {
        queryStr += queryExecutors[i]->toString();
        queryStr += queryExecutors.size() - 1 == i ? "" : ",";
    }
    queryStr += ")";
    return queryStr;
}

} // namespace search
} // namespace isearch
