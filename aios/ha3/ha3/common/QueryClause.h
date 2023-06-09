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

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "ha3/common/ClauseBase.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace common {
class Query;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

class QueryClause : public ClauseBase
{
public:
    QueryClause();
    QueryClause(Query* query);
    ~QueryClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    void setRootQuery(Query *query, uint32_t layer = 0);
    Query *getRootQuery(uint32_t layer = 0) const;
    void insertQuery(Query *query, int32_t layer);
    uint32_t getQueryCount() const {
        return _rootQuerys.size();
    }
private:
    std::vector<Query*> _rootQuerys;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryClause> QueryClausePtr;

typedef QueryClause AuxQueryClause;
typedef std::shared_ptr<AuxQueryClause> AuxQueryClausePtr;


} // namespace common
} // namespace isearch

