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

namespace isearch {
namespace common {
class Query;
}  // namespace common
namespace queryparser {
class QueryExpr;
}  // namespace queryparser
}  // namespace isearch

namespace isearch {
namespace queryparser {

class QueryParser;

class ParserContext
{
public:
    enum Status {
        OK = 0,
        INITIAL_STATUS,
        EVALUATE_FAILED,
        SYNTAX_ERROR,
    };
private:
    static const char *STATUS_MSGS[];
public:
    ParserContext(QueryParser &queryParser);
    ~ParserContext();
public:
    Status getStatus() const {return _status;}
    void setStatus(Status status) {_status = status;}

    const std::string getStatusMsg() const;
    void setStatusMsg(const std::string &statusMsg) {
        _statusMsg = statusMsg;
    }

    void addQuery(common::Query *query);
    const std::vector<common::Query*> &getQuerys() {return _rootQuerys;}
    std::vector<common::Query*> stealQuerys();

    void addQueryExpr(QueryExpr *queryExpr);
    std::vector<QueryExpr*> stealQueryExprs();
    const std::vector<QueryExpr*> &getQueryExprs() {return _rootQueryExprs;}

    QueryParser& getParser() {return _queryParser;}
private:
    QueryParser &_queryParser;
    std::vector<common::Query*> _rootQuerys;
    std::vector<QueryExpr*> _rootQueryExprs;
    Status _status;
    std::string _statusMsg;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch

