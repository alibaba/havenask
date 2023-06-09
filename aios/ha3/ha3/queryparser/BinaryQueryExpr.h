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


#include "ha3/queryparser/QueryExpr.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace queryparser {

class BinaryQueryExpr : public QueryExpr
{
public:
    BinaryQueryExpr(QueryExpr *a, QueryExpr *b);
    ~BinaryQueryExpr();
public:
    QueryExpr *getLeftExpr() {return _exprA;}
    QueryExpr *getRightExpr() {return _exprB;}
    virtual void setLeafIndexName(const std::string &indexName) override {
        _exprA->setLeafIndexName(indexName);
        _exprB->setLeafIndexName(indexName);
    }
private:
    QueryExpr *_exprA;
    QueryExpr *_exprB;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch
