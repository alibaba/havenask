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

namespace isearch {
namespace queryparser {

class QueryExprEvaluator;

class QueryExpr {
public:
    QueryExpr();
    virtual ~QueryExpr();

public:
    virtual void evaluate(QueryExprEvaluator *qee) = 0;
    virtual void setLeafIndexName(const std::string &indexName) = 0;
    virtual void setLabel(const std::string &label) {
        _label = label;
    }
    virtual std::string getLabel() const {
        return _label;
    }

private:
    std::string _label;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch
